import os
import uuid
import asyncio
import threading
import logging
from functools import wraps
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, redirect, url_for, session, send_file, Response
import base64
from deep_translator import GoogleTranslator
from flask_sqlalchemy import SQLAlchemy

DOWNLOADS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server_downloads")
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

# {download_id: {status, filename, safe_name, path, error, chat_id, msg_id, size}}
download_queue = {}

# Configure logging
logging.basicConfig(
    level=logging.WARNING, # Changed from INFO to reduce pyrogram logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING) # Specifically silence pyrogram info logs

# FIX: Pyrogram sync mode requires an event loop in the main thread during import
try:
    asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from pyrogram import Client

app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.environ.get("SECRET_KEY", "default-secret-key-change-me")
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=60)

# Global client management
telegram_clients = {}

def _is_broken_pipe(e):
    return "Broken pipe" in str(e) or "BrokenPipeError" in type(e).__name__

async def get_client(session_string, force_reconnect=False):
    if not session_string:
        return None
    if force_reconnect and session_string in telegram_clients:
        logger.warning("Force-reconnecting Telegram client due to stale connection")
        try:
            await telegram_clients[session_string].stop()
        except: pass
        del telegram_clients[session_string]
    if session_string not in telegram_clients:
        client = create_telegram_client(session_string)
        await client.start()
        telegram_clients[session_string] = client
    return telegram_clients[session_string]

async def run_with_reconnect(session_string, coro_factory):
    """Run coro_factory(client). On broken pipe, reconnect once and retry."""
    for attempt in range(2):
        try:
            client = await get_client(session_string, force_reconnect=(attempt > 0))
            return await coro_factory(client)
        except Exception as e:
            if _is_broken_pipe(e) and attempt == 0:
                logger.warning("Broken pipe detected — reconnecting and retrying")
                continue
            raise

# PostgreSQL Configuration
# Replit provides DATABASE_URL for PostgreSQL integrations
database_url = os.environ.get("DATABASE_URL")
if database_url and database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)
app.config['SQLALCHEMY_DATABASE_URI'] = database_url or "sqlite:///app.db"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

class MessageStore(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    message_id = db.Column(db.BigInteger)
    chat_id = db.Column(db.BigInteger)
    user_id = db.Column(db.BigInteger)
    text = db.Column(db.Text)
    date = db.Column(db.DateTime)
    media_path = db.Column(db.String(500))
    is_deleted = db.Column(db.Boolean, default=False)

with app.app_context():
    db.create_all()

API_ID = int(os.environ.get("API_ID", "12345"))
API_HASH = os.environ.get("API_HASH", "your_api_hash_here")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "password123")

def format_file_size(size_bytes):
    if not size_bytes: return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])

def get_proxy_config():
    PROXY_TYPE = os.environ.get("PROXY_TYPE", "").lower()
    PROXY_HOST = os.environ.get("PROXY_HOST", "")
    PROXY_PORT = int(os.environ.get("PROXY_PORT", "0") or "0")
    if not PROXY_TYPE or not PROXY_HOST or not PROXY_PORT: return None
    proxy = {"scheme": PROXY_TYPE, "hostname": PROXY_HOST, "port": PROXY_PORT}
    if os.environ.get("PROXY_USER") and os.environ.get("PROXY_PASS"):
        proxy["username"] = os.environ.get("PROXY_USER")
        proxy["password"] = os.environ.get("PROXY_PASS")
    return proxy

def create_telegram_client(session_string):
    client = Client(name=f"session_{hash(session_string)}", session_string=session_string, api_id=API_ID, api_hash=API_HASH, proxy=get_proxy_config(), in_memory=True)
    
    @client.on_message()
    async def log_message(c, m):
        try:
            with app.app_context():
                new_msg = MessageStore(
                    message_id=m.id,
                    chat_id=m.chat.id,
                    user_id=m.from_user.id if m.from_user else None,
                    text=m.text or m.caption or "",
                    date=m.date
                )
                db.session.add(new_msg)
                db.session.commit()
        except Exception as e:
            logger.error(f"Error logging message: {e}")
            
    return client

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('app_authenticated'): return redirect(url_for('app_login'))
        return f(*args, **kwargs)
    return decorated_function

# Fixed run_async to handle nested loops and prevent thread block issues
_loop = asyncio.new_event_loop()
_loop_thread = threading.Thread(target=_loop.run_forever, daemon=True)
_loop_thread.start()

def run_async(coro):
    future = asyncio.run_coroutine_threadsafe(coro, _loop)
    return future.result()

async def _download_to_server(download_id, session_str, chat_id, msg_id):
    entry = download_queue[download_id]
    try:
        entry["status"] = "downloading"
        try: peer_id = int(chat_id)
        except: peer_id = chat_id

        async def _do(client):
            msg = await client.get_messages(peer_id, int(msg_id))
            if not msg or not msg.media:
                entry["status"] = "failed"
                entry["error"] = "No downloadable media"
                return
            media_obj = getattr(msg, msg.media.value, None)
            file_name = getattr(media_obj, "file_name", None)
            if not file_name:
                ext = ".file"
                if msg.photo: ext = ".jpg"
                elif msg.video: ext = ".mp4"
                elif msg.audio: ext = ".mp3"
                elif msg.voice: ext = ".ogg"
                elif msg.animation: ext = ".mp4"
                elif msg.sticker: ext = ".webp"
                elif msg.video_note: ext = ".mp4"
                file_name = f"file_{msg_id}{ext}"
            safe_name = f"{download_id}_{file_name}"
            dest = os.path.join(DOWNLOADS_DIR, safe_name)
            entry["filename"] = file_name
            entry["safe_name"] = safe_name
            await client.download_media(msg, file_name=dest)
            if os.path.exists(dest):
                entry["size"] = format_file_size(os.path.getsize(dest))
                entry["path"] = dest
                entry["status"] = "done"
            else:
                entry["status"] = "failed"
                entry["error"] = "File not written"

        await run_with_reconnect(session_str, _do)
    except Exception as e:
        logger.error(f"Server download error [{download_id}]: {e}")
        entry["status"] = "failed"
        entry["error"] = str(e)

async def get_dialogs_list(client, offset=0, limit=20):
    dialogs = []
    count = 0
    async for dialog in client.get_dialogs():
        if count < offset:
            count += 1
            continue
        dialogs.append({
            "name": dialog.chat.title or dialog.chat.first_name or "Unknown",
            "id": dialog.chat.id,
            "unread_count": dialog.unread_messages_count,
            "is_channel": dialog.chat.type.value == "channel",
            "is_group": dialog.chat.type.value in ["group", "supergroup"],
            "can_manage": dialog.chat.type.value in ["channel", "group", "supergroup"]
        })
        if len(dialogs) >= limit: break
    return dialogs

async def get_account_info(session_string):
    try:
        async def _do(client):
            me = await client.get_me()
            profile_photo = None
            if me.photo:
                try:
                    path = await client.download_media(me.photo.big_file_id)
                    if path:
                        with open(path, "rb") as f: profile_photo = base64.b64encode(f.read()).decode('utf-8')
                        os.remove(path)
                except: pass
            dialogs = await get_dialogs_list(client, offset=0, limit=20)
            return {
                "id": me.id, "first_name": me.first_name or "", "last_name": me.last_name or "",
                "username": me.username or "No username", "phone": me.phone_number or "Hidden",
                "profile_photo": profile_photo, "dialogs": dialogs, "has_more_dialogs": len(dialogs) == 20
            }
        return await run_with_reconnect(session_string, _do)
    except Exception as e: return {"error": str(e)}

async def get_messages_from_chat(session_string, chat_id, limit=100, offset_id=0, query=None, media_only=False):
    try: chat_id = int(chat_id)
    except: pass

    DOWNLOADABLE_MEDIA = {"photo", "video", "document", "audio", "voice", "animation", "video_note", "sticker"}

    def extract_forward(msg):
        if msg.forward_from:
            name = f"{msg.forward_from.first_name or ''} {msg.forward_from.last_name or ''}".strip()
            return name or "Unknown"
        elif msg.forward_from_chat:
            return msg.forward_from_chat.title or msg.forward_from_chat.first_name or "Unknown"
        elif getattr(msg, 'forward_sender_name', None):
            return msg.forward_sender_name
        return None

    def build_msg_dict(msg):
        media_type = msg.media.value if msg.media else None
        downloadable = media_type in DOWNLOADABLE_MEDIA if media_type else False
        m = {
            "id": msg.id, "date": msg.date.isoformat() if msg.date else None,
            "text": msg.text or msg.caption or "", "is_outgoing": msg.outgoing,
            "has_media": downloadable, "media_type": media_type,
            "forward_from": extract_forward(msg)
        }
        if downloadable:
            try:
                obj = getattr(msg, media_type)
                if hasattr(obj, 'file_size') and obj.file_size: m["file_size"] = format_file_size(obj.file_size)
                if hasattr(obj, 'file_name') and obj.file_name: m["file_name"] = obj.file_name
            except: pass
        return m

    try:
        async def _do(client):
            chat = None
            try:
                chat = await client.get_chat(chat_id)
            except Exception as e:
                logger.warning(f"Initial get_chat failed for {chat_id}: {e}")
                async for dialog in client.get_dialogs(limit=50):
                    if str(dialog.chat.id) == str(chat_id):
                        chat = dialog.chat
                        break
                if not chat:
                    try:
                        chat = await client.get_chat(chat_id)
                    except Exception as final_e:
                        logger.error(f"Final get_chat failed for {chat_id}: {final_e}")
                        return {"error": f"Chat not found: {final_e}"}

            chat_name = getattr(chat, "title", None) or getattr(chat, "first_name", None) or "Chat"
            messages = []
            has_more = False

            if query:
                async for msg in client.search_messages(chat_id, query=query, limit=limit):
                    if media_only and not msg.media: continue
                    messages.append(build_msg_dict(msg))
                messages.sort(key=lambda x: x["id"], reverse=True)
            elif media_only:
                count = 0
                async for msg in client.get_chat_history(chat_id, limit=1500, offset_id=offset_id if offset_id > 0 else 0):
                    if not msg.media: continue
                    media_type = msg.media.value if msg.media else None
                    if media_type not in DOWNLOADABLE_MEDIA: continue
                    messages.append(build_msg_dict(msg))
                    count += 1
                    if count >= limit:
                        has_more = True
                        break
                else:
                    has_more = False
            else:
                async for msg in client.get_chat_history(chat_id, limit=limit, offset_id=offset_id if offset_id > 0 else 0):
                    messages.append(build_msg_dict(msg))
                has_more = len(messages) >= limit

            return {
                "messages": messages,
                "chat_name": chat_name,
                "has_more": has_more,
                "can_manage": getattr(chat.type, "value", "") in ["channel", "group", "supergroup"]
            }

        return await run_with_reconnect(session_string, _do)
    except Exception as e:
        logger.error(f"Error in get_messages_from_chat: {e}", exc_info=True)
        return {"error": str(e)}

@app.route("/api/messages/<chat_id>")
@login_required
def get_messages_route(chat_id):
    return jsonify(run_async(get_messages_from_chat(
        session.get("session_string"), chat_id, 
        limit=int(request.args.get("limit", 50)), 
        offset_id=int(request.args.get("offset_id", 0)),
        query=request.args.get("query"),
        media_only=request.args.get("media_only") == "true"
    )))

@app.route("/api/dialogs")
@login_required
def get_dialogs_route():
    offset = int(request.args.get("offset", 0))
    session_str = session.get("session_string")
    if not session_str: return jsonify({"error": "No session"}), 400
    
    async def task():
        client = await get_client(session_str)
        dialogs = await get_dialogs_list(client, offset=offset)
        return {"dialogs": dialogs, "has_more_dialogs": len(dialogs) == 20}

    try:
        result = run_async(task())
        return jsonify(result)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/sessions")
@login_required
def get_sessions():
    session_str = session.get("session_string")
    if not session_str: return jsonify({"error": "No session"}), 400

    async def task():
        from pyrogram import raw
        client = await get_client(session_str)
        result = await client.invoke(raw.functions.account.GetAuthorizations())
        sessions = []
        for auth in result.authorizations:
            sessions.append({
                "hash": auth.hash,
                "current": auth.hash == 0,
                "device": auth.device_model,
                "platform": auth.platform,
                "system": auth.system_version,
                "app_name": auth.app_name,
                "app_version": auth.app_version,
                "ip": auth.ip,
                "country": auth.country,
                "region": auth.region,
                "date_created": auth.date_created.isoformat() if hasattr(auth.date_created, 'isoformat') else str(auth.date_created),
                "date_active": auth.date_active.isoformat() if hasattr(auth.date_active, 'isoformat') else str(auth.date_active),
            })
        sessions.sort(key=lambda s: (not s["current"], s["date_active"]), reverse=False)
        return {"sessions": sessions}

    try:
        return jsonify(run_async(task()))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/sessions/terminate", methods=["POST"])
@login_required
def terminate_session():
    session_str = session.get("session_string")
    data = request.json
    hash_val = data.get("hash")
    if not session_str or hash_val is None: return jsonify({"error": "Missing params"}), 400
    if hash_val == 0: return jsonify({"error": "Cannot terminate current session"}), 400

    async def task():
        from pyrogram import raw
        client = await get_client(session_str)
        await client.invoke(raw.functions.account.ResetAuthorization(hash=hash_val))
        return {"success": True}

    try:
        return jsonify(run_async(task()))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/queue-download/<chat_id>/<message_id>")
@login_required
def queue_download(chat_id, message_id):
    session_str = session.get("session_string")
    if not session_str: return jsonify({"error": "No session"}), 400
    download_id = str(uuid.uuid4())[:8]
    download_queue[download_id] = {
        "status": "queued", "chat_id": chat_id, "msg_id": message_id,
        "filename": None, "safe_name": None, "path": None, "error": None, "size": None
    }
    asyncio.run_coroutine_threadsafe(
        _download_to_server(download_id, session_str, chat_id, message_id), _loop
    )
    return jsonify({"download_id": download_id})

@app.route("/api/downloads")
@login_required
def list_downloads():
    items = []
    for k, v in list(download_queue.items()):
        items.append({"id": k, "status": v["status"], "filename": v["filename"],
                      "safe_name": v["safe_name"], "error": v["error"], "size": v["size"]})
    return jsonify({"downloads": items})

@app.route("/serve-download/<download_id>")
@login_required
def serve_download(download_id):
    entry = download_queue.get(download_id)
    if not entry or entry["status"] != "done" or not entry.get("path"):
        return "File not ready", 404
    path = entry["path"]
    if not os.path.exists(path): return "File missing on server", 404
    return send_file(path, as_attachment=True, download_name=entry["filename"])

@app.route("/api/downloads/delete/<download_id>", methods=["DELETE"])
@login_required
def delete_server_download(download_id):
    entry = download_queue.get(download_id)
    if not entry: return jsonify({"error": "Not found"}), 404
    if entry.get("path") and os.path.exists(entry["path"]):
        try: os.remove(entry["path"])
        except: pass
    download_queue.pop(download_id, None)
    return jsonify({"success": True})

@app.route("/translate", methods=["POST"])
@login_required
def translate_text():
    data = request.json
    text = data.get("text")
    target_lang = data.get("lang", "en")
    if not text: return jsonify({"error": "No text"}), 400
    try:
        translated = GoogleTranslator(source='auto', target=target_lang).translate(text)
        return jsonify({"translated": translated})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/delete-messages", methods=["POST"])
@login_required
def delete_messages():
    data = request.json
    chat_id = data.get("chat_id")
    message_ids = data.get("message_ids")
    session_str = session.get("session_string")
    if not chat_id or not message_ids or not session_str: return jsonify({"error": "Missing params"}), 400
    
    async def task():
        client = await get_client(session_str)
        await client.delete_messages(chat_id, message_ids)
        return {"success": True}

    try:
        return jsonify(run_async(task()))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/download/<chat_id>/<message_id>")
@login_required
def download_media_route(chat_id, message_id):
    session_str = session.get("session_string")
    if not session_str: return "No session", 400
    
    async def generate_download():
        client = await get_client(session_str)
        try: 
            peer_id = int(chat_id)
        except: 
            peer_id = chat_id
        
        try:
            msg = await client.get_messages(peer_id, int(message_id))
            if not msg or not msg.media:
                return

            # Use a generator to stream the file from Telegram to the browser
            async for chunk in client.stream_media(msg):
                yield chunk
        except Exception as e:
            logger.error(f"Stream error: {e}")
            if "doesn't contain any downloadable media" in str(e):
                return
            # Try to recover client if session is broken
            if "Broken pipe" in str(e) or "Session" in str(e):
                if session_str in telegram_clients:
                    try:
                        await telegram_clients[session_str].stop()
                    except: pass
                    del telegram_clients[session_str]
            raise e

    # Since we're using a generator, we need to wrap it in a Response object
    # We also need to get some info about the file for headers
    async def get_file_info():
        try:
            client = await get_client(session_str)
            try: peer_id = int(chat_id)
            except: peer_id = chat_id
            msg = await client.get_messages(peer_id, int(message_id))
            if not msg or not msg.media: return None, None
            media = getattr(msg, msg.media.value)
            
            # IMPROVED EXTENSION HANDLING
            file_name = getattr(media, "file_name", None)
            if not file_name:
                # Guess extension based on media type if file_name is missing
                ext = ".file"
                if msg.photo: ext = ".jpg"
                elif msg.video: ext = ".mp4"
                elif msg.audio: ext = ".mp3"
                elif msg.voice: ext = ".ogg"
                elif msg.document: 
                    # Try to get mime type
                    mime = getattr(media, "mime_type", "")
                    if "video" in mime: ext = ".mp4"
                    elif "image" in mime: ext = ".jpg"
                    elif "audio" in mime: ext = ".mp3"
                file_name = f"download_{message_id}{ext}"
            
            return file_name, getattr(media, "file_size", None)
        except Exception as e:
            logger.error(f"File info error: {e}")
            if "Broken pipe" in str(e):
                if session_str in telegram_clients:
                    try: await telegram_clients[session_str].stop()
                    except: pass
                    del telegram_clients[session_str]
            raise e
    
    try:
        file_name, file_size = run_async(get_file_info())
        
        if not file_name:
            return "File not found", 404

        def stream_wrapper():
            # Use the existing loop thread for streaming
            # Create a queue to bridge async generator and sync iterator
            import queue
            q = queue.Queue(maxsize=20) # Increased queue size
            
            async def producer():
                try:
                    async for chunk in generate_download():
                        # Use a thread-safe way to put into queue
                        while True:
                            try:
                                q.put(chunk, block=True, timeout=2.0)
                                break
                            except queue.Full:
                                # Check if client disconnected on browser side
                                continue
                except Exception as e:
                    logger.error(f"Producer error: {e}")
                finally:
                    q.put(None) # Signal end

            # Schedule the producer on the background loop
            asyncio.run_coroutine_threadsafe(producer(), _loop)

            while True:
                try:
                    chunk = q.get(timeout=30) # Add timeout to avoid hanging
                    if chunk is None:
                        break
                    yield chunk
                except queue.Empty:
                    logger.warning("Stream queue empty, closing connection")
                    break
                except Exception as e:
                    logger.error(f"Stream yield error: {e}")
                    break

        headers = {
            'Content-Disposition': f'attachment; filename="{file_name}"',
            'Cache-Control': 'no-cache',
            'X-Content-Type-Options': 'nosniff',
        }
        if file_size:
            headers['Content-Length'] = str(file_size)
            
        return Response(stream_wrapper(), headers=headers, mimetype='application/octet-stream')
    except Exception as e: 
        logger.error(f"Download error: {e}")
        return str(e), 500

@app.route("/api/deleted-messages/<chat_id>")
@login_required
def get_deleted_messages(chat_id):
    try: chat_id = int(chat_id)
    except: pass
    msgs = MessageStore.query.filter_by(chat_id=chat_id).order_by(MessageStore.date.desc()).all()
    return jsonify({"messages": [{
        "text": m.text,
        "date": m.date.isoformat() if m.date else None
    } for m in msgs]})

@app.route("/")
def index():
    if not session.get('app_authenticated'): return redirect(url_for('app_login'))
    if session.get("session_string"): return redirect(url_for("view_account"))
    return render_template("index.html")

@app.route("/login", methods=["GET", "POST"])
def app_login():
    if request.method == "POST":
        if request.form.get("password") == APP_PASSWORD:
            session['app_authenticated'] = True
            return redirect(url_for('index'))
        return render_template("login.html", error="Invalid password")
    return render_template("login.html")

@app.route("/view", methods=["GET", "POST"])
@login_required
def view_account():
    session_string = request.form.get("session_string") or session.get("session_string")
    if not session_string: return redirect(url_for("index"))
    session["session_string"] = session_string
    logger.info(f"Viewing account with session string starting with: {session_string[:10]}...")
    result = run_async(get_account_info(session_string))
    if "error" in result: 
        logger.error(f"Error getting account info: {result['error']}")
        return render_template("index.html", error=result["error"])
    return render_template("account.html", account=result)

@app.route("/chat/<chat_id>")
@login_required
def view_chat(chat_id):
    logger.info(f"Viewing chat: {chat_id}")
    offset_id = int(request.args.get("offset_id", 0))
    media_only = request.args.get("media_only") == "true"
    result = run_async(get_messages_from_chat(session.get("session_string"), chat_id, offset_id=offset_id, media_only=media_only))
    if "error" in result: 
        logger.error(f"Error getting messages for chat {chat_id}: {result['error']}")
        return redirect(url_for("view_account"))
    return render_template("chat.html", 
        messages=result["messages"], 
        chat_id=chat_id, 
        chat_name=result["chat_name"],
        can_manage=result["can_manage"],
        media_only=media_only,
        has_more=result.get("has_more", True)
    )

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for('app_login'))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

