#!/bin/bash
set -e

if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

mkdir -p server_downloads

exec gunicorn -w 1 --threads 4 -b 127.0.0.1:5000 --timeout 120 --access-logfile - app:app
