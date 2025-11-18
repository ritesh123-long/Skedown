# app.py
import os
import threading
import time
import hashlib
import sqlite3
from datetime import datetime, timedelta
from urllib.parse import unquote_plus
from flask import Flask, request, jsonify, send_from_directory, abort
from yt_dlp import YoutubeDL
from werkzeug.utils import secure_filename

# ---------- Config ----------
STORAGE_DIR = os.environ.get("STORAGE_DIR", "downloads")
DB_PATH = os.environ.get("DB_PATH", "downloads.db")
CLEANUP_INTERVAL_SECONDS = 60 * 60     # 1 hour
FILE_TTL_HOURS = int(os.environ.get("FILE_TTL_HOURS", "24"))
MAX_WAIT_SECONDS = int(os.environ.get("MAX_WAIT_SECONDS", "120"))
API_KEY = os.environ.get("API_KEY", "")  # set in env for protection (optional)
# ----------------------------

os.makedirs(STORAGE_DIR, exist_ok=True)
app = Flask(__name__)

# ---------- Database setup ----------
def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS downloads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cache_key TEXT UNIQUE,
        origin_url TEXT,
        fmt TEXT,
        quality TEXT,
        filename TEXT,
        created_at TEXT,
        expires_at TEXT,
        status TEXT, -- pending, processing, done, error, expired
        error_msg TEXT,
        progress REAL DEFAULT 0,            -- percent 0..100
        downloaded_bytes INTEGER DEFAULT 0,
        total_bytes INTEGER DEFAULT 0
    )
    """)
    conn.commit()
    return conn

DB = init_db()
DB_LOCK = threading.Lock()

def get_entry(cache_key):
    with DB_LOCK:
        c = DB.cursor()
        c.execute("SELECT id,cache_key,origin_url,fmt,quality,filename,created_at,expires_at,status,error_msg,progress,downloaded_bytes,total_bytes FROM downloads WHERE cache_key = ?", (cache_key,))
        r = c.fetchone()
        return r

def upsert_entry(cache_key, origin_url, fmt, quality, filename, status, error_msg=None):
    now = datetime.utcnow()
    expires = now + timedelta(hours=FILE_TTL_HOURS)
    with DB_LOCK:
        c = DB.cursor()
        existing = get_entry(cache_key)
        if existing:
            c.execute("""UPDATE downloads SET origin_url=?,fmt=?,quality=?,filename=?,created_at=?,expires_at=?,status=?,error_msg=?
                         WHERE cache_key=?""",
                      (origin_url, fmt, quality, filename, now.isoformat(), expires.isoformat(), status, error_msg, cache_key))
        else:
            c.execute("""INSERT INTO downloads (cache_key,origin_url,fmt,quality,filename,created_at,expires_at,status,error_msg)
                         VALUES (?,?,?,?,?,?,?,?,?)""",
                      (cache_key, origin_url, fmt, quality, filename, now.isoformat(), expires.isoformat(), status, error_msg))
        DB.commit()

def set_status(cache_key, status, error_msg=None):
    with DB_LOCK:
        c = DB.cursor()
        if error_msg is not None:
            c.execute("UPDATE downloads SET status=?, error_msg=? WHERE cache_key=?", (status, error_msg, cache_key))
        else:
            c.execute("UPDATE downloads SET status=? WHERE cache_key=?", (status, cache_key))
        DB.commit()

def update_progress(cache_key, percent=None, downloaded=None, total=None):
    with DB_LOCK:
        c = DB.cursor()
        updates = []
        params = []
        if percent is not None:
            updates.append("progress=?"); params.append(percent)
        if downloaded is not None:
            updates.append("downloaded_bytes=?"); params.append(downloaded)
        if total is not None:
            updates.append("total_bytes=?"); params.append(total)
        if not updates:
            return
        params.append(cache_key)
        sql = f"UPDATE downloads SET {', '.join(updates)} WHERE cache_key=?"
        c.execute(sql, tuple(params))
        DB.commit()

# ---------- Utilities ----------
def make_cache_key(url, fmt, quality):
    key_raw = f"{url}|{fmt}|{quality}"
    return hashlib.sha256(key_raw.encode("utf-8")).hexdigest()

def sanitize_filename(s):
    return secure_filename(s) or "file"

# ---------- Progress hook for yt-dlp ----------
def make_progress_hook(cache_key):
    def hook(d):
        try:
            status = d.get('status')
            if status == 'downloading':
                downloaded = d.get('downloaded_bytes') or 0
                total = d.get('total_bytes') or 0
                percent = None
                if total and downloaded:
                    percent = round(downloaded / total * 100, 2)
                # Lightweight DB update
                update_progress(cache_key, percent=percent, downloaded=downloaded, total=total)
            elif status == 'finished':
                # finished downloading to a temp file; finalization will follow in worker
                update_progress(cache_key, percent=100, downloaded=d.get('downloaded_bytes') or 0, total=d.get('total_bytes') or 0)
        except Exception:
            # Never let hook raise
            pass
    return hook

# ---------- Download worker ----------
def download_worker(cache_key, url, fmt, quality):
    try:
        set_status(cache_key, "processing")
        ydl_opts = {
            "outtmpl": os.path.join(STORAGE_DIR, "%(id)s.%(ext)s"),
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "progress_hooks": [ make_progress_hook(cache_key) ],
            # Additional options can be set here
        }

        # Format selection
        if fmt == "music":
            ydl_opts.update({
                "format": "bestaudio/best",
                "postprocessors": [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
            })
        else:
            # quality like "720p" -> use height selector
            if quality and quality.lower() not in ("best","worst"):
                if quality.endswith("p"):
                    height = ''.join(ch for ch in quality if ch.isdigit())
                    if height:
                        ydl_opts["format"] = f"bestvideo[height<={height}]+bestaudio/best"
                    else:
                        ydl_opts["format"] = "best"
                else:
                    ydl_opts["format"] = quality
            else:
                ydl_opts["format"] = "best"

        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if "entries" in info:
                info = info["entries"][0]
            filename = ydl.prepare_filename(info)

            # If postprocessor converted, try common extensions
            if not os.path.exists(filename):
                base = os.path.splitext(filename)[0]
                for ext in [".mp3", ".m4a", ".mp4", ".webm", ".mkv", ".opus"]:
                    cand = base + ext
                    if os.path.exists(cand):
                        filename = cand
                        break

            # finalize filename and move to unique name
            safe = sanitize_filename(os.path.basename(filename))
            unique_name = f"{hashlib.sha1((cache_key+safe).encode()).hexdigest()}_{safe}"
            dest = os.path.join(STORAGE_DIR, unique_name)
            try:
                os.replace(filename, dest)
            except Exception:
                # fallback: copy
                import shutil
                shutil.copyfile(filename, dest)
            upsert_entry(cache_key, url, fmt, quality, unique_name, "done")
            update_progress(cache_key, percent=100)
    except Exception as e:
        set_status(cache_key, "error", str(e))

# ---------- Cleanup thread ----------
def cleanup_loop():
    while True:
        try:
            now = datetime.utcnow()
            with DB_LOCK:
                c = DB.cursor()
                c.execute("SELECT cache_key,filename,expires_at,status FROM downloads")
                rows = c.fetchall()
                for cache_key, filename, expires_at, status in rows:
                    try:
                        expires = datetime.fromisoformat(expires_at)
                    except Exception:
                        continue
                    path = os.path.join(STORAGE_DIR, filename) if filename else None
                    if expires < now:
                        if path and os.path.exists(path):
                            try:
                                os.remove(path)
                            except:
                                pass
                        c.execute("UPDATE downloads SET status=? WHERE cache_key=?", ("expired", cache_key))
                DB.commit()
        except Exception:
            pass
        time.sleep(CLEANUP_INTERVAL_SECONDS)

cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
cleanup_thread.start()

# ---------- Routes ----------
def require_api_key():
    if API_KEY:
        key = request.headers.get("X-API-KEY") or request.args.get("api_key")
        if key != API_KEY:
            abort(401, "Invalid API key")

@app.route("/<fmt>/<quality>", methods=["GET"])
def download_route(fmt, quality):
    """
    Example: GET /video/720p?url=...&wait=false
    fmt: video or music
    quality: 720p, best, 128k etc.
    """
    require_api_key()
    url = request.args.get("url")
    if not url:
        return jsonify({"error":"missing url parameter"}), 400
    url = unquote_plus(url)
    wait = request.args.get("wait", "false").lower() == "true"
    cache_key = make_cache_key(url, fmt, quality)
    entry = get_entry(cache_key)

    if entry:
        # entry tuple: (id,cache_key,origin_url,fmt,quality,filename,created_at,expires_at,status,error_msg,progress,downloaded_bytes,total_bytes)
        (_id, _ck, origin_url, _fmt, _quality, filename, created_at, expires_at, status, error_msg, progress, downloaded_bytes, total_bytes) = entry
        if status == "done" and filename:
            file_path = os.path.join(STORAGE_DIR, filename)
            if os.path.exists(file_path):
                download_url = request.url_root.rstrip("/") + "/files/" + filename
                return jsonify({"status":"done","download_url":download_url,"cached":True,"progress":100}), 200
        if status in ("processing","pending"):
            return jsonify({"status":status,"progress":progress or 0,"downloaded_bytes":downloaded_bytes or 0,"total_bytes":total_bytes or 0}), 202
        if status == "error":
            # allow retry by starting new job
            pass
        # expired -> continue to start a fresh job

    # start background job
    upsert_entry(cache_key, url, fmt, quality, None, "pending")
    worker = threading.Thread(target=download_worker, args=(cache_key, url, fmt, quality), daemon=True)
    worker.start()

    if wait:
        start = time.time()
        while time.time() - start < MAX_WAIT_SECONDS:
            e = get_entry(cache_key)
            if e:
                status = e[8]
                filename = e[5]
                progress = e[10] or 0
                if status == "done" and filename:
                    download_url = request.url_root.rstrip("/") + "/files/" + filename
                    return jsonify({"status":"done","download_url":download_url,"cached":False,"progress":100}), 200
                if status == "error":
                    return jsonify({"status":"error","message":e[9]}), 500
            time.sleep(1)
        return jsonify({"status":"processing","message":"download started, wait timed out. Poll /status?cache_key=...","progress":None}), 202
    else:
        return jsonify({"status":"processing","message":"download started. Poll /status?cache_key=...","progress":None}), 202

@app.route("/status", methods=["GET"])
def status_route():
    require_api_key()
    cache_key = request.args.get("cache_key")
    if not cache_key:
        return jsonify({"error":"missing cache_key"}), 400
    e = get_entry(cache_key)
    if not e:
        return jsonify({"error":"not_found"}), 404
    (_id, _ck, origin_url, fmt, quality, filename, created_at, expires_at, status, error_msg, progress, downloaded_bytes, total_bytes) = e
    resp = {
        "cache_key": cache_key,
        "origin_url": origin_url,
        "fmt": fmt,
        "quality": quality,
        "status": status,
        "created_at": created_at,
        "expires_at": expires_at,
        "progress": progress or 0,
        "downloaded_bytes": downloaded_bytes or 0,
        "total_bytes": total_bytes or 0
    }
    if status == "done" and filename:
        resp["download_url"] = request.url_root.rstrip("/") + "/files/" + filename
    if error_msg:
        resp["error_msg"] = error_msg
    return jsonify(resp)

@app.route("/files/<path:fname>", methods=["GET"])
def files_route(fname):
    path = os.path.join(STORAGE_DIR, fname)
    if not os.path.exists(path):
        return abort(404)
    return send_from_directory(STORAGE_DIR, fname, as_attachment=True)

@app.route("/health")
def health():
    return jsonify({"status":"ok","time":datetime.utcnow().isoformat()})

if __name__ == "__main__":
    # for local dev only; Render/Gunicorn will use gunicorn start command
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
