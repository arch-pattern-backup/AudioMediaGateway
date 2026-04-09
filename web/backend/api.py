from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from celery import Celery
import os
import sys
import json
import logging
from typing import List, Optional, Dict
from pydantic import BaseModel
import redis
import sqlalchemy
from sqlalchemy import text
import boto3
from botocore.config import Config
import urllib.parse

# Add parent directory to path to import local modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config_manager import ConfigManager
from toneroot_downloader import ToneRootDownloader

# --- Configuration ---
base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONFIG_FILE = os.path.join(base_path, "config.json")
config_manager = ConfigManager(CONFIG_FILE)

# Shared Redis for Celery and Status
REDIS_URL = config_manager.get("redis_url", "redis://redis.toneroot.svc.cluster.local:6379/0")
r_client = redis.from_url(REDIS_URL)

# Postgres Connection (Shared with StoryLoom)
DB_USER = os.getenv("DB_USER", "app_user")
DB_PASS = os.getenv("DB_PASS", "app_password")
# Use 'db' as it corresponds to the ClusterIP service found earlier
DB_HOST = os.getenv("DB_HOST", "db.infra-data.svc.cluster.local")
DB_NAME = os.getenv("DB_NAME", "mydatabase")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

engine = sqlalchemy.create_engine(DATABASE_URL, pool_pre_ping=True)

def _ensure_db_setup():
    """Create the songs table if it doesn't exist."""
    print("Checking database schema...")
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            # Create table if it doesn't exist (using JSONB for flexible metadata)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS songs (
                    uuid TEXT PRIMARY KEY,
                    title TEXT,
                    artist TEXT,
                    s3_key TEXT,
                    local_path TEXT,
                    created_at TEXT,
                    metadata_json JSONB,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            # Indices for performance and future search features
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_songs_title ON songs(title)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_songs_artist ON songs(artist)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_songs_created ON songs(created_at)"))
            conn.commit()
            print("Database schema verified.")
    except Exception as e:
        print(f"Database schema setup failed: {e}")

# Celery Setup
celery_app = Celery('toneroot', broker=REDIS_URL, backend=REDIS_URL)
celery_app.conf.task_track_started = True

app = FastAPI(title="ToneRoot Web API")

@app.get("/health")
async def health_check():
    """Simple health check for Kubernetes probes."""
    return {"status": "healthy"}

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    _ensure_db_setup()
    # Auto-sync on startup if DB is empty? (Handled in list_songs fallback too)

# --- Models ---
class TokenUpdate(BaseModel):
    token: str

class DownloadRequest(BaseModel):
    token: Optional[str] = None
    max_pages: int = 5
    start_page: int = 1
    organize_by_month: bool = True
    organize_by_track: bool = False
    prefer_wav: bool = False

class Song(BaseModel):
    uuid: str
    title: Optional[str] = None
    artist: Optional[str] = None
    s3_key: Optional[str] = None
    local_path: Optional[str] = None
    created_at: Optional[str] = None
    metadata: Optional[dict] = None

# --- Helpers ---
def get_db():
    try:
        from db_manager import DBManager
        # Use sunosync.db which we found exists in /app/
        db_path = os.path.join(base_path, "sunosync.db")
        return DBManager(db_path)
    except Exception as e:
        print(f"Database initialization failed: {e}")
        return None

# --- Tasks ---
@celery_app.task(bind=True)
def run_downloader_task(self, params: Dict):
    """Background task to run the ToneRootDownloader."""
    self.update_state(state='PROGRESS', meta={'msg': 'Initializing downloader...'})
    
    # Initialize Downloader
    downloader = ToneRootDownloader()
    
    # Bridge Signals to Redis for real-time frontend updates
    def on_log(msg, mtype="info"):
        status = {"msg": msg, "type": mtype, "task_id": self.request.id}
        r_client.publish("toneroot_logs", json.dumps(status))
        r_client.set(f"toneroot_status:{self.request.id}:last_msg", json.dumps(status))

    def on_song_updated(uuid, status_msg, progress):
        update = {"uuid": uuid, "status": status_msg, "progress": progress}
        r_client.publish("toneroot_progress", json.dumps(update))

    downloader.signals.log_message.connect(on_log)
    downloader.signals.song_updated.connect(on_song_updated)
    
    # Configure and Run
    token = params.get("token") or config_manager.get("token")
    directory = config_manager.get("path") or config_manager.get("directory") or base_path
    
    s3_conf = {
        "endpoint": config_manager.get("s3_endpoint"),
        "bucket": config_manager.get("s3_bucket"),
        "region": config_manager.get("s3_region"),
        "access_key": config_manager.get("s3_access_key"),
        "secret_key": config_manager.get("s3_secret_key"),
        "prefix": config_manager.get("s3_path_prefix")
    }

    downloader.configure(
        token=token,
        directory=directory,
        max_pages=params.get("max_pages", 5),
        start_page=params.get("start_page", 1),
        organize_by_month=params.get("organize_by_month", True),
        embed_metadata_enabled=True,
        prefer_wav=params.get("prefer_wav", False),
        download_delay=1.0,
        organize_by_track=params.get("organize_by_track", False),
        storage_type=config_manager.get("storage_type", "local"),
        s3_config=s3_conf
    )
    
    downloader.db = get_db()
    
    try:
        downloader.run()
        return {"status": "completed"}
    except Exception as e:
        on_log(f"Downloader failed: {str(e)}", "error")
        raise e

# --- Helpers ---
def get_inventory():
    """Helper to fetch and merge inventory from S3 and local file."""
    combined_items = {}
    
    # 1. Try S3
    try:
        s3_conf = {
            "endpoint": config_manager.get("s3_endpoint"),
            "bucket": config_manager.get("s3_bucket"),
            "access_key": config_manager.get("s3_access_key"),
            "secret_key": config_manager.get("s3_secret_key"),
        }
        if all([s3_conf["endpoint"], s3_conf["bucket"], s3_conf["access_key"], s3_conf["secret_key"]]):
            print("Fetching inventory from S3 for sync...")
            s3_client = boto3.client(
                's3',
                endpoint_url=s3_conf['endpoint'],
                aws_access_key_id=s3_conf['access_key'],
                aws_secret_access_key=s3_conf['secret_key'],
                config=Config(signature_version='s3v4', connect_timeout=2, read_timeout=2)
            )
            response = s3_client.get_object(Bucket=s3_conf['bucket'], Key="s3_inventory.json")
            s3_data = json.loads(response['Body'].read().decode('utf-8'))
            s3_items = s3_data.get("items", {})
            combined_items.update(s3_items)
    except Exception as e:
        print(f"S3 inventory sync fetch failed: {e}")

    # 2. Try Local
    try:
        inventory_path = os.path.join(base_path, "s3_inventory.json")
        if os.path.exists(inventory_path):
            with open(inventory_path, 'r') as f:
                local_data = json.load(f)
                local_items = local_data.get("items", {})
                combined_items.update(local_items)
    except Exception as e:
        print(f"Local inventory load failed: {e}")

    return {"items": combined_items}

async def sync_inventory_to_db():
    """Task to synchronize JSON inventory into PostgreSQL."""
    print("Starting background inventory-to-DB sync...")
    try:
        data = get_inventory()
        items = data.get("items", {})
        if not items:
            print("No items found to sync.")
            return

        with engine.connect() as conn:
            for uuid, item in items.items():
                metadata = item.get("metadata", {})
                # Promote artist/title if missing in top-level but present in metadata
                title = item.get("title") or metadata.get("title")
                artist = item.get("artist") or metadata.get("display_name")
                
                conn.execute(text("""
                    INSERT INTO songs (uuid, title, artist, s3_key, local_path, created_at, metadata_json, last_updated)
                    VALUES (:uuid, :title, :artist, :s3_key, :local_path, :created_at, :metadata_json, CURRENT_TIMESTAMP)
                    ON CONFLICT (uuid) DO UPDATE SET
                        title = EXCLUDED.title,
                        artist = EXCLUDED.artist,
                        s3_key = EXCLUDED.s3_key,
                        local_path = EXCLUDED.local_path,
                        created_at = EXCLUDED.created_at,
                        metadata_json = EXCLUDED.metadata_json,
                        last_updated = CURRENT_TIMESTAMP
                """), {
                    "uuid": uuid,
                    "title": title,
                    "artist": artist,
                    "s3_key": item.get("s3_key"),
                    "local_path": item.get("local_path"),
                    "created_at": item.get("created_at"),
                    "metadata_json": json.dumps(metadata)
                })
            conn.commit()
            # Clear cache after sync to ensure fresh data
            r_client.delete("toneroot_songs_count")
            print(f"Successfully synced {len(items)} items to PostgreSQL.")
    except Exception as e:
        print(f"Sync failed: {e}")

# --- Endpoints ---
@app.post("/api/admin/sync")
async def trigger_sync(background_tasks: BackgroundTasks):
    """Trigger a manual synchronization of JSON inventory to DB."""
    background_tasks.add_task(sync_inventory_to_db)
    return {"status": "Sync started in background"}

@app.post("/api/token")
async def update_token(req: TokenUpdate):
    """Update the Suno session token in config."""
    config_manager.set("token", req.token)
    config_manager.save()
    return {"status": "Token updated successfully"}

@app.get("/api/songs", response_model=List[Song])
async def list_songs(
    search: Optional[str] = None, 
    limit: int = 100, 
    offset: int = 0,
    background_tasks: BackgroundTasks = None
):
    """Returns songs from PostgreSQL with Search, Pagination, and Redis Caching."""
    cache_key = f"songs_list:{search}:{limit}:{offset}"
    
    # 1. Try Redis Cache
    try:
        cached_data = r_client.get(cache_key)
        if cached_data:
            print(f"Serving {cache_key} from cache.")
            return json.loads(cached_data)
    except Exception as e:
        print(f"Redis cache Read failed: {e}")

    # 2. Query PostgreSQL
    songs = []
    try:
        with engine.connect() as conn:
            query_str = "SELECT * FROM songs"
            params = {"limit": limit, "offset": offset}
            
            if search:
                query_str += " WHERE title ILIKE :search OR artist ILIKE :search"
                params["search"] = f"%{search}%"
            
            query_str += " ORDER BY created_at DESC NULLS LAST LIMIT :limit OFFSET :offset"
            
            result = conn.execute(text(query_str), params)
            for row in result:
                songs.append({
                    "uuid": row.uuid,
                    "title": row.title,
                    "artist": row.artist,
                    "s3_key": row.s3_key,
                    "local_path": row.local_path,
                    "created_at": row.created_at,
                    "metadata": row.metadata_json if isinstance(row.metadata_json, dict) else json.loads(row.metadata_json or "{}")
                })
        
        # 3. If DB is empty, trigger an auto-sync and try one last fallback to inventory
        if not songs and offset == 0:
            print("DB empty, triggering auto-sync...")
            if background_tasks:
                background_tasks.add_task(sync_inventory_to_db)
            
            # Temporary fallback to ensure the user sees SOMETHING while sync runs
            data = get_inventory()
            items = data.get("items", {})
            for uuid, item in list(items.items())[:limit]:
                songs.append({
                    "uuid": uuid,
                    "title": item.get("title"),
                    "artist": item.get("artist"),
                    "s3_key": item.get("s3_key"),
                    "local_path": item.get("local_path"),
                    "created_at": item.get("created_at"),
                    "metadata": item.get("metadata")
                })
        
        # 4. Cache Result
        if songs:
            try:
                r_client.setex(cache_key, 300, json.dumps(songs)) # Cache for 5 mins
            except Exception as e:
                print(f"Redis cache write failed: {e}")
                
    except Exception as e:
        print(f"Database query failed: {e}")
        # Fallback to direct inventory on DB failure
        data = get_inventory()
        items = data.get("items", {})
        for uuid, item in list(items.items())[offset:offset+limit]:
            songs.append({
                "uuid": uuid,
                "title": item.get("title"),
                "artist": item.get("artist"),
                "s3_key": item.get("s3_key"),
                "local_path": item.get("local_path"),
                "created_at": item.get("created_at"),
                "metadata": item.get("metadata")
            })

    return songs

@app.post("/api/download")
async def start_download(req: DownloadRequest):
    task = run_downloader_task.delay(req.dict())
    return {"task_id": task.id, "status": "queued"}

@app.get("/api/download/status/{task_id}")
async def get_download_status(task_id: str):
    task = run_downloader_task.AsyncResult(task_id)
    last_msg = r_client.get(f"toneroot_status:{task_id}:last_msg")
    
    return {
        "task_id": task_id,
        "state": task.state,
        "last_log": json.loads(last_msg) if last_msg else None,
        "info": task.info if isinstance(task.info, dict) else str(task.info)
    }

@app.get("/api/stream/{uuid}")
async def stream_song(uuid: str):
    s3_conf = {
        "endpoint": config_manager.get("s3_endpoint"),
        "bucket": config_manager.get("s3_bucket"),
        "region": config_manager.get("s3_region"),
        "access_key": config_manager.get("s3_access_key"),
        "secret_key": config_manager.get("s3_secret_key"),
    }
    
    s3_key = None
    
    # 1. Try Inventory (S3 then Local)
    data = get_inventory()
    s3_key_raw = data.get("items", {}).get(uuid, {}).get("s3_key")
    if s3_key_raw:
        s3_key = urllib.parse.unquote(s3_key_raw)

    # 2. Try PostgreSQL as fallback
    if not s3_key:
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT s3_key FROM songs WHERE uuid = :uuid"), {"uuid": uuid})
                row = result.fetchone()
                if row:
                    s3_key = row[0] # s3_key is at index 0
                    if s3_key:
                        s3_key = urllib.parse.unquote(s3_key)
        except Exception as e:
            print(f"Postgres key lookup failed: {e}")

    # Use public endpoint for pre-signed URLs if available
    public_endpoint = config_manager.get("s3_public_endpoint") or s3_conf.get("endpoint")
    
    if s3_key and all([s3_conf["endpoint"], s3_conf["bucket"], s3_conf["access_key"], s3_conf["secret_key"]]):
        s3_client = boto3.client(
            's3',
            endpoint_url=public_endpoint,
            aws_access_key_id=s3_conf.get('access_key'),
            aws_secret_access_key=s3_conf.get('secret_key'),
            config=Config(signature_version='s3v4', connect_timeout=2, read_timeout=2)
        )
        url = s3_client.generate_presigned_url(
            'get_object', Params={'Bucket': s3_conf['bucket'], 'Key': s3_key}, ExpiresIn=3600
        )
        return RedirectResponse(url)

    raise HTTPException(status_code=404, detail="Audio file not found")

@app.get("/api/storage/status")
async def storage_status():
    import requests
    url = config_manager.get("storage_status_url", "http://localhost:5000/api/storage/status")
    try:
        r = requests.get(url, timeout=2)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return {"mode": "Local", "egress_pct": 0, "status": "standalone"}

if __name__ == "__main__":
    import uvicorn
    # Use reload=True for development/Tilt syncs
    uvicorn.run("api:app", host="0.0.0.0", port=8001, reload=True)
