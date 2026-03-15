import os
import json
import requests
import time
import traceback

try:
    import boto3
    from botocore.config import Config
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

from suno_downloader import SunoDownloader, GEN_API_BASE
from db_manager import DBManager
from config_manager import ConfigManager

def run_integrity_sync(config_mgr=None, log_callback=None, clear_db=False):
    def log(msg, mtype="info"):
        if log_callback:
            log_callback(msg, mtype)
        else:
            print(f"[{mtype.upper()}] {msg}" if mtype != "info" else msg)

    if not BOTO3_AVAILABLE:
        log("CRITICAL ERROR: boto3 is not installed. S3 sync cannot continue.", "error")
        return False

    if log_callback:
        log("=== SunoSync S3 Integrity & Inventory Sync ===", "info")
    else:
        print("=== SunoSync S3 Integrity & Inventory Sync ===")
    
    # 1. Load Configuration
    if not config_mgr:
        config_mgr = ConfigManager()
    
    config = config_mgr.config
    
    token = config.get("token")
    directory = config.get("path") or config.get("directory")
    storage_type = config.get("storage_type")
    
    def log(msg, mtype="info"):
        if log_callback:
            log_callback(msg, mtype)
        else:
            print(f"[{mtype.upper()}] {msg}" if mtype != "info" else msg)

    if not token or not directory:
        log("Error: Token or Directory not configured. Please set them in the app first.", "error")
        return

    if storage_type != "s3":
        log("Error: Storage type is not set to S3. This script is for S3 inventory sync.", "error")
        return

    # ... rest of config ...
    s3_conf = {
        "endpoint": config.get("s3_endpoint"),
        "bucket": config.get("s3_bucket"),
        "region": config.get("s3_region"),
        "access_key": config.get("s3_access_key"),
        "secret_key": config.get("s3_secret_key"),
        "prefix": config.get("s3_path_prefix")
    }

    # 2. Initialize Downloader (to reuse its logic)
    downloader = SunoDownloader()
    downloader.configure(
        token=token,
        directory=directory,
        max_pages=0,
        start_page=1,
        organize_by_month=config.get("organize_by_month", True),
        embed_metadata_enabled=True,
        prefer_wav=config.get("prefer_wav", False),
        download_delay=0,
        storage_type="s3",
        s3_config=s3_conf,
        organize_by_track=config.get("organize_by_track", False)
    )
    
    db_path = os.path.join(directory, "sunosync.db")
    
    if clear_db and os.path.exists(db_path):
        log(f"Clearing existing database for fresh rebuild: {db_path}", "warning")
        try:
            os.remove(db_path)
        except Exception as e:
            log(f"Failed to clear DB: {e}", "error")

    db = DBManager(db_path)
    downloader.db = db # Attach DB to downloader

    log(f"Database: {db_path}")
    log(f"S3 Bucket: {s3_conf['bucket']}")

    # 3. List all keys currently in S3
    log("Listing objects in S3 (this may take a minute)...")
    s3_keys = downloader._list_s3_keys()
    
    if not s3_keys:
        log("CRITICAL ERROR: No objects found in S3 bucket or connection failed.", "error")
        log("Check your S3 configuration and ensures your MinIO port-forward is active.", "error")
        return False

    log(f"Successfully listed {len(s3_keys)} objects on S3.")

    # 4. Fetch full library from Suno API
    log("Fetching full library from Suno API...")
    headers = {"Authorization": f"Bearer {token}"}
    all_suno_songs = []
    page = 1
    consecutive_429s = 0
    
    while True:
        try:
            url = f"{GEN_API_BASE}/api/feed/?page={page}"
            r = requests.get(url, headers=headers, timeout=30)
            
            if r.status_code == 429:
                consecutive_429s += 1
                wait_time = min(60, (2 ** consecutive_429s) + 5) # Exponential backoff up to 60s
                log(f"Rate limited (429). Waiting {wait_time} seconds before retry...", "warning")
                time.sleep(wait_time)
                continue
            
            if r.status_code != 200:
                log(f"API Fetch ended at page {page} (Status: {r.status_code})")
                break
            
            consecutive_429s = 0 # Reset on success
            data = r.json()
            clips = data if isinstance(data, list) else data.get('clips', [])
            if not clips:
                log(f"No more clips found at page {page}")
                break
            
            for clip in clips:
                if isinstance(clip, dict) and "clip" in clip:
                    all_suno_songs.append(clip["clip"])
                else:
                    all_suno_songs.append(clip)
            
            if page % 10 == 0:
                log(f"  Fetched {len(all_suno_songs)} songs so far (Page {page})...")
            
            page += 1
            time.sleep(0.5) # Be kind to API
        except Exception as e:
            log(f"Error fetching page {page}: {e}", "error")
            break

    if not all_suno_songs:
        log("CRITICAL ERROR: Failed to fetch songs from Suno API. Check your token.", "error")
        return False

    log(f"Total songs in Suno Library to process: {len(all_suno_songs)}")

    # 5. Match Library to S3 Keys and Update DB
    log("Matching Suno library against S3 keys using current organizational settings...")
    matched_count = 0
    skipped_count = 0
    
    # Debug: Sample the first few generated keys to see why they might miss
    debug_samples = 0

    for song in all_suno_songs:
        uuid = song.get("id")
        if not uuid: continue
        
        # Check both mp3 and wav extensions
        found_key = None
        tried_keys = []
        for ext in [".mp3", ".wav"]:
            expected_key = downloader._get_expected_s3_key(song, ext)
            tried_keys.append(expected_key)
            if expected_key in s3_keys:
                found_key = expected_key
                break
        
        if found_key:
            # We have a match! Update DB with full metadata
            song["s3_key"] = found_key
            song["local_path"] = None
            
            # Ensure lyrics are present in metadata if found
            metadata = song.get("metadata", {})
            if not isinstance(metadata, dict):
                metadata = {}
                
            lyrics = metadata.get("lyrics") or metadata.get("text") or song.get("prompt")
            if lyrics:
                metadata["lyrics_sync"] = lyrics
                song["metadata"] = metadata

            db.upsert_song(song)
            matched_count += 1
            if matched_count % 100 == 0:
                log(f"  Matched {matched_count} songs...")
        else:
            skipped_count += 1
            if debug_samples < 3:
                log(f"  DEBUG: No S3 match for '{song.get('title') or uuid}'. Tried keys: {tried_keys}", "info")
                debug_samples += 1

    log(f"Sync Result: {matched_count} songs matched and updated in Nexus. {skipped_count} songs in Suno library not found on S3.", "success")

    # 6. Export Final Manifest
    log("Exporting and uploading final JSON manifest...")
    try:
        # Explicitly flush DB to disk by deleting manager object or similar 
        # But DBManager uses context managers, so it should be fine.
        
        s3_robust_config = Config(
            signature_version='s3v4',
            retries={'max_attempts': 10, 'mode': 'standard'},
            connect_timeout=15,
            read_timeout=60
        )
        s3_client = boto3.client(
            's3',
            endpoint_url=s3_conf.get('endpoint'),
            aws_access_key_id=s3_conf.get('access_key'),
            aws_secret_access_key=s3_conf.get('secret_key'),
            config=s3_robust_config
        )
        
        # Verify DB file size before export
        db_size = os.path.getsize(db_path)
        log(f"Finalizing Database (Size: {db_size} bytes)...")
        
        downloader._generate_and_upload_manifest(directory, s3_client, s3_conf)
        log(f"Success: s3_inventory.json ({matched_count} items) is now up-to-date and uploaded to S3.", "success")
        return True
    except Exception as e:
        log(f"Failed to upload final manifest: {e}", "error")
        return False

if __name__ == "__main__":
    run_integrity_sync()
