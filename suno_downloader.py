import os
import time
import traceback
import requests
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
import threading
import re
import tempfile
try:
    import boto3
    from botocore.exceptions import NoCredentialsError
    from botocore.config import Config
except ImportError:
    boto3 = None

from suno_utils import RateLimiter, build_uuid_cache, embed_metadata, sanitize_filename, get_unique_filename
from db_manager import DBManager
from mutagen.id3 import ID3
from mutagen.wave import WAVE

GEN_API_BASE = "https://studio-api.prod.suno.com"


class Signal:
    """A simple signal implementation for observer pattern."""
    def __init__(self, arg_types=None):
        self._subscribers = []
        self.arg_types = arg_types

    def connect(self, callback):
        if callback not in self._subscribers:
            self._subscribers.append(callback)

    def emit(self, *args):
        for callback in self._subscribers:
            try:
                callback(*args)
            except Exception:
                traceback.print_exc()


class DownloaderSignals:
    """Container for all signals emitted by SunoDownloader."""
    def __init__(self):
        self.status_changed = Signal(str)       # msg
        self.log_message = Signal((str, str))   # msg, type (info, error, success, downloading)
        self.progress_updated = Signal(int)     # percentage (optional usage)
        self.download_complete = Signal(bool)   # success
        self.error_occurred = Signal(str)       # error message
        self.thumbnail_fetched = Signal((bytes, str)) # data, title/id context
        
        # New Signals for Queue
        self.song_started = Signal((str, str, bytes, dict)) # uuid, title, thumbnail_data, metadata
        self.song_updated = Signal((str, str, int))   # uuid, status, progress
        self.song_finished = Signal((str, bool, str)) # uuid, success, filepath
        self.song_found = Signal((dict,))             # metadata (for preload)


class SunoDownloader:
    STEM_INDICATORS = [
        "(bass)", "(drums)", "(backing vocal)", "(backing vocals)", "(vocals)", "(instrumental)",
        "(woodwinds)", "(brass)", "(fx)", "(synth)", "(strings)", 
        "(percussion)", "(keyboard)", "(guitar)"
    ]

    def __init__(self):
        self.signals = DownloaderSignals()
        self.stop_event = threading.Event()
        self.config = {}
        self.rate_limiter = RateLimiter(0.0)

    def configure(self, token, directory, max_pages, start_page, 
                  organize_by_month, embed_metadata_enabled, prefer_wav, download_delay, 
                  filter_settings=None, scan_only=False, target_songs=None, save_lyrics=True,
                  organize_by_track=False, stems_only=False, smart_resume=False,
                  storage_type="local", s3_config=None):
        self.config = {
            "token": token,
            "directory": directory,
            "max_pages": max_pages,
            "start_page": start_page,
            "organize_by_month": organize_by_month,
            "embed_metadata": embed_metadata_enabled,
            "save_lyrics": save_lyrics,
            "prefer_wav": prefer_wav,
            "download_delay": max(0.0, float(download_delay)),
            "filter_settings": filter_settings or {},
            "scan_only": scan_only,
            "target_songs": target_songs or [], # List of dicts or UUIDs
            "organize_by_track": organize_by_track,
            "stems_only": stems_only,
            "smart_resume": smart_resume,
            "storage_type": storage_type,
            "s3_config": s3_config or {}
        }
        self.rate_limiter = RateLimiter(self.config["download_delay"])

    def stop(self):
        self.stop_event.set()

    def is_stopped(self):
        return self.stop_event.is_set()

    def _log(self, message, msg_type="info", thumbnail_data=None):
        """Internal helper to emit log signals."""
        # Also print for debug window capture
        print(f"[{msg_type.upper()}] {message}")
        self.signals.log_message.emit(message, msg_type, thumbnail_data)
        if thumbnail_data:
            self.signals.thumbnail_fetched.emit(thumbnail_data, message)

    def run(self):
        self.stop_event.clear()
        print(f"DEBUG: Starting download/preload run()")
        print(f"DEBUG: Config keys: {list(self.config.keys())}")
        
        token = self.config.get("token", "").strip()
        print(f"DEBUG: Token present: {bool(token)}, length: {len(token) if token else 0}")
        
        # Sanitize token: Remove any non-ASCII characters (e.g. ellipsis from copy-paste)
        if token:
            token = re.sub(r'[^\x00-\x7F]+', '', token)
            
        if not token:
            error_msg = "Token missing; download halted."
            self._log(error_msg, "error")
            print(f"ERROR: {error_msg}")
            self.signals.download_complete.emit(False)
            return

        directory = self.config.get("directory")
        print(f"DEBUG: Download directory: {directory}")
        if not directory:
            error_msg = "Download directory not set."
            self._log(error_msg, "error")
            print(f"ERROR: {error_msg}")
            self.signals.download_complete.emit(False)
            return

        if not os.path.exists(directory):
            os.makedirs(directory)
        
        delay = self.config.get("download_delay", 0)
        if delay > 0:
            self._log(f"Rate limiter enabled: waiting {delay:.2f}s between downloads.", "info")

        scan_only = self.config.get("scan_only", False)
        # Removed: if self.config.get("scan_only"): self._run_scan_only(); return
        # The logic below handles scan_only mode correctly.

        target_songs = self.config.get("target_songs", [])
        filters = self.config.get("filter_settings", {})
        
        headers = {"Authorization": f"Bearer {token}"}
        
        # --- CACHE INITIALIZATION ---
        self._log("Building cache of existing songs...", "info")
        existing_uuids = build_uuid_cache(directory)
        
        # Initialize SQLite DB
        db_path = os.path.join(directory, "sunosync.db")
        self.db = DBManager(db_path)
        
        is_s3 = self.config.get("storage_type") == "s3"
        s3_keys = set()
        s3_client = None
        s3_conf = self.config.get("s3_config", {})
        
        if is_s3:
            s3_keys = self._list_s3_keys()
            self._log(f"Found {len(s3_keys)} objects in S3 bucket.", "info")
            
            # Sync S3 keys to database
            self.db.bulk_upsert_s3_keys(s3_keys, s3_conf.get("bucket", ""))
            
            # Create a reusable S3 client with robust config
            try:
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
            except:
                pass

        self._log(f"Found {len(existing_uuids)} existing songs in local cache.", "info")

        # Mode 1: Download Specific Songs (from Preload)
        if target_songs:
            self.signals.status_changed.emit(f"Downloading {len(target_songs)} selected songs...")
            self._log(f"Starting download of {len(target_songs)} selected songs...", "info")
            
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = []
                for song_data in target_songs:
                    if self.is_stopped(): break
                    futures.append(
                        executor.submit(
                            self.download_single_song,
                            song_data,
                            directory,
                            headers,
                            token,
                            existing_uuids,
                            self.rate_limiter,
                            s3_client
                        )
                    )
                
                # Wait for futures but check stop event
                for future in futures:
                    if self.is_stopped():
                        executor.shutdown(wait=False, cancel_futures=True)
                        break
                    try:
                        future.result()
                    except Exception as e:
                        import traceback
                        error_msg = f"Download error: {str(e)}\n{traceback.format_exc()}"
                        self._log(error_msg, "error")
                        print(error_msg)  # Also print for debug log
            
            if self.is_stopped():
                self.signals.status_changed.emit("Stopped")
            else:
                self.signals.status_changed.emit("Complete")
            
            # Final Manifest Export (Mode 1)
            self._generate_and_upload_manifest(directory, s3_client, s3_conf)
            
            self.signals.download_complete.emit(True)
            return

        # Mode 2: Scan/Download from Feed/Workspace
        self.signals.status_changed.emit("Scanning...")
        # (Rest of URL Selection Logic)

        # --- URL Selection Logic ---
        workspace_id = filters.get("workspace_id")
        is_public = filters.get("is_public", False)
        
        params = []
        # Common params
        if filters.get("liked"): params.append("liked=true")
        if filters.get("trashed"): params.append("trashed=true")
        
        if workspace_id:
            # Workspace/Project Endpoint
            # User correction: Use /api/project/{id} (no /clips, no trailing slash before ?)
            if workspace_id == "default":
                # Assuming default project ID is "default"
                base_url = "https://studio-api.prod.suno.com/api/project/default"
            else:
                # Check if it is a playlist or project
                if filters.get("is_playlist"):
                     # Playlists might not support pagination, so we'll try without page parameter first
                     base_url = f"https://studio-api.prod.suno.com/api/playlist/{workspace_id}/"
                else:
                     base_url = f"https://studio-api.prod.suno.com/api/project/{workspace_id}"
            
            self._log(f"Fetching from {filters.get('type', 'Project')}: {filters.get('workspace_name', workspace_id)}", "info")
        elif is_public:
            # Public Feed (v2)
            base_url = "https://studio-api.prod.suno.com/api/feed/v2"
            params.append("is_public=true")
            self._log("Fetching from Public Feed", "info")
        else:
            # My Library (v1) - Default
            base_url = "https://studio-api.prod.suno.com/api/feed/"
            self._log("Fetching from My Library", "info")
            
        # Append params to base_url
        if params:
            separator = "&" if "?" in base_url else "?"
            base_url += separator + "&".join(params)
        
        # Check if this is a playlist (playlists might not support pagination)
        is_playlist = (filters and filters.get("type") == "playlist") or (filters and filters.get("is_playlist"))
        
        # Ensure URL ends with page= for the loop (unless it's a playlist)
        if not is_playlist:
            separator = "&" if "?" in base_url else "?"
            base_url += f"{separator}page="

        self._log(f"API URL: {base_url}...", "info")

        max_pages = self.config.get("max_pages", 0)
        page_num = self.config.get("start_page", 1)

        success = True
        try:
            self.signals.status_changed.emit("Fetching List...")
            self._log("Fetching song list...", "info")
            
            # Smart Resume Thresholding
            library_size = len(existing_uuids)
            if library_size < 100:
                smart_resume_threshold = 2
            elif library_size < 1000:
                smart_resume_threshold = 5
            elif library_size < 5000:
                smart_resume_threshold = 10
            else:
                smart_resume_threshold = 20
            
            # Track if we've found ANY new songs yet
            found_new_songs = False
            
            if self.config.get("smart_resume"):
                self._log(f"Smart Resume: Will stop after {smart_resume_threshold} consecutive pages with no new songs (library size: {library_size} songs).", "info")
            
            with ThreadPoolExecutor(max_workers=3) as executor:
                while not self.is_stopped():
                    if max_pages > 0 and page_num > max_pages:
                        self._log(f"Reached max pages limit ({max_pages}). Stopping.", "info")
                        break

                    self._log(f"Page {page_num}...", "info")
                    # Retry logic for fetching page
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            # For playlists, don't append page number
                            if is_playlist:
                                url = base_url
                            else:
                                url = f"{base_url}{page_num}"
                            # Increased timeout to 30s and added retry loop
                            r = requests.get(url, headers=headers, timeout=30)
                            
                            # 404 Fallback Logic: Project -> Playlist
                            if r.status_code == 404:
                                if "/api/project/" in base_url:
                                    self._log("Project endpoint 404. Switching to Playlist endpoint...", "warning")
                                    # Regex replace /api/project/ID -> /api/playlist/ID/
                                    base_url = re.sub(r"/api/project/([^?&]+)", r"/api/playlist/\1/", base_url)
                                    
                                    # Switch to playlist mode and clean URL
                                    is_playlist = True
                                    base_url = re.sub(r"[?&]page=$", "", base_url)
                                    continue # Retry immediately with new URL
                                else:
                                    self._log("Error: Resource not found (404).", "error")
                                    success = False
                                    break

                            if r.status_code == 401:
                                self._log("Error: Token expired.", "error")
                                self.signals.error_occurred.emit("Token expired. Please get a new token.")
                                success = False
                                break # Break retry loop, outer loop will also break due to success=False
                            r.raise_for_status()
                            data = r.json()
                            
                            # Debug: Log response structure for playlists
                            if is_playlist:
                                print(f"\n=== PLAYLIST API DEBUG ===")
                                print(f"URL: {url}")
                                print(f"Response Status: {r.status_code}")
                                print(f"Response Type: {type(data)}")
                                if isinstance(data, dict):
                                    print(f"Response Keys: {list(data.keys())}")
                                    # Check for various possible keys
                                    for key in ["playlist_clips", "clips", "items", "songs", "tracks", "playlist"]:
                                        if key in data:
                                            items = data[key]
                                            if isinstance(items, list):
                                                print(f"Found '{key}' with {len(items)} items")
                                                if len(items) > 0:
                                                    print(f"First item keys: {list(items[0].keys()) if isinstance(items[0], dict) else 'Not a dict'}")
                                            elif isinstance(items, dict):
                                                print(f"Found '{key}' as dict with keys: {list(items.keys())}")
                                elif isinstance(data, list):
                                    print(f"Response is a list with {len(data)} items")
                                    if len(data) > 0:
                                        print(f"First item type: {type(data[0])}")
                                        if isinstance(data[0], dict):
                                            print(f"First item keys: {list(data[0].keys())}")
                                print(f"Full Response (first 1000 chars): {str(data)[:1000]}")
                                print(f"=== END PLAYLIST DEBUG ===\n")
                                
                                self._log(f"Playlist API Response Keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}", "info")
                            
                            # If successful, break the retry loop
                            break 
                        except Exception as exc:
                            if attempt < max_retries - 1:
                                self._log(f"Connection error on page {page_num} (Attempt {attempt+1}/{max_retries}): {exc}. Retrying...", "warning")
                                time.sleep(2)
                                continue
                            else:
                                self._log(f"Request failed after {max_retries} attempts: {exc}", "error")
                                self.signals.error_occurred.emit(f"Network error on page {page_num}: {exc}")
                                success = False
                                break # Break retry loop
                    
                    if not success:
                        break # Break page loop

                    # Handle different API response structures and robustly unwrap clips
                    # 1. Project/Workspace: {"project_clips": [{"clip": {...}}, ...]}
                    # 2. Main Library: [{"id": ...}, ...] or {"clips": [...]}
                    
                    # --- WORKSPACE PARSING LOGIC ---
                    
                    # 1. Identify the list source
                    raw_data = data
                    raw_items = []
                    
                    if isinstance(raw_data, dict):
                        # Try various possible keys for playlist/workspace data
                        if "project_clips" in raw_data:
                            raw_items = raw_data["project_clips"]
                        elif "playlist_clips" in raw_data:
                            raw_items = raw_data["playlist_clips"]
                        elif "clips" in raw_data:
                            raw_items = raw_data["clips"]
                        elif "items" in raw_data:
                            raw_items = raw_data["items"]
                        elif "songs" in raw_data:
                            raw_items = raw_data["songs"]
                        elif "tracks" in raw_data:
                            raw_items = raw_data["tracks"]
                        elif "playlist" in raw_data and isinstance(raw_data["playlist"], dict):
                            # Nested playlist structure
                            playlist_data = raw_data["playlist"]
                            if "playlist_clips" in playlist_data:
                                raw_items = playlist_data["playlist_clips"]
                            elif "clips" in playlist_data:
                                raw_items = playlist_data["clips"]
                            elif "items" in playlist_data:
                                raw_items = playlist_data["items"]
                    elif isinstance(raw_data, list):
                        # Direct list of items
                        raw_items = raw_data
                    
                    if is_playlist:
                        print(f"Parsed {len(raw_items)} items from playlist response")
                        self._log(f"Parsed {len(raw_items)} items from playlist response", "info")
                        if len(raw_items) == 0:
                            print(f"\n!!! WARNING: No items found in playlist response !!!")
                            print(f"Response type: {type(data)}")
                            if isinstance(data, dict):
                                print(f"Response keys: {list(data.keys())}")
                                # Print full response structure
                                import json as json_module
                                try:
                                    response_str = json_module.dumps(data, indent=2)
                                    print(f"Full Response:\n{response_str}")
                                except Exception as e:
                                    print(f"Could not serialize response: {e}")
                                    print(f"Response repr: {repr(data)[:1000]}")
                            print(f"!!! END WARNING !!!\n")
                            
                            self._log(f"WARNING: No items found in playlist response. Response type: {type(data)}, Keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}", "warning")

                    filtered_clips = []

                    # 2. Setup Filter Flags from UI
                    filter_liked_only = filters.get("liked", False)
                    filter_hide_stems = filters.get("hide_gen_stems", False)
                    filter_exclude_trash = not filters.get("trashed", False)
                    filter_hide_disliked = filters.get("hide_disliked", False)
                    filter_public_only = filters.get("is_public", False)
                    filter_hide_studio = filters.get("hide_studio_clips", False)
                    filter_type = filters.get("type", "all")
                    search_text = filters.get("search_text", "").strip().lower()

                    # Override: If Stems Only is active, disable Hide Stems
                    if self.config.get("stems_only"):
                        filter_hide_stems = False

                    for index, item in enumerate(raw_items):
                        # A. UNWRAP STRATEGY
                        if isinstance(item, dict) and "clip" in item:
                            song_data = item["clip"]
                        else:
                            song_data = item

                        if not song_data:
                            continue

                        # B. EXTRACT VARIABLES
                        title = song_data.get("title", "") or "Unknown Title"
                        title_lower = title.lower() # Needed for search
                        
                        # Robust Liked Check
                        is_liked_bool = song_data.get("is_liked", False)
                        reaction = song_data.get("reaction", {}) 
                        if reaction is None: reaction = {} 
                        reaction_type = reaction.get("reaction_type", "")
                        vote = song_data.get("vote", "") or song_data.get("metadata", {}).get("vote", "")
                        
                        # It is liked if Boolean is True OR Reaction is 'L' OR Vote is 'up'
                        is_liked = is_liked_bool or (reaction_type == "L") or (vote == "up")

                        # Extract metadata for filters
                        metadata = song_data.get("metadata", {}) or {}
                        if metadata is None: metadata = {}
                        clip_type = metadata.get("type", "")

                        is_stem = self._is_stem(song_data)

                        # Trash Check
                        is_trashed = song_data.get("is_trashed", False)
                        
                        # Public Check
                        is_public = song_data.get("is_public", False)
                        
                        # Audio URL
                        audio_url = song_data.get("audio_url")

                        # D. APPLY FILTERS
                        
                        # 0. Audio URL (Critical)
                        if not audio_url and not scan_only:
                            continue

                        # 1. Trash Filter
                        if filter_exclude_trash and is_trashed:
                            continue

                        # 2. Stem Filter
                        if filter_hide_stems and is_stem:
                            continue

                        # 2b. Stems Only Filter
                        if self.config.get("stems_only") and not is_stem:
                            continue

                        # 3. Liked Filter
                        if filter_liked_only:
                            if not is_liked:
                                continue
                        
                        # 4. Hide Disliked
                        if filter_hide_disliked and (vote == "down" or reaction_type == "D"):
                            continue

                        # 5. Public Only
                        if filter_public_only and not is_public:
                            continue
                            
                        # 6. Hide Studio
                        if filter_hide_studio and clip_type == "studio_clip":
                            continue
                            
                        # 7. Type Filter
                        if filter_type == "uploads" and clip_type != "upload":
                            continue
                            
                        # 8. Search Text
                        if search_text:
                            tags = metadata.get("tags", "") or ""
                            prompt = metadata.get("prompt", "") or ""
                            searchable_content = f"{title_lower} {tags.lower()} {prompt.lower()}"
                            if search_text not in searchable_content:
                                continue

                        # Extract UUID
                        uuid = song_data.get("id")

                        # 9. Duplicate Check (Local Cache)
                        if uuid and uuid in existing_uuids:
                            self._log(f"Skipping {title} (UUID found in local cache)", "info")
                            continue
                            
                        # 10. S3 Duplicate Check (Key-Based)
                        if is_s3:
                            found_on_s3 = False
                            # Check for common extensions
                            for ext in [".mp3", ".wav"]:
                                expected_key = self._get_expected_s3_key(song_data, ext)
                                if expected_key in s3_keys:
                                    found_on_s3 = True
                                    break
                            
                            if found_on_s3:
                                self._log(f"Skipping {title} (found on S3: {expected_key})", "info")
                                if uuid: existing_uuids.add(uuid)
                                continue

                        # E. SUCCESS
                        filtered_clips.append(song_data)


                    if not filtered_clips:
                        self._log(f"Page {page_num}: All songs filtered out or skipped.", "info")
                    
                    # Track if we found new songs on this page
                    if filtered_clips:
                        found_new_songs = True
                        consecutive_skipped_pages = 0  # Reset counter when we find new songs
                    else:
                        # Increment skipped counter every time a page is empty
                        consecutive_skipped_pages += 1
                         
                    # Smart Resume: Stop if we've seen too many empty pages
                    # Note: We removed 'found_new_songs' check to allow stopping even if EVERYTHING is old
                    if self.config.get("smart_resume") and consecutive_skipped_pages >= smart_resume_threshold:
                        self._log(f"Smart Resume: {consecutive_skipped_pages} consecutive pages with no new songs. Library appears up-to-date. Stopping.", "success")
                        success = True
                        break
                    
                    if scan_only:
                        for clip in filtered_clips:
                            if self.is_stopped(): break
                            self.signals.song_found.emit(clip)
                    else:
                        futures = []
                        for clip in filtered_clips:
                            if self.is_stopped(): break
                            futures.append(
                                executor.submit(
                                    self.download_single_song,
                                    clip,
                                    directory,
                                    headers,
                                    token,
                                    existing_uuids,
                                    self.rate_limiter,
                                    s3_client
                                )
                            )

                        for future in futures:
                            if self.is_stopped():
                                executor.shutdown(wait=False, cancel_futures=True)
                                break
                            try:
                                future.result()
                            except Exception:
                                pass

                    # For playlists, only fetch once (no pagination)
                    if is_playlist:
                        break
                    
                    # Check if stopped before continuing to next page
                    if self.is_stopped():
                        break
                    
                    page_num += 1
                    time.sleep(1)
        except Exception as exc:
            tb = traceback.format_exc()
            self._log(f"Critical Error: {exc}\n{tb}", "error")
            self.signals.error_occurred.emit(f"Critical Error: {exc}")
            success = False

        if self.is_stopped():
            self.signals.status_changed.emit("Stopped")
        elif success:
            self.signals.status_changed.emit("Complete")
        else:
            self.signals.status_changed.emit("Error")
            
        # Final Manifest Export (Mode 2)
        self._generate_and_upload_manifest(directory, s3_client, s3_conf)

        self.signals.download_complete.emit(success)

    def fetch_workspaces(self, token):
        """Fetch list of workspaces (projects) using the correct endpoint with pagination."""
        headers = {"Authorization": f"Bearer {token}"}
        
        # Endpoint provided by user: 
        # https://studio-api.prod.suno.com/api/project/me?page=1&sort=created_at&show_trashed=false
        
        all_projects = []
        page_num = 1
        
        while True:
            url = f"{GEN_API_BASE}/api/project/me?page={page_num}&sort=created_at&show_trashed=false"
            
            try:
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    # User confirmed structure: {"projects": [...]}
                    projects = data.get("projects", [])
                    
                    # If no projects on this page, we've reached the end
                    if not projects:
                        break
                    
                    all_projects.extend(projects)
                    page_num += 1
                elif r.status_code == 404:
                    # No more pages
                    break
                else:
                    self._log(f"Failed to fetch projects page {page_num}: {r.status_code} {r.text}", "error")
                    break
            except Exception as e:
                self._log(f"Error fetching projects page {page_num}: {e}", "error")
                break
        
        return all_projects

    def fetch_playlists(self, token):
        """Fetch list of playlists with pagination."""
        headers = {"Authorization": f"Bearer {token}"}
        # Endpoint: /api/playlist/me?page=1&show_trashed=false&show_sharelist=false
        
        all_playlists = []
        page_num = 1
        
        while True:
            url = f"{GEN_API_BASE}/api/playlist/me?page={page_num}&show_trashed=false&show_sharelist=false"
            
            try:
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    # Structure: {"playlists": [...]}
                    playlists = data.get("playlists", [])
                    
                    # If no playlists on this page, we've reached the end
                    if not playlists:
                        break
                    
                    all_playlists.extend(playlists)
                    page_num += 1
                elif r.status_code == 404:
                    # No more pages
                    break
                else:
                    self._log(f"Failed to fetch playlists page {page_num}: {r.status_code} {r.text}", "error")
                    break
            except Exception as e:
                self._log(f"Error fetching playlists page {page_num}: {e}", "error")
                break
        
        return all_playlists

    def _list_s3_keys(self):
        """Fetch all object keys from the configured S3 bucket with robust retry logic."""
        s3_conf = self.config.get("s3_config", {})
        if not boto3 or not s3_conf.get("bucket"):
            return set()

        # Robust config for long-running listings or unstable port-forwards
        s3_robust_config = Config(
            region_name=s3_conf.get("region", "us-east-1"),
            signature_version='s3v4',
            retries={'max_attempts': 10, 'mode': 'standard'},
            connect_timeout=30,
            read_timeout=300 # 5 minutes for very large buckets
        )

        max_retries = 3
        for attempt in range(max_retries):
            try:
                s3_client = boto3.client(
                    "s3",
                    endpoint_url=s3_conf.get("endpoint"),
                    aws_access_key_id=s3_conf.get("access_key"),
                    aws_secret_access_key=s3_conf.get("secret_key"),
                    config=s3_robust_config
                )
                
                keys = set()
                paginator = s3_client.get_paginator("list_objects_v2")
                pages = paginator.paginate(Bucket=s3_conf.get("bucket"), Prefix=s3_conf.get("prefix", ""))
                
                for page in pages:
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            keys.add(obj["Key"])
                return keys
            except Exception as e:
                self._log(f"S3 Listing Attempt {attempt+1} failed: {e}", "warning")
                if attempt < max_retries - 1:
                    time.sleep(5) # Wait for port-forward to recover
                else:
                    self._log(f"Failed to list S3 objects after {max_retries} attempts.", "error")
                    return set()

    def _get_expected_s3_key(self, song_data, extension):
        """Calculate the expected S3 key for a given song clip and extension."""
        s3_conf = self.config.get("s3_config", {})
        prefix = s3_conf.get("prefix", "")
        
        title = song_data.get("title", "") or song_data.get("id")
        created_at = song_data.get("created_at", "")
        
        # Determine relative path based on organization settings
        rel_path_parts = []
        if self.config.get("organize_by_month") and created_at:
            rel_path_parts.append(created_at[:7])
            
        if self.config.get("organize_by_track") and self._is_stem(song_data):
            base_title = self._get_base_title(title)
            safe_title = sanitize_filename(base_title)
            rel_path_parts.append(safe_title)
            
        # Extension is already sanitized in resolve_audio_stream, but we provide it here
        fname = sanitize_filename(title) + extension
        
        # Build the full S3 key
        key_parts = []
        if prefix:
            # Clean prefix (remove leading/trailing slashes)
            clean_prefix = prefix.strip("/")
            if clean_prefix:
                key_parts.append(clean_prefix)
                
        for part in rel_path_parts:
            # S3 keys use forward slashes even on Windows
            key_parts.append(part.replace(os.path.sep, "/"))
            
        key_parts.append(fname)
        
        # Join and ensure no double slashes
        return "/".join(key_parts).replace("//", "/")

    def _generate_and_upload_manifest(self, directory, s3_client, s3_conf):
        """Generate JSON manifest and upload to S3 if enabled."""
        if not hasattr(self, 'db'):
            return

        try:
            self._log("Generating S3 Inventory Manifest...", "info")
            json_path = os.path.join(directory, "s3_inventory.json")
            self.db.export_json(json_path, s3_conf.get("bucket", ""))
            
            if s3_client and s3_conf.get("bucket"):
                bucket = s3_conf.get("bucket")
                
                # Re-initialize client with robust config for the final upload if it's missing or to be safe
                s3_robust_config = Config(
                    signature_version='s3v4',
                    retries={'max_attempts': 10, 'mode': 'standard'},
                    connect_timeout=15,
                    read_timeout=60
                )
                s3_upload_client = boto3.client(
                    's3',
                    endpoint_url=s3_conf.get('endpoint'),
                    aws_access_key_id=s3_conf.get('access_key'),
                    aws_secret_access_key=s3_conf.get('secret_key'),
                    config=s3_robust_config
                )

                prefix = s3_conf.get("prefix", "")
                manifest_key = "s3_inventory.json"
                if prefix:
                    manifest_key = f"{prefix.strip('/')}/s3_inventory.json"
                
                self._log(f"Uploading manifest to S3: {manifest_key}...", "info")
                s3_upload_client.upload_file(json_path, bucket, manifest_key)
                self._log("Manifest sync complete.", "success")
        except Exception as e:
            self._log(f"Manifest generation error: {e}", "warning")

    def download_single_song(self, clip, directory, headers, token, existing_uuids, rate_limiter, s3_client=None):
        if self.is_stopped():
            return

        uuid = clip.get("id")
        if uuid in existing_uuids:
            self._log(f"Skipping: {clip.get('title') or uuid} (already downloaded)", "info")
            return

        title = clip.get("title") or uuid
        image_url = clip.get("image_url")
        display_name = clip.get("display_name")
        metadata = clip.get("metadata", {})
        prompt = metadata.get("prompt", "")
        
        # --- REFETCH STRATEGY ---
        if not prompt:
            clip_id = clip.get("id")
            if clip_id:
                try:
                    detail_url = f"https://studio-api.prod.suno.com/api/clip/{clip_id}"
                    r_refetch = requests.get(detail_url, headers=headers, timeout=10)
                    if r_refetch.status_code == 200:
                        full_details = r_refetch.json()
                        metadata = full_details.get("metadata", {})
                        prompt = metadata.get("prompt", "")
                        clip["metadata"] = metadata
                except Exception as e:
                    self._log(f"Failed to refetch prompt for {clip_id}: {e}", "warning")
        
        tags = metadata.get("tags", "")
        created_at = clip.get("created_at", "")
        year = created_at[:4] if created_at else None
        lyrics = metadata.get("lyrics") or metadata.get("text") or prompt
        if lyrics:
            self._log(f"Lyrics found ({len(lyrics)} chars). Start: {lyrics[:30]}...", "info")
        else:
            self._log(f"No lyrics found for {title} in metadata", "warning")
        
        thumb_data = self.fetch_thumbnail_bytes(image_url) if image_url else None
        
        self.signals.song_started.emit(uuid, title, thumb_data, metadata)

        audio_url, file_ext, used_wav = self._resolve_audio_stream(clip, title, headers)
        if not audio_url:
            self._log(f"No usable audio stream for {title}; skipping.", "error")
            self.signals.song_updated.emit(uuid, "Error", 0)
            return

        # Handle S3 Logic vs Local
        is_s3 = self.config.get("storage_type") == "s3"
        s3_conf = self.config.get("s3_config", {})
        
        target_dir = directory
        if self.config.get("organize_by_month") and created_at:
            try:
                month_folder = created_at[:7]
                target_dir = os.path.join(directory, month_folder)
                if not os.path.exists(target_dir) and not is_s3:
                    os.makedirs(target_dir)
            except:
                pass

        if self.config.get("organize_by_track") and self._is_stem(clip):
            try:
                base_title = self._get_base_title(title)
                safe_title = sanitize_filename(base_title)
                target_dir = os.path.join(target_dir, safe_title)
                if not os.path.exists(target_dir) and not is_s3:
                    os.makedirs(target_dir)
            except:
                pass

        if is_s3:
             local_download_dir = tempfile.mkdtemp()
        else:
             local_download_dir = target_dir
             if not os.path.exists(local_download_dir):
                 os.makedirs(local_download_dir)

        ext = file_ext or ".mp3"
        fname = sanitize_filename(title) + ext
        out_path = os.path.join(local_download_dir, fname)
        
        if not is_s3:
            if os.path.exists(out_path):
                out_path = get_unique_filename(out_path)

        # --- S3 IDEMPOTENCY CHECK ---
        s3_key = ""
        if is_s3:
            try:
                # Calculate S3 Key early
                rel_path = os.path.relpath(target_dir, directory) 
                if rel_path == ".": rel_path = ""
                
                s3_key_parts = []
                prefix = s3_conf.get('prefix', '')
                if prefix: s3_key_parts.append(prefix)
                if rel_path: s3_key_parts.append(rel_path.replace(os.path.sep, '/'))
                s3_key_parts.append(fname)
                s3_key = "/".join(s3_key_parts).replace("//", "/")

                if not s3_client:
                    if not boto3:
                        raise ImportError("boto3 is not installed")

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
                bucket = s3_conf.get('bucket')

                try:
                    s3_client.head_object(Bucket=bucket, Key=s3_key)
                    self._log(f"Skipping: {title} (already on S3)", "info")
                    self.signals.song_finished.emit(uuid, True, f"s3://{bucket}/{s3_key}")
                    self.signals.song_updated.emit(uuid, "Complete (S3)", 100)
                    existing_uuids.add(uuid)
                    return
                except:
                    # Not found or error checking, proceed to download
                    pass
            except Exception as e:
                self._log(f"S3 Check Error: {e}", "warning")
        
        self._log(f"Downloading: {title}", "downloading", thumbnail_data=thumb_data)
        self.signals.song_updated.emit(uuid, "Downloading", 0)

        max_retries = 3
        download_success = False
        for attempt in range(max_retries):
            try:
                if rate_limiter:
                    rate_limiter.wait()
                with requests.get(audio_url, stream=True, headers=headers, timeout=60) as r_dl:
                    r_dl.raise_for_status()
                    total_size = int(r_dl.headers.get('content-length', 0))
                    downloaded = 0
                    
                    with open(out_path, "wb") as f:
                        for chunk in r_dl.iter_content(chunk_size=8192):
                            if self.is_stopped():
                                f.close()
                                if os.path.exists(out_path):
                                    os.remove(out_path)
                                if is_s3:
                                    try: os.rmdir(local_download_dir)
                                    except: pass
                                return
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0:
                                percent = int(downloaded * 100 / total_size)
                                self.signals.song_updated.emit(uuid, "Downloading", percent)
                download_success = True
                break
            except Exception as exc:
                if attempt < max_retries - 1:
                    self._log(f"  Retry {attempt+1}/{max_retries}...", "info")
                    time.sleep(2)
                else:
                    self._log(f"Failed: {title} - {exc}", "error")
                    self.signals.song_updated.emit(uuid, "Error", 0)
                    if is_s3:
                        try:
                            if os.path.exists(out_path): os.remove(out_path)
                            os.rmdir(local_download_dir)
                        except: pass
                    return

        if download_success and self.config.get("embed_metadata"):
             try:
                 self._log(f"Embedding metadata for {title}...", "info")
                 
                 # Extract exact arguments expected by embed_metadata
                 meta_dict = clip.get("metadata", {})
                 embed_metadata(
                     audio_path=out_path,
                     image_url=clip.get("image_url"),
                     title=title,
                     artist=clip.get("display_name"),
                     album="Suno AI Generation", # Default album
                     genre=meta_dict.get("tags"),
                     year=None, # will be parsed from created_at inside if needed, or pass explicit
                     comment=meta_dict.get("prompt"),
                     lyrics=lyrics,
                     uuid=uuid,
                     token=self.config.get("token")
                 )
             except Exception as e:
                 self._log(f"Metadata error: {e}", "warning")
        
        # S3 Upload Phase
        if download_success and is_s3:
            try:
                if not s3_client:
                    if not boto3:
                        raise ImportError("boto3 is not installed")
                    
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

                bucket = s3_conf.get('bucket')
                
                # Use pre-calculated key if available, otherwise recalculate
                if not s3_key:
                    prefix = s3_conf.get('prefix', '')
                    rel_path = os.path.relpath(target_dir, directory) 
                    if rel_path == ".": rel_path = ""
                    
                    s3_key_parts = []
                    if prefix: s3_key_parts.append(prefix)
                    if rel_path: s3_key_parts.append(rel_path.replace(os.path.sep, '/'))
                    s3_key_parts.append(fname)
                    
                    s3_key = "/".join(s3_key_parts).replace("//", "/")
                
                self.signals.song_updated.emit(uuid, "Uploading to S3", 100)
                self._log(f"Uploading to S3: {title}...", "info")
                
                # Upload with metadata
                s3_client.upload_file(
                    out_path, bucket, s3_key,
                    ExtraArgs={'Metadata': {'suno_uuid': str(uuid)}}
                )
                self._log(f"Uploaded to {bucket}/{s3_key}", "success")
                
            except Exception as e:
                self._log(f"S3 Upload Error: {e}", "error")
                download_success = False 
            finally:
                try:
                    if os.path.exists(out_path): os.remove(out_path)
                    os.rmdir(local_download_dir)
                except:
                    pass

        # Lyrics saving
        if lyrics and self.config.get("save_lyrics", True):
            try:
                # Ensure lyrics are in the metadata dict for DB/JSON export
                if "metadata" not in clip: clip["metadata"] = {}
                clip["metadata"]["lyrics_sync"] = lyrics 
                
                if is_s3 and download_success:
                    s3_parts = s3_key.split(".")
                    # Simply add .txt extension to the key, replacing audio ext if possible or appending
                    if len(s3_parts) > 1:
                        s3_parts[-1] = "txt"
                        lyrics_key = ".".join(s3_parts)
                    else:
                        lyrics_key = s3_key + ".txt"

                    s3_client.put_object(
                        Body=lyrics.encode('utf-8'),
                        Bucket=bucket,
                        Key=lyrics_key
                    )
                elif not is_s3 and download_success:
                    txt_path = os.path.splitext(out_path)[0] + ".txt"
                    with open(txt_path, "w", encoding="utf-8") as f:
                        f.write(lyrics)
            except Exception as e:
                self._log(f"Lyrics save error: {e}", "warning")

        if download_success:
            final_path = out_path if not is_s3 else f"s3://{s3_conf.get('bucket')}/{s3_key}"
            
            # --- DATABASE UPDATE ---
            try:
                clip["s3_key"] = s3_key if is_s3 else None
                clip["local_path"] = out_path if not is_s3 else None
                if hasattr(self, 'db'):
                    self.db.upsert_song(clip)
            except Exception as e:
                self._log(f"Database update error for {title}: {e}", "warning")
            
            existing_uuids.add(uuid)
            self._log(f"✓ {title}", "success", thumbnail_data=thumb_data)
            self.signals.song_finished.emit(uuid, True, final_path)
            self.signals.song_updated.emit(uuid, "Complete", 100)
        else:
            self.signals.song_finished.emit(uuid, False, "")

    def _is_stem(self, song_data):
        """Check if song is a stem."""
        metadata = song_data.get("metadata", {}) or {}
        if metadata is None: metadata = {}
        clip_type = metadata.get("type", "")
        top_type = song_data.get("type", "")
        title = song_data.get("title", "") or ""
        
        title_lower = title.lower()
        is_stem_title = any(ind in title_lower for ind in self.STEM_INDICATORS)
        
        return (clip_type in ["gen_stem", "stem"] or 
                "stem" in top_type or 
                is_stem_title)

    def _get_base_title(self, title):
        """Strip stem indicators from title to get base song name."""
        clean_title = title
        for ind in self.STEM_INDICATORS:
            pattern = re.escape(ind)
            clean_title = re.sub(pattern, "", clean_title, flags=re.IGNORECASE)
        return clean_title.strip()

    def _resolve_audio_stream(self, clip, title, headers):
        prefer_wav = self.config.get("prefer_wav")
        audio_url = clip.get("audio_url")
        extension = ".mp3"
        used_wav = False
        wav_url = self._find_wav_url(clip)
        if prefer_wav and wav_url:
            audio_url = wav_url
            extension = self._extract_extension_from_url(wav_url, default=".wav")
            used_wav = True
        elif prefer_wav:
            # self._log(f"WAV stream unavailable for '{title}'. Requesting conversion...", "info")
            converted = self._fetch_converted_wav(clip, headers)
            if converted:
                audio_url = converted
                extension = self._extract_extension_from_url(converted, default=".wav")
                used_wav = True
            else:
                self._log(f"Conversion failed or timed out for '{title}'. Falling back to MP3.", "error")

        if not audio_url:
            return None, None, False

        if not used_wav:
            extension = self._extract_extension_from_url(audio_url, default=".mp3")

        return audio_url, extension, used_wav

    def _find_wav_url(self, data):
        if isinstance(data, str):
            val = data.strip()
            lowered = val.lower()
            if lowered.startswith("http") and ".wav" in lowered:
                return val
            return None

        if isinstance(data, dict):
            prioritized = (
                "audio_url_wav",
                "wav_url",
                "wav_audio_url",
                "master_wav_url",
                "preview_wav_url",
            )
            for key in prioritized:
                val = data.get(key)
                if isinstance(val, str) and val.lower().startswith("http") and ".wav" in val.lower():
                    return val
            for value in data.values():
                candidate = self._find_wav_url(value)
                if candidate:
                    return candidate

        if isinstance(data, list):
            for entry in data:
                candidate = self._find_wav_url(entry)
                if candidate:
                    return candidate
        return None

    def _fetch_converted_wav(self, clip, headers):
        clip_id = clip.get("id")
        if not clip_id:
            return None
        convert_url = f"{GEN_API_BASE}/api/gen/{clip_id}/convert_wav/"
        # self._log(f"Requesting WAV conversion for '{clip_id}'...", "info")
        try:
            resp = requests.post(convert_url, headers=headers, timeout=15)
            resp.raise_for_status()
        except Exception as exc:
            self._log(f"Failed to request WAV conversion: {exc}", "error")
            return None
        return self._wait_for_wav_url(clip_id, headers)

    def _wait_for_wav_url(self, clip_id, headers, timeout=120, interval=2):
        deadline = time.monotonic() + timeout
        detail_url = f"https://studio-api.prod.suno.com/api/gen/{clip_id}/wav_file/"
        while time.monotonic() < deadline and not self.is_stopped():
            try:
                resp = requests.get(detail_url, headers=headers, timeout=15)
                if resp.status_code == 404:
                    time.sleep(interval)
                    continue
                resp.raise_for_status()
                data = resp.json()
                wav_url = self._find_wav_url(data)
                if wav_url:
                    return wav_url
            except requests.HTTPError as http_err:
                status = http_err.response.status_code if http_err.response else "?"
                if status != 404:
                    self._log(f"WAV status check failed ({status}): {http_err}", "info")
            except Exception as exc:
                self._log(f"WAV status check failed: {exc}", "info")
            time.sleep(interval)
        if self.is_stopped():
            self._log("WAV polling aborted.", "info")
        else:
            self._log("WAV conversion timed out.", "error")
        return None

    def _extract_extension_from_url(self, url, default=".mp3"):
        try:
            path = urlparse(url).path
            ext = os.path.splitext(path)[1]
            return ext.lower() if ext else default
        except:
            return default

    def fetch_thumbnail_bytes(self, url, size=40):
        try:
            from io import BytesIO
            from PIL import Image
            resp = requests.get(url, timeout=8)
            resp.raise_for_status()
            img = Image.open(BytesIO(resp.content))
            img = img.resize((size, size), Image.Resampling.LANCZOS)
            buffer = BytesIO()
            img.save(buffer, format="PNG")
            return buffer.getvalue()
        except:
            return None

    def migrate_to_s3(self, local_dir, s3_config, remove_local=False):
        """
        Migrates files from a local directory to S3.
        
        Args:
            local_dir (str): The local directory to scan.
            s3_config (dict): S3 configuration dictionary.
            remove_local (bool): Whether to remove local files after successful upload.
        """
        if not boto3:
            self._log("Boto3 not installed. Cannot migrate.", "error")
            return

        pk = s3_config.get('access_key')
        sk = s3_config.get('secret_key')
        endpoint = s3_config.get('endpoint')
        region = s3_config.get('region')
        bucket = s3_config.get('bucket')
        prefix = s3_config.get('prefix', '')

        if not (pk and sk and endpoint and bucket):
            self._log("Incomplete S3 configuration.", "error")
            return

        from botocore.client import Config
        try:
            s3_client = boto3.client(
                's3',
                endpoint_url=endpoint,
                aws_access_key_id=pk,
                aws_secret_access_key=sk,
                region_name=region,
                config=Config(s3={'addressing_style': 'path'})
            )
        except Exception as e:
            self._log(f"Failed to create S3 client: {e}", "error")
            return

        self._log(f"Starting migration from {local_dir} to s3://{bucket}/{prefix}", "info")
        self.signals.status_changed.emit("Migrating...")

        # --- SAFETY CHECK: Detect if Local Dir is a mount of the S3 Bucket ---
        try:
            canary_filename = f".sunosync_canary_{int(time.time())}.tmp"
            canary_path = os.path.join(local_dir, canary_filename)
            canary_key = f"{prefix}/{canary_filename}".replace("//", "/") if prefix else canary_filename
            
            # 1. Create local canary
            with open(canary_path, "w") as f:
                f.write("canary")
            
            # 2. Check if it appears in S3 (Wait briefly for consistency if needed, though mounts are usually immediate-ish)
            # Actually, if it's a mount, writing locally SHOULD make it appear in S3 list_objects or head_object
            # But S3 is eventually consistent. 
            # Better check: List S3, see if file exists.
            
            # Give it a moment if it's a network mount sync
            time.sleep(1) 
            
            try:
                s3_client.head_object(Bucket=bucket, Key=canary_key)
                is_coupled = True
            except:
                is_coupled = False
            
            # Cleanup local canary
            if os.path.exists(canary_path):
                os.remove(canary_path)
                
            if is_coupled:
                self._log("CRITICAL: Source directory appears to be coupled with S3 Bucket (Mount detected).", "error")
                self._log("Aborting migration to prevent data loss. You are already on S3!", "error")
                self.signals.status_changed.emit("Migration Aborted")
                self.signals.download_complete.emit(False)
                return
                
        except Exception as e:
            self._log(f"Safety check warning: {e}", "warning")
        # ---------------------------------------------------------------------

        files_to_migrate = []
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                if file.startswith("."): continue # Skip hidden files
                files_to_migrate.append(os.path.join(root, file))

        total_files = len(files_to_migrate)
        self._log(f"Found {total_files} files to migrate.", "info")
        
        migrated_count = 0
        errors_count = 0

        for idx, file_path in enumerate(files_to_migrate):
            if self.is_stopped():
                self._log("Migration stopped by user.", "warning")
                break

            try:
                # Calculate S3 Key
                rel_path = os.path.relpath(file_path, local_dir)
                
                s3_key_parts = []
                if prefix: s3_key_parts.append(prefix)
                s3_key_parts.append(rel_path.replace(os.path.sep, '/'))
                s3_key = "/".join(s3_key_parts).replace("//", "/")

                # Check if exists on S3
                try:
                    head = s3_client.head_object(Bucket=bucket, Key=s3_key)
                    # Exists
                    if remove_local:
                        # SAFETY CHECK: If the file exists and we are asked to delete local,
                        # we must be careful not to delete if source == dest.
                        # Since we can't easily know if it's a mount, we should be conservative.
                        # However, legitimate use case is "Move to S3 (Deduplicate)".
                        
                        # Verify sizes match at least
                        local_size = os.path.getsize(file_path)
                        remote_size = head.get('ContentLength', -1)
                        
                        if local_size == remote_size:
                             self._log(f"File exists on S3 (size match), deleting local: {os.path.basename(file_path)}", "info")
                             os.remove(file_path)
                             migrated_count += 1
                             continue
                        else:
                             self._log(f"File exists on S3 but size differs (Local: {local_size}, Remote: {remote_size}). Skipping delete for safety.", "warning")
                             continue
                    else:
                        self._log(f"File already exists on S3, skipping: {os.path.basename(file_path)}", "info")
                        continue
                except:
                    # Does not exist (or error), proceed to upload
                    pass

                # Upload
                self.signals.status_changed.emit(f"Migrating {idx+1}/{total_files}: {os.path.basename(file_path)}")
                s3_client.upload_file(file_path, bucket, s3_key)
                
                if remove_local:
                    os.remove(file_path)
                
                migrated_count += 1
                
            except Exception as e:
                error_msg = str(e)
                if "S3 API Requests must be made to API port" in error_msg:
                    self._log(f"Migration Error: connecting to MinIO Console port (9001)? Try port 9000.", "error")
                else:
                    self._log(f"Failed to migrate {os.path.basename(file_path)}: {e}", "error")
                errors_count += 1

            # Update progress signal if mapped to something valid
            # self.signals.progress_updated.emit(int((idx + 1) / total_files * 100))

        # Cleanup empty directories if remove_local is True
        if remove_local:
            for root, dirs, files in os.walk(local_dir, topdown=False):
                for name in dirs:
                    try:
                        os.rmdir(os.path.join(root, name))
                    except:
                        pass

        self._log(f"Migration complete. Migrated: {migrated_count}, Errors: {errors_count}", "success")
        self.signals.status_changed.emit("Migration Complete")
        self.signals.download_complete.emit(True)

    def repair_s3_metadata(self, target_month=None):
        """
        Repairs metadata for files on S3.
        Strategy:
        1. Local First: If file exists locally, embed metadata and upload to S3.
        2. S3 Second: If not local, download from S3, embed, and re-upload.
        """
        token = self.config.get("token", "").strip()
        if not token:
            self._log("Token missing. Cannot fetch library for repair.", "error")
            return

        directory = self.config.get("directory")
        s3_conf = self.config.get("s3_config", {})
        
        # Check S3 Config
        if not (s3_conf.get('access_key') and s3_conf.get('bucket')):
             self._log("S3 configuration missing. Cannot repair S3 files.", "error")
             return

        # Initialize S3 Client
        try:
            if not boto3: raise ImportError("boto3 missing")
            s3_client = boto3.client(
                's3',
                endpoint_url=s3_conf.get('endpoint'),
                aws_access_key_id=s3_conf.get('access_key'),
                aws_secret_access_key=s3_conf.get('secret_key'),
                region_name=s3_conf.get('region')
            )
            bucket = s3_conf.get('bucket')
            prefix = s3_conf.get('prefix', '')
        except Exception as e:
            self._log(f"Failed to initialize S3 client: {e}", "error")
            return

        self._log("Fetching library from Suno...", "info")
        # auto-detect workspace/project from filter settings if needed, but for now scan generic library
        # defaulting to "My Library" behavior (fetch all)
        projects = [None] # None represents main library
        
        # If user wants a specific project, we should probably support that, 
        # but the request was "constrain to this month", implying a date filter on the WHOLE library.
        # We will use fetch_user_library logic equivalent.
        
        # We'll use the existing pagination logic but just to get the list
        # Re-using internal fetch logic is hard because 'run' does everything.
        # Let's write a targeted fetch loop here.
        
        headers = {"Authorization": f"Bearer {token}"}
        base_url = "https://studio-api.prod.suno.com/api/feed/?page="
        page = 1
        processed_count = 0
        repaired_count = 0
        skipped_count = 0
        error_count = 0
        
        self.signals.status_changed.emit("Fetching Library...")

        while True:
            if self.is_stopped(): break
            if self.is_stopped(): break
            try:
                msg = f"Fetching page {page}..."
                print(msg)
                self.signals.status_changed.emit(msg)
                
                # Retry loop for 429 Errors
                max_retries = 3
                retry_delay = 30 # seconds
                
                for attempt in range(max_retries + 1):
                    try:
                        r = requests.get(f"{base_url}{page}", headers=headers, timeout=30)
                        r.raise_for_status()
                        break # Success
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 429:
                            if attempt < max_retries:
                                self._log(f"Rate limited (429). Waiting {retry_delay}s...", "warning")
                                time.sleep(retry_delay)
                                retry_delay *= 2 # Exponential backoff
                                continue
                        raise e # Re-raise other errors or if retries exhausted

                data = r.json()
                
                # Rate Limit Kindness
                time.sleep(2) # Wait 2 seconds between pages
                
                clips = data if isinstance(data, list) else data.get('clips', [])
                if not clips: break # End of library
                
                for clip in clips:
                    if self.is_stopped(): break
                    
                    # Unwrap logic
                    if isinstance(clip, dict) and "clip" in clip:
                        clip = clip["clip"]
                    
                    if not clip: continue

                    # Date Filter
                    created_at = clip.get("created_at", "")
                    if target_month:
                        # expected format: YYYY-MM
                        if not created_at.startswith(target_month):
                            # Assumes feed is roughly chronological. 
                            # If we see a date OLDER than target_month, we might be able to stop?
                            # Suno feed is usually newest first.
                            if created_at < target_month:
                                self._log("Reached songs older than target month. Stopping scan.", "success")
                                return
                            continue

                    # Metadata
                    uuid = clip.get("id")
                    title = clip.get("title") or uuid
                    clean_title = sanitize_filename(title)
                    display_name = clip.get("display_name")
                    metadata = clip.get("metadata", {})
                    prompt = metadata.get("prompt", "")
                    
                    self._log(f"Processing: {title} ({created_at})", "info")
                    processed_count += 1

                    # Determine expected filenames
                    # We check:
                    # 1. Root download dir
                    # 2. Month folder (if created_at exists)
                    # 3. Track folder (if stem)
                    
                    possible_local_paths = []
                    
                    # Extensions to check
                    extensions = [".mp3", ".wav"]
                    
                    # Root
                    for ext in extensions:
                        possible_local_paths.append(os.path.join(directory, f"{clean_title}{ext}"))
                    
                    # Month Folder
                    if created_at:
                        month_folder = created_at[:7]
                        for ext in extensions:
                            possible_local_paths.append(os.path.join(directory, month_folder, f"{clean_title}{ext}"))
                    
                    # Check which one exists
                    local_file_path = None
                    for p in possible_local_paths:
                        if os.path.exists(p):
                            local_file_path = p
                            break
                        # Debug: check for partial matches or "v2" suffix if not found?
                        # For now, let's just log what we checked if we fail
                    
                    if not local_file_path:
                         self._log(f"  Debug: Checked paths: {possible_local_paths}", "downloading")

                    # S3 Key Calculation (We align with standard upload logic)
                    
                    # S3 Key Calculation (We align with standard upload logic)
                    # Standard logic: prefix + (optional month folder) + filename
                    # We need to know where it SHOULD be on S3.
                    # If the user uses 'organize_by_month', it goes into a folder.
                    # We should probably check BOTH locations on S3 or prefer the month folder if we have it?
                    # Plan: Check S3 for the file using the same logic as local folders.
                    
                    s3_keys_to_check = []
                    
                    # 1. Determine Target Key (for Upload - Case A)
                    # Use local extension if found, otherwise default to .mp3
                    target_ext = ".mp3"
                    if local_file_path:
                        _, target_ext = os.path.splitext(local_file_path)
                    
                    target_parts = []
                    if prefix: target_parts.append(prefix)
                    if created_at: target_parts.append(created_at[:7]) # Always use month folder for target
                    target_parts.append(f"{clean_title}{target_ext}")
                    target_s3_key = "/".join(target_parts).replace("//", "/")

                    # 2. Build Search Keys (for Download fallback - Case B)
                    # Check Month and Root, for both mp3 and wav
                    search_locs = []
                    if created_at: search_locs.append(created_at[:7])
                    search_locs.append("") # Root
                    
                    for loc in search_locs:
                        for ext in [".mp3", ".wav"]:
                            kp = []
                            if prefix: kp.append(prefix)
                            if loc: kp.append(loc)
                            kp.append(f"{clean_title}{ext}")
                            k = "/".join(kp).replace("//", "/")
                            if k not in s3_keys_to_check:
                                s3_keys_to_check.append(k)
                    
                    # Smart Metadata Check: Verify if repair is actually needed
                    if local_file_path:
                        try:
                            has_lyrics = False
                            has_cover = False
                            
                            # Check based on extension
                            is_wav = local_file_path.lower().endswith(".wav")
                            
                            if is_wav:
                                audio = WAVE(local_file_path)
                                if audio.tags:
                                    # mutagen.wave uses ID3 tags if added via our utility
                                    # keys are like 'USLT:...'
                                    for key in audio.tags.keys():
                                        if key.startswith("USLT"): has_lyrics = True
                                        if key.startswith("APIC"): has_cover = True
                            else:
                                audio = ID3(local_file_path)
                                for key in audio.keys():
                                    if key.startswith("USLT"): has_lyrics = True
                                    if key.startswith("APIC"): has_cover = True
                            
                            # Decision logic
                            # 1. Cover Art: Always required if we have an image_url (which we almost always do)
                            # 2. Lyrics: Required ONLY if the API provided lyrics/text/prompt
                            
                            should_have_lyrics = bool(lyrics and lyrics.strip())
                            
                            is_complete = has_cover
                            missing_items = []
                            
                            if not has_cover:
                                is_complete = False
                                missing_items.append("Cover Art")
                                
                            if should_have_lyrics and not has_lyrics:
                                is_complete = False
                                missing_items.append("Lyrics")
                            
                            if is_complete:
                                self._log(f"  Metadata already complete (Lyrics={'Yes' if has_lyrics else 'N/A'}, Cover=Yes). Skipping.", "success")
                                skipped_count += 1
                                continue
                            else:
                                self._log(f"  Metadata incomplete (Missing: {', '.join(missing_items)}). Repairing...", "warning")

                        except Exception as check_e:
                            # If checking fails (e.g. corrupt tag), proceed to repair
                            self._log(f"  Metadata check failed ({check_e}), forcing repair.", "warning")

                    # Repair Logic
                    try:
                        # CASE A: Local File Exists
                        if local_file_path:
                            self._log(f"  Found locally: {local_file_path}", "info")
                            # Embed Metadata
                            self._log("  Embedding metadata...", "info")
                             # Extract exact arguments expected by embed_metadata
                            meta_dict = clip.get("metadata", {})
                            lyrics = meta_dict.get("lyrics") or meta_dict.get("text") or meta_dict.get("prompt")
                            
                            embed_metadata(
                                audio_path=local_file_path,
                                image_url=clip.get("image_url"),
                                title=title,
                                artist=clip.get("display_name"),
                                album="Suno AI Generation", 
                                genre=meta_dict.get("tags"),
                                year=created_at[:4] if created_at else None,
                                comment=meta_dict.get("prompt"),
                                lyrics=lyrics,
                                uuid=uuid,
                                token=self.config.get("token")
                            )
                            
                            # Upload to S3
                            self._log(f"  Uploading to S3: {target_s3_key}", "info")
                            s3_client.upload_file(local_file_path, bucket, target_s3_key)
                            
                            # CLEANUP: If we just uploaded a .wav, check if there's an old .mp3 version and delete it
                            if target_s3_key.endswith(".wav"):
                                incorrect_mp3_key = target_s3_key[:-4] + ".mp3"
                                try:
                                    s3_client.head_object(Bucket=bucket, Key=incorrect_mp3_key)
                                    self._log(f"  Found duplicate/incorrect .mp3 on S3. Deleting: {incorrect_mp3_key}", "warning")
                                    s3_client.delete_object(Bucket=bucket, Key=incorrect_mp3_key)
                                except:
                                    pass # No .mp3 found, all good

                            self._log("  Sync Complete.", "success")
                            repaired_count += 1
                        
                        else:
                            # CASE B: No Local File -> Check S3
                            self._log("  Not found locally. Checking S3...", "info")
                            found_s3_key = None
                            for k in s3_keys_to_check:
                                try:
                                    s3_client.head_object(Bucket=bucket, Key=k)
                                    found_s3_key = k
                                    break
                                except:
                                    pass
                            
                            if found_s3_key:
                                self._log(f"  Found on S3: {found_s3_key}. Downloading for repair...", "info")
                                
                                # Use correct extension from S3 key (e.g. .wav)
                                _, s3_ext = os.path.splitext(found_s3_key)
                                if not s3_ext: s3_ext = ".mp3" # Fallback

                                with tempfile.NamedTemporaryFile(suffix=s3_ext, delete=False) as tmp:
                                    temp_path = tmp.name
                                
                                try:
                                    s3_client.download_file(bucket, found_s3_key, temp_path)
                                    
                                    # Embed
                                    meta_dict = clip.get("metadata", {})
                                    lyrics = meta_dict.get("lyrics") or meta_dict.get("text") or meta_dict.get("prompt")
                                    embed_metadata(
                                        audio_path=temp_path,
                                        image_url=clip.get("image_url"),
                                        title=title,
                                        artist=clip.get("display_name"),
                                        album="Suno AI Generation", 
                                        genre=meta_dict.get("tags"),
                                        year=created_at[:4] if created_at else None,
                                        comment=meta_dict.get("prompt"),
                                        lyrics=lyrics,
                                        uuid=uuid,
                                        token=self.config.get("token")
                                    )
                                    
                                    # Upload
                                    self._log(f"  Re-uploading repaired file to {found_s3_key}", "info")
                                    s3_client.upload_file(temp_path, bucket, found_s3_key)
                                    self._log("  Repair Complete.", "success")
                                    repaired_count += 1
                                    
                                finally:
                                    if os.path.exists(temp_path):
                                        os.remove(temp_path)
                            else:
                                self._log("  File not found on S3 either. Skipping.", "warning")
                                skipped_count += 1
                                
                    except Exception as e:
                        self._log(f"  Repair Failed: {e}", "error")
                        error_count += 1

                page += 1
            except Exception as e:
                 self._log(f"Error fetching page {page}: {e}", "error")
                 break

        self._log(f"Repair Session Finished. Repaired: {repaired_count}, Skipped: {skipped_count}, Errors: {error_count}", "success")
