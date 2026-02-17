import sys
import os
import argparse
import json

# Add current directory to path so we can import modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from suno_downloader import SunoDownloader
from config_manager import ConfigManager

def main():
    parser = argparse.ArgumentParser(description="Repair S3 Metadata for SunoSync")
    parser.add_argument("--month", type=str, help="Target month in YYYY-MM format (e.g. 2026-02)", default=None)
    args = parser.parse_args()

    args = parser.parse_args()

    # Load Config - Search in multiple locations
    base_dir = os.path.dirname(os.path.abspath(__file__))
    possible_paths = [
        os.path.join(base_dir, "config.json"),
        os.path.join(base_dir, "dist", "config.json"),
        "config.json" # Current working directory
    ]
    
    config_file = None
    for path in possible_paths:
        if os.path.exists(path):
            config_file = path
            break
            
    if not config_file:
        print("Error: config.json not found in root, dist/, or current directory.")
        print("Please run the main application first to generate config.")
        return

    print(f"Using config file: {config_file}")
    cm = ConfigManager(config_file)
    
    # Initialize Downloader
    downloader = SunoDownloader()
    
    # Configure downloader using loaded config
    # We map config keys to the configure method arguments
    downloader.configure(
        token=cm.get("token"),
        directory=cm.get("path"),
        max_pages=0,
        start_page=1,
        organize_by_month=cm.get("organize"),
        embed_metadata_enabled=True, # Force True for repair
        prefer_wav=cm.get("prefer_wav"),
        download_delay=cm.get("download_delay"),
        filter_settings=cm.get("filter_settings"),
        scan_only=False,
        save_lyrics=cm.get("save_lyrics"),
        organize_by_track=cm.get("track_folder"),
        stems_only=False,
        smart_resume=False,
        storage_type=cm.get("storage_type"),
        s3_config={
            "endpoint": cm.get("s3_endpoint"),
            "bucket": cm.get("s3_bucket"),
            "region": cm.get("s3_region"),
            "access_key": cm.get("s3_access_key"),
            "secret_key": cm.get("s3_secret_key"),
            "prefix": cm.get("s3_path_prefix")
        }
    )

    print(f"Starting Repair Mode...")
    if args.month:
        print(f"Target Month: {args.month}")
    else:
        print("Target: All Files (No month filter)")

    # Run Repair
    downloader.repair_s3_metadata(target_month=args.month)
    print("Repair script finished.")

if __name__ == "__main__":
    main()
