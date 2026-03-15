import sqlite3
import json
import os
from datetime import datetime

class DBManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        """Initialize the SQLite database with the required schema."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS songs (
                    uuid TEXT PRIMARY KEY,
                    title TEXT,
                    artist TEXT,
                    s3_key TEXT,
                    local_path TEXT,
                    created_at TEXT,
                    metadata_json TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Index for faster S3 key lookups
            conn.execute("CREATE INDEX IF NOT EXISTS idx_s3_key ON songs(s3_key)")
            conn.commit()

    def upsert_song(self, song_data):
        """Insert or update a song record in the database."""
        uuid = song_data.get("id")
        if not uuid:
            return

        title = song_data.get("title")
        artist = song_data.get("display_name")
        s3_key = song_data.get("s3_key")
        local_path = song_data.get("local_path")
        created_at = song_data.get("created_at")
        
        # Merge top-level metadata and internal API metadata
        # Suno API often puts the interesting stuff (lyrics, prompt) inside song_data['metadata']
        metadata = song_data.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {"raw_value": str(metadata)}
            
        # Ensure lyrics are explicitly promoted to a top-level metadata key for easier integration
        if "lyrics_sync" not in metadata:
            lyrics = metadata.get("lyrics") or metadata.get("text") or song_data.get("prompt")
            if lyrics:
                metadata["lyrics_sync"] = lyrics

        metadata_str = json.dumps(metadata)

        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO songs (uuid, title, artist, s3_key, local_path, created_at, metadata_json, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(uuid) DO UPDATE SET
                    title = COALESCE(excluded.title, title),
                    artist = COALESCE(excluded.artist, artist),
                    s3_key = COALESCE(excluded.s3_key, s3_key),
                    local_path = COALESCE(excluded.local_path, local_path),
                    created_at = COALESCE(excluded.created_at, created_at),
                    metadata_json = excluded.metadata_json,
                    last_updated = CURRENT_TIMESTAMP
            """, (uuid, title, artist, s3_key, local_path, created_at, metadata_str))
            conn.commit()

    def bulk_upsert_s3_keys(self, s3_keys, bucket_name):
        """
        Populates the DB with keys found on S3.
        Identifies UUIDs within filenames to prevent duplicate 's3:' prefixed records.
        """
        import re
        # Standard UUID pattern: 8-4-4-4-12 hex chars
        uuid_pattern = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE)
        
        with self._get_connection() as conn:
            for key in s3_keys:
                # Try to find a real UUID in the filename/key
                match = uuid_pattern.search(key)
                if match:
                    item_id = match.group(0).lower()
                else:
                    item_id = f"s3:{key}"

                conn.execute("""
                    INSERT INTO songs (uuid, s3_key, last_updated)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(uuid) DO UPDATE SET
                        s3_key = excluded.s3_key,
                        last_updated = CURRENT_TIMESTAMP
                """, (item_id, key))
            conn.commit()

    def export_json(self, json_path, bucket_name=""):
        """Export the entire database to a structured JSON manifest."""
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM songs ORDER BY created_at DESC")
            rows = cursor.fetchall()

            inventory = {
                "generated_at": datetime.now().isoformat(),
                "bucket": bucket_name,
                "total_count": len(rows),
                "items": {}
            }

            for row in rows:
                item_uuid = row["uuid"]
                inventory["items"][item_uuid] = {
                    "title": row["title"],
                    "artist": row["artist"],
                    "s3_key": row["s3_key"],
                    "local_path": row["local_path"],
                    "created_at": row["created_at"],
                    "metadata": json.loads(row["metadata_json"]) if row["metadata_json"] else {}
                }

            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(inventory, f, indent=2)
            
            return inventory
