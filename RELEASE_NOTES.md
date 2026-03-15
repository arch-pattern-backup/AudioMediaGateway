# SunoSync-Nexus Release Notes (v2.1.0)

## Overview
**SunoSync-Nexus** transforms the application from a simple downloader into an authoritative data hub for your Suno AI music collection. This release introduces a dual-layer "Source of Truth" system and powerful retroactive synchronization capabilities.

## Major Features

### 1. Dual-Layer "Source of Truth"
*   **Nexus SQLite Database (`sunosync.db`)**: A high-performance, indexed database that tracks every song, S3 key, local path, and full metadata. Perfect for secondary integrations needing fast, structured queries.
*   **Nexus JSON Manifest (`s3_inventory.json`)**: A clean, portable snapshot of your entire collection. Ideal for lightweight scripts, web frontends, and non-database integrations.

### 2. Automated S3 Manifest Sync
*   Every sync session now automatically generates and uploads a fresh `s3_inventory.json` to the root of your S3 bucket.
*   Your cloud inventory is always up-to-date and accessible via a single JSON fetch from S3.

### 3. Full Inventory & Integrity Sync (Retroactive)
*   **New Feature**: A dedicated "Full Inventory Sync" process that matches your entire Suno library against existing S3 files.
*   **Retroactive Backfill**: Automatically populates the database and manifest with full metadata (including lyrics) for songs already stored in S3.
*   **Integrity Check**: Verifies the consistency between your S3 objects and your local data records.

### 4. Early S3 Duplicate Detection
*   **Performance Boost**: The application now pre-scans S3 keys at the start of a run, skipping redundant downloads *before* expensive API calls or thumbnail fetches.
*   **Smart Resume Fix**: Full compatibility with S3; the app now correctly identifies already-archived songs to intelligently stop scanning.

## Enhancements & Optimizations
*   **Automatic Lyric Sync**: Lyrics are now explicitly extracted and included in the metadata manifest for every song.
*   **S3 Connection Pooling**: Reuses S3 client connections to reduce overhead during high-volume sync sessions.
*   **Integrated UI Controls**: Added "Full Inventory Sync" button and "Last Sync" timestamp display to the S3 settings.
*   **UUID Metadata**: S3 uploads now include the Suno UUID in the object's S3 metadata for enhanced cloud-native tracking.

## Technical Fixes
*   Eliminated "skipped" log noise by performing duplicate checks earlier in the lifecycle.
*   Refactored internal cache management to be storage-agnostic (Local vs. S3).
*   Cleaned up redundant utility functions and improved error handling for S3 network timeouts.
