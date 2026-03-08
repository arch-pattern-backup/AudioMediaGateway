import tkinter as tk
from tkinter import ttk, messagebox
import os
import json
import threading
import queue
import time
import datetime
try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

from suno_utils import read_song_metadata, save_lyrics_to_file, open_file
from theme_manager import ThemeManager


class LibraryTab(tk.Frame):
    """Library tab for browsing and playing downloaded songs."""
    
    def __init__(self, parent, config_manager, cache_file=None, tags_file=None, **kwargs):
        super().__init__(parent, **kwargs)
        
        self.config_manager = config_manager
        self.cache_file = cache_file
        self.tags_file = tags_file
        self.download_path = self.config_manager.get("path", "")
        self.all_songs = []  # Full song list
        self.filtered_songs = []  # Filtered by search
        self.tags = {}
        self.active_filters = {"keep": False, "trash": False, "star": False}
        self._load_tags()
        
        # Caching & Threading
        self.cache = {}
        self.scan_queue = queue.Queue()
        self.is_scanning = False
        self._load_cache()
        
        # Apply theme
        theme = ThemeManager()
        self.bg_dark = theme.bg_dark
        self.bg_card = theme.bg_card
        self.bg_input = theme.bg_input  # Added for text area
        self.fg_primary = theme.fg_primary
        self.fg_secondary = theme.fg_secondary
        self.accent_purple = theme.accent_purple
        
        self.configure(bg=self.bg_dark)
        
        self.create_widgets()
        self.refresh_library()
    
    def create_widgets(self):
        """Create the library UI."""
        # Top toolbar
        toolbar = tk.Frame(self, bg=self.bg_dark, height=60)
        toolbar.pack(fill="x", padx=20, pady=(20, 10))
        
        # Search bar
        search_frame = tk.Frame(toolbar, bg=self.bg_card, highlightthickness=1, 
                               highlightbackground=self.fg_secondary)
        search_frame.pack(side=tk.LEFT, fill="x", expand=True, padx=(0, 10))
        
        tk.Label(search_frame, text="🔍", bg=self.bg_card, fg=self.fg_secondary,
                font=("Segoe UI", 12)).pack(side=tk.LEFT, padx=(10, 5))
        
        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", self.on_search)
        search_entry = tk.Entry(search_frame, textvariable=self.search_var,
                               bg=self.bg_card, fg=self.fg_primary,
                               font=("Segoe UI", 10), relief="flat", bd=0)
        search_entry.pack(side=tk.LEFT, fill="x", expand=True, padx=(0, 10), pady=8)
        
        # Filter Buttons
        filter_frame = tk.Frame(toolbar, bg=self.bg_dark)
        filter_frame.pack(side=tk.LEFT, padx=10)
        
        self.filter_btns = {}
        filters = [("👍", "keep", "#22c55e"), ("⭐", "star", "#eab308"), ("🗑️", "trash", "#ef4444")]
        
        for icon, tag, color in filters:
            btn = tk.Button(filter_frame, text=icon, 
                           command=lambda t=tag, c=color: self.toggle_filter(t, c),
                           bg=self.bg_card, fg=self.fg_secondary,
                           font=("Segoe UI", 12), relief="flat", cursor="hand2",
                           width=3, padx=5, pady=2)
            btn.pack(side=tk.LEFT, padx=2)
            self.filter_btns[tag] = btn
        
        # Refresh button
        self.refresh_btn = tk.Button(toolbar, text="🔄 Refresh", command=self.refresh_library,
                                bg=self.accent_purple, fg="white",
                                font=("Segoe UI", 10, "bold"),
                                relief="flat", cursor="hand2",
                                padx=20, pady=8)
        self.refresh_btn.pack(side=tk.RIGHT)
        
        # Open Folder button (Show in Explorer)
        open_btn = tk.Button(toolbar, text="📂 Show in Explorer", command=self.open_download_folder,
                               bg=self.bg_card, fg=self.fg_primary,
                               font=("Segoe UI", 10),
                               relief="flat", cursor="hand2",
                               padx=15, pady=8)
        open_btn.pack(side=tk.RIGHT, padx=10)

        # Change Folder button
        change_btn = tk.Button(toolbar, text="📁 Change Folder", command=self.change_library_folder,
                               bg=self.bg_card, fg=self.fg_primary,
                               font=("Segoe UI", 10),
                               relief="flat", cursor="hand2",
                               padx=15, pady=8)
        change_btn.pack(side=tk.RIGHT, padx=10)

        # S3 Toggle Button
        self.use_s3 = False
        self.s3_btn = tk.Button(toolbar, text="💾 Storage: Local", command=self.toggle_storage_mode,
                               bg=self.bg_card, fg=self.fg_primary,
                               font=("Segoe UI", 10),
                               relief="flat", cursor="hand2",
                               padx=15, pady=8)
        self.s3_btn.pack(side=tk.RIGHT, padx=10)
        
        # About button
        about_btn = tk.Button(toolbar, text="ℹ️ About", command=self.show_about,
                               bg=self.bg_card, fg=self.fg_primary,
                               font=("Segoe UI", 10),
                               relief="flat", cursor="hand2",
                               padx=15, pady=8)
        about_btn.pack(side=tk.RIGHT, padx=10)
        
        # Song count label (with minimum width to prevent truncation)
        self.count_label = tk.Label(toolbar, text="0 songs", bg=self.bg_dark,
                                   fg=self.fg_secondary, font=("Segoe UI", 9), width=15, anchor="e")
        self.count_label.pack(side=tk.RIGHT, padx=10)
        
        # Treeview (file list)
        tree_frame = tk.Frame(self, bg=self.bg_dark)
        tree_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        
        # Create Treeview with custom styling
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Library.Treeview",
                       background=self.bg_card,
                       foreground=self.fg_primary,
                       fieldbackground=self.bg_card,
                       borderwidth=0,
                       font=("Segoe UI", 10))
        style.configure("Library.Treeview.Heading",
                       background=self.bg_dark,
                       foreground=self.fg_primary,
                       borderwidth=0,
                       font=("Segoe UI", 10, "bold"))
        style.map("Library.Treeview",
                 background=[("selected", self.accent_purple)])
        
        # Scrollbars
        v_scroll = ttk.Scrollbar(tree_frame, orient="vertical")
        v_scroll.pack(side=tk.RIGHT, fill="y")
        
        h_scroll = ttk.Scrollbar(tree_frame, orient="horizontal")
        h_scroll.pack(side=tk.BOTTOM, fill="x")
        
        # Treeview columns
        self.tree = ttk.Treeview(tree_frame, style="Library.Treeview",
                                columns=("tag", "title", "artist", "duration", "date", "size"),
                                show="headings",
                                yscrollcommand=v_scroll.set,
                                xscrollcommand=h_scroll.set)
        
        # Column headings
        self.tree.heading("tag", text="", command=lambda: self.sort_column("tag"))
        self.tree.heading("title", text="Title", command=lambda: self.sort_column("title"))
        self.tree.heading("artist", text="Artist", command=lambda: self.sort_column("artist"))
        self.tree.heading("duration", text="Duration", command=lambda: self.sort_column("duration"))
        self.tree.heading("date", text="Date", command=lambda: self.sort_column("date"))
        self.tree.heading("size", text="Size", command=lambda: self.sort_column("size"))
        
        # Column widths
        self.tree.column("tag", width=50, minwidth=50, anchor="center")
        self.tree.column("title", width=300, minwidth=150)
        self.tree.column("artist", width=200, minwidth=100)
        self.tree.column("duration", width=80, minwidth=60)
        self.tree.column("date", width=100, minwidth=80)
        self.tree.column("size", width=80, minwidth=60)
        
        self.tree.pack(side=tk.LEFT, fill="both", expand=True)
        
        v_scroll.config(command=self.tree.yview)
        h_scroll.config(command=self.tree.xview)
        
        self.tree.bind("<Double-1>", self.on_double_click)
        self.tree.bind("<<TreeviewSelect>>", self.on_selection_change)
        
        # Right-click menu
        self.context_menu = tk.Menu(self, tearoff=0, bg=self.bg_card, fg=self.fg_primary)
        self.context_menu.add_command(label="Play", command=self.play_selected)
        self.context_menu.add_command(label="View/Edit Lyrics", command=self.edit_lyrics)
        self.context_menu.add_command(label="Open Folder", command=self.open_folder)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="Delete", command=self.delete_selected)
        
        self.tree.bind("<Button-3>", self.show_context_menu)
        
        # Reference to player widget (set by main.py)
        self.player_widget = None

    def _load_tags(self):
        """Load tags from file."""
        if self.tags_file and os.path.exists(self.tags_file):
            try:
                with open(self.tags_file, 'r', encoding='utf-8') as f:
                    self.tags = json.load(f)
            except:
                self.tags = {}
    
    def reload_tags(self):
        """Reload tags and update UI."""
        try:
            # Check if tree exists and is ready
            if not hasattr(self, 'tree') or not self.tree:
                return
            
            # Preserve current selection before rebuilding tree
            selected_filepath = None
            selection = self.tree.selection()
            if selection:
                item = selection[0]
                selected_filepath = self.tree.item(item)['tags'][0]
                if selected_filepath:
                    selected_filepath = os.path.normpath(selected_filepath)
            
            # Reload tags from file
            self._load_tags()
            
            # Re-apply filters to update the view (this rebuilds the tree)
            self.on_search()
            
            # Restore selection after tree rebuild
            if selected_filepath:
                # Use after() to ensure tree is fully updated before restoring selection
                self.after(50, lambda: self._restore_selection(selected_filepath))
        except Exception as e:
            print(f"Error in reload_tags: {e}")
            import traceback
            traceback.print_exc()

    def _get_s3_client(self, use_public_endpoint=False):
        """Helper to get S3 client."""
        try:
            s3_endpoint = self.config_manager.get("s3_endpoint", "")
            
            # Use public endpoint if requested and available
            if use_public_endpoint:
                public_ep = self.config_manager.get("s3_public_endpoint", "")
                if public_ep:
                    print(f"DEBUG: Using Public S3 Endpoint: {public_ep}")
                    s3_endpoint = public_ep
                else:
                    print(f"DEBUG: Public S3 Endpoint requested but not set. Using internal: {s3_endpoint}")

            s3_access_key = self.config_manager.get("s3_access_key", "")
            s3_secret_key = self.config_manager.get("s3_secret_key", "")
            
            session = boto3.session.Session()
            return session.client(
                's3',
                endpoint_url=s3_endpoint,
                aws_access_key_id=s3_access_key,
                aws_secret_access_key=s3_secret_key,
                region_name='us-east-1'
            )
        except Exception as e:
            print(f"Failed to create S3 client: {e}")
            return None

    
    def _restore_selection(self, filepath):
        """Restore selection to the specified filepath."""
        try:
            if not filepath:
                return
            
            # Normalize filepath for comparison
            filepath = os.path.normpath(filepath)
            filepath_alt = filepath.replace('\\', '/')
            
            # Find and select the item
            for item in self.tree.get_children():
                try:
                    item_tags = self.tree.item(item, "tags")
                    if item_tags and len(item_tags) > 0:
                        item_filepath = os.path.normpath(item_tags[0])
                        item_filepath_alt = item_filepath.replace('\\', '/')
                        
                        if item_filepath == filepath or item_filepath == filepath_alt or \
                           item_filepath_alt == filepath or item_filepath_alt == filepath_alt:
                            self.tree.selection_set(item)
                            self.tree.see(item)
                            # Update player tag UI now that selection is restored
                            if self.player_widget:
                                self.player_widget.update_tag_ui()
                            return
                except tk.TclError:
                    continue
        except Exception as e:
            print(f"Error in _restore_selection: {e}")

    def _get_tag_icon(self, song):
        """Get icon for song tag."""
        try:
            uuid = song.get('id')
            if not uuid:
                # Normalize filepath for consistent lookup
                filepath = song.get('filepath', '')
                if not filepath:
                    return ""
                uuid = os.path.normpath(filepath)
            
            if not uuid:
                return ""
            
            # Try lookup with UUID first
            tag = self.tags.get(uuid)
            
            # If not found and UUID is a filepath, try with forward slashes (for compatibility)
            if not tag and uuid and os.path.sep in uuid:
                uuid_alt = uuid.replace('\\', '/')
                tag = self.tags.get(uuid_alt)
                if tag:
                    # Update the tags dict to use normalized path for future lookups
                    self.tags[uuid] = tag
                    if uuid_alt in self.tags:
                        del self.tags[uuid_alt]
            
            if tag == "keep": return "👍"
            if tag == "trash": return "🗑️"
            if tag == "star": return "⭐"
            return ""
        except Exception as e:
            print(f"Error in _get_tag_icon: {e}")
            return ""

    def _load_cache(self):
        """Load metadata cache from file."""
        if self.cache_file and os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.cache = json.load(f)
            except Exception as e:
                print(f"Error loading cache: {e}")
                self.cache = {}

    def _save_cache(self):
        """Save metadata cache to file."""
        if self.cache_file:
            try:
                with open(self.cache_file, 'w', encoding='utf-8') as f:
                    json.dump(self.cache, f)
            except Exception as e:
                print(f"Error saving cache: {e}")

    def _scan_thread(self):
        """Background thread to scan library."""
        new_songs = []
        cache_updated = False
        
        try:
            for root, dirs, files in os.walk(self.download_path):
                for file in files:
                    if file.lower().endswith(('.mp3', '.wav')):
                        filepath = os.path.join(root, file)
                        try:
                            mtime = os.path.getmtime(filepath)
                            
                            # Check cache
                            cached_data = self.cache.get(filepath)
                            if cached_data and cached_data.get('mtime') == mtime:
                                song_data = cached_data
                            else:
                                # Parse file
                                song_data = read_song_metadata(filepath)
                                if song_data:
                                    song_data['mtime'] = mtime
                                    self.cache[filepath] = song_data
                                    cache_updated = True
                            
                            if song_data:
                                new_songs.append(song_data)
                                
                                # Batch update UI every 20 songs
                                if len(new_songs) >= 20:
                                    self.scan_queue.put(("batch", list(new_songs)))
                                    new_songs = []
                                    time.sleep(0.01) # Yield
                        except Exception as e:
                            print(f"Error processing {file}: {e}")
                                
            # Final batch
            if new_songs:
                self.scan_queue.put(("batch", new_songs))
                
            self.scan_queue.put(("done", None))
            
            if cache_updated:
                self._save_cache()
                
        except Exception as e:
            print(f"Scan error: {e}")
            self.scan_queue.put(("done", None))

    def _process_scan_queue(self):
        """Process updates from scan thread."""
        try:
            # Process only one batch at a time to avoid freezing UI or recursion depth issues
            try:
                msg_type, data = self.scan_queue.get_nowait()
                
                if msg_type == "batch":
                    self.all_songs.extend(data)
                    self._add_songs_to_tree(data)
                    self.count_label.config(text=f"{len(self.all_songs)} songs")
                    
                elif msg_type == "done":
                    self.is_scanning = False
                    self.refresh_btn.config(state="normal", text="🔄 Refresh")
                    # Final sort
                    self.all_songs.sort(key=lambda x: x['date'], reverse=True)
                    self.filtered_songs = self.all_songs.copy()
                    self.update_tree()
                    return # Stop processing
            except queue.Empty:
                pass
            
        except Exception as e:
            print(f"Queue error: {e}")
        
        if self.is_scanning or not self.scan_queue.empty():
            self.after(10, self._process_scan_queue)

    def _add_songs_to_tree(self, songs):
        """Add a batch of songs to the treeview."""
        try:
            for song in songs:
                duration_str = self.format_duration(song['duration'])
                size_str = self.format_size(song['filesize'])
                tag_icon = self._get_tag_icon(song)
                
                self.tree.insert("", "end", values=(
                    tag_icon,
                    song['title'],
                    song['artist'],
                    duration_str,
                    song['date'],
                    size_str
                ), tags=(song['filepath'].replace('\\', '/'),))
        except Exception as e:
            print(f"Tree insert error: {e}")

    def refresh_library(self):
        """Scan download folder and populate tree."""
        if self.is_scanning:
            return
            
        # Clear current
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        self.all_songs = []
        
        # Update path from config
        self.download_path = self.config_manager.get("path", "")
        
        if not self.download_path or (not os.path.exists(self.download_path) and not self.use_s3):
            # Silent return if not set, or maybe just show empty
            return
            
        # Start scanning
        self.is_scanning = True
        
        if self.use_s3:
             self.refresh_btn.config(state="disabled", text="Scanning S3...")
             self.count_label.config(text="Scanning S3...")
             threading.Thread(target=self._scan_s3_thread, daemon=True).start()
        else:
            self.refresh_btn.config(state="disabled", text="Scanning...")
            self.count_label.config(text="Scanning...")
            threading.Thread(target=self._scan_thread, daemon=True).start()
            
        self._process_scan_queue()
    
    def update_tree(self):
        """Update treeview with filtered songs."""
        # Clear existing
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Add songs
        self._add_songs_to_tree(self.filtered_songs)
    
        # self.update_tree()
        self.count_label.config(text=f"{len(self.filtered_songs)} / {len(self.all_songs)} songs")

    def toggle_filter(self, tag, color):
        """Toggle a tag filter."""
        self.active_filters[tag] = not self.active_filters[tag]
        
        # Update button visual
        btn = self.filter_btns[tag]
        if self.active_filters[tag]:
            btn.config(bg=color, fg="white")
        else:
            btn.config(bg=self.bg_card, fg=self.fg_secondary)
            
        # Re-apply filters
        self.on_search()

    def on_search(self, *args):
        """Filter songs by search query and tags."""
        query = self.search_var.get().lower()
        
        # Start with all songs
        candidates = self.all_songs
        
        # 1. Apply Tag Filters
        # If any filter is active, we only show songs that match AT LEAST ONE of the active filters
        # OR should it be AND? Usually filters are AND, but for tags like "Liked" and "Trash" they are mutually exclusive usually.
        # Let's assume OR for now if multiple selected, but usually user selects one.
        # Actually, let's do: if any filter is active, song must have one of the active tags.
        
        active_tags = [t for t, active in self.active_filters.items() if active]
        
        if active_tags:
            filtered_by_tags = []
            for song in candidates:
                # Get UUID for tag lookup (normalize filepath for consistency)
                uuid = song.get('id')
                if not uuid:
                    uuid = os.path.normpath(song.get('filepath', ''))
                
                # Look up tag (try normalized path first, then forward slashes)
                tag = self.tags.get(uuid)
                if not tag and uuid and os.path.sep in uuid:
                    uuid_alt = uuid.replace('\\', '/')
                    tag = self.tags.get(uuid_alt)
                
                if tag in active_tags:
                    filtered_by_tags.append(song)
            candidates = filtered_by_tags
            
        # 2. Apply Search Query
        if query:
            self.filtered_songs = [
                song for song in candidates
                if query in song['title'].lower() or query in song['artist'].lower()
            ]
        else:
            self.filtered_songs = list(candidates)
        
        self.update_tree()
        self.count_label.config(text=f"{len(self.filtered_songs)} / {len(self.all_songs)} songs")
    
    def sort_column(self, col):
        """Sort tree by column."""
        # Toggle sort order
        if not hasattr(self, 'sort_reverse'):
            self.sort_reverse = {}
        
        reverse = self.sort_reverse.get(col, False)
        self.sort_reverse[col] = not reverse
        
        # Sort
        if col == "duration":
            self.filtered_songs.sort(key=lambda x: x['duration'], reverse=reverse)
        elif col == "size":
            self.filtered_songs.sort(key=lambda x: x['filesize'], reverse=reverse)
        elif col == "title":
            self.filtered_songs.sort(key=lambda x: x['title'], reverse=reverse)
        elif col == "artist":
            self.filtered_songs.sort(key=lambda x: x['artist'], reverse=reverse)
        elif col == "date":
            self.filtered_songs.sort(key=lambda x: x['date'], reverse=reverse)
        
        self.update_tree()
    
    def on_selection_change(self, event=None):
        """Handle selection change - update player tag UI."""
        if self.player_widget:
            self.player_widget.update_tag_ui()
    
    def on_double_click(self, event):
        """Handle double-click on song."""
        self.play_selected()
    
    def play_selected(self):
        """Play the selected song (to be connected to player)."""
        selection = self.tree.selection()
        if not selection:
            return
        
        item = selection[0]
        # Get tag (which is filepath or S3 key)
        filepath = self.tree.item(item)['tags'][0]
        
        # S3 Support
        if self.use_s3:
            # excessive logic to get full song object not needed if we trust the tag/filepath
            # But we need the bucket from config
            s3_bucket = self.config_manager.get("s3_bucket", "")
            if not s3_bucket:
                messagebox.showerror("Configuration Error", "S3 Bucket not configured.")
                return

            # Generate presigned URL
            try:
                # Use public endpoint for playback if available
                s3_client = self._get_s3_client(use_public_endpoint=True)
                if not s3_client:
                    raise Exception("Failed to initialize S3 client")
                
                # The filepath in tree tag is s3://bucket/key
                # We need just the key
                key = filepath.replace(f"s3://{s3_bucket}/", "")
                
                url = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': s3_bucket,
                                                            'Key': key},
                                                    ExpiresIn=3600)
                
                if self.player_widget:
                    # Manually set metadata for the player
                    filename = os.path.basename(key)
                    title = filename
                    artist = "Unknown"
                    # Try to find in our list to get better metadata
                    for song in self.filtered_songs:
                         if song['id'] == key:
                             title = song['title']
                             artist = song['artist']
                             break
                    
                    print(f"DEBUG: Playing S3 URL: {url}")
                    self.player_widget.play_file(url)
                    self.player_widget.now_playing_label.config(text=title)
                    self.player_widget.artist_label.config(text=f"{artist} (Streamed from S3)")
                    
            except Exception as e:
                messagebox.showerror("S3 Error", f"Failed to generate playback URL: {e}")
            return

        # Local File Support
        # Normalize filepath
        filepath = os.path.normpath(filepath)
        
        # Verify file exists
        if not os.path.exists(filepath):
            import tkinter.messagebox as messagebox
            messagebox.showerror("File Not Found", f"File does not exist:\n{filepath}")
            return
        
        # Find index in filtered_songs
        index = -1
        for i, song in enumerate(self.filtered_songs):
            if os.path.normpath(song['filepath']) == filepath:
                index = i
                break
        
        if index != -1:
            self.current_playlist = self.filtered_songs
            self.current_index = index
            self.event_generate("<<PlaySong>>")
        else:
            # If not in filtered list, try to play directly
            if self.player_widget:
                self.player_widget.play_file(filepath)
    
    def select_song(self, filepath):
        """Select song in tree by filepath."""
        if not filepath:
            return
        
        # Normalize filepath for comparison
        filepath = os.path.normpath(filepath)
        
        # Also try with forward slashes for compatibility
        filepath_alt = filepath.replace('\\', '/')
            
        # Find item with matching tag
        # Since we don't have a direct map, we iterate. 
        # For large libraries this might be slow, but acceptable for now.
        for item in self.tree.get_children():
            try:
                item_tags = self.tree.item(item, "tags")
                if item_tags and len(item_tags) > 0:
                    item_filepath = os.path.normpath(item_tags[0])
                    item_filepath_alt = item_filepath.replace('\\', '/')
                    
                    # Try both normalized paths
                    if item_filepath == filepath or item_filepath == filepath_alt or \
                       item_filepath_alt == filepath or item_filepath_alt == filepath_alt:
                        self.tree.selection_set(item)
                        self.tree.see(item)
                        # Update player tag UI
                        if self.player_widget:
                            self.player_widget.update_tag_ui()
                        return
            except tk.TclError:
                # Item was deleted, skip it
                continue

    def get_selected_filepath(self):
        """Get filepath of selected song."""
        selection = self.tree.selection()
        if not selection:
            return None
        
        item = selection[0]
        return self.tree.item(item)['tags'][0]
    
    def show_context_menu(self, event):
        """Show right-click context menu."""
        # Select the item under cursor
        item = self.tree.identify_row(event.y)
        if item:
            self.tree.selection_set(item)
            self.context_menu.post(event.x_root, event.y_root)
    
    def toggle_storage_mode(self):
        """Toggle between Local and S3 storage."""
        if not BOTO3_AVAILABLE:
            messagebox.showerror("Missing Dependency", "boto3 is not installed or available.\nCannot access S3 storage.")
            return

        self.use_s3 = not self.use_s3
        
        if self.use_s3:
            self.s3_btn.config(text="☁️ Storage: S3", fg="white", bg=self.accent_purple)
            self.refresh_btn.config(text="🔄 Refresh S3")
        else:
            self.s3_btn.config(text="💾 Storage: Local", fg=self.fg_primary, bg=self.bg_card)
            self.refresh_btn.config(text="🔄 Refresh")
            
        self.refresh_library()

    def _scan_s3_thread(self):
        """Background thread to scan S3 bucket."""
        new_songs = []
        
        try:
            # Get config
            s3_endpoint = self.config_manager.get("s3_endpoint", "")
            s3_access_key = self.config_manager.get("s3_access_key", "")
            s3_secret_key = self.config_manager.get("s3_secret_key", "")
            s3_bucket = self.config_manager.get("s3_bucket", "")
            
            if not s3_endpoint or not s3_access_key or not s3_secret_key or not s3_bucket:
                print("S3 config missing")
                self.scan_queue.put(("error", "S3 Config Missing"))
                self.scan_queue.put(("done", None))
                return

            session = boto3.session.Session()
            s3_client = session.client(
                's3',
                endpoint_url=s3_endpoint,
                aws_access_key_id=s3_access_key,
                aws_secret_access_key=s3_secret_key,
                region_name='us-east-1'
            )
            
            # List objects
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=s3_bucket)
            
            for page in pages:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.lower().endswith(('.mp3', '.wav', '.flac')):
                        # Parse simple metadata from key/size/date
                        filename = os.path.basename(key)
                        size = obj['Size']
                        last_modified = obj['LastModified']
                        
                        # Try to parse Artist - Title from filename
                        title = filename
                        artist = "Unknown"
                        
                        # Remove extension
                        name_no_ext = os.path.splitext(filename)[0]
                        if " - " in name_no_ext:
                            parts = name_no_ext.split(" - ", 1)
                            artist = parts[0]
                            title = parts[1]
                        
                        song_data = {
                            'id': key, # Use S3 key as ID
                            'filepath': f"s3://{s3_bucket}/{key}", # Virtual path
                            'title': title,
                            'artist': artist,
                            'duration': 0, # Cannot easily get duration without downloading head
                            'filesize': size,
                            'date': last_modified.strftime('%Y-%m-%d %H:%M'),
                            'genre': '',
                            'tags': [],
                            's3_key': key,
                            's3_bucket': s3_bucket
                        }
                        
                        new_songs.append(song_data)
                        
                        if len(new_songs) >= 50:
                            self.scan_queue.put(("batch", list(new_songs)))
                            new_songs = []
            
            if new_songs:
                self.scan_queue.put(("batch", new_songs))
                
            self.scan_queue.put(("done", None))
            
        except Exception as e:
            print(f"S3 Scan error: {e}")
            self.scan_queue.put(("done", None))

    def open_download_folder(self):
        """Open the main download directory in system explorer."""
        if self.use_s3:
            messagebox.showinfo("S3 Storage", "Cannot open S3 bucket in file explorer.\nUse 'Web Console' or 'Change Folder' to switch to Local.")
            return
            
        path = self.config_manager.get("path", "")
        if path and os.path.exists(path):
            open_file(path)
        else:
            messagebox.showwarning("Error", "Download folder not set or does not exist.\nPlease configure it in the Downloader tab.")

    def change_library_folder(self):
        """Change the library folder."""
        from tkinter import filedialog
        current_path = self.config_manager.get("path", "")
        folder = filedialog.askdirectory(initialdir=current_path, title="Select Library Folder")
        if folder:
            self.config_manager.set("path", folder)
            self.config_manager.save_config()
            self.refresh_library()

    def show_about(self):
        """Show about dialog."""
        messagebox.showinfo("About SunoSync", 
            "SunoSync v2.0\n\n"
            "Your World, Your Music. Seamlessly Synced.\n\n"
            "Created by @InternetThot\n"
            "Buy me a coffee: buymeacoffee.com/audioalchemy")

    def open_folder(self):
        """Open folder containing selected song."""
        filepath = self.get_selected_filepath()
        if filepath:
            folder = os.path.dirname(filepath)
            open_file(folder)
    
    def edit_lyrics(self):
        """Open dialog to view/edit lyrics."""
        filepath = self.get_selected_filepath()
        if not filepath:
            messagebox.showwarning("No Selection", "Please select a song first.")
            return
            
        current_lyrics = ''
        song_title = "Unknown"
        
        # --- S3 Handling ---
        if self.use_s3:
            s3_bucket = self.config_manager.get("s3_bucket", "")
            if not s3_bucket: return
            
            # Extract key
            key = filepath.replace(f"s3://{s3_bucket}/", "")
            song_title = os.path.basename(key)
            
            # Try to get existing lyrics from .txt
            txt_key = os.path.splitext(key)[0] + ".txt"
            
            s3_client = self._get_s3_client() # Use internal endpoint for API ops
            if s3_client:
                try:
                    response = s3_client.get_object(Bucket=s3_bucket, Key=txt_key)
                    current_lyrics = response['Body'].read().decode('utf-8')
                except ClientError as e:
                    # 404 is fine, just means no lyrics yet
                    if e.response['Error']['Code'] != "NoSuchKey":
                        print(f"Error fetching lyrics: {e}")
                except Exception as e:
                    print(f"Error fetching lyrics: {e}")
        
        # --- Local File Handling ---
        else:
            # Normalize filepath for comparison
            filepath = os.path.normpath(filepath)
            
            # Always read fresh metadata from file to get latest lyrics
            try:
                song_meta = read_song_metadata(filepath)
                if not song_meta:
                    messagebox.showerror("Error", f"Could not read metadata from file:\n{filepath}")
                    return
                song_title = song_meta.get('title', os.path.basename(filepath))
            except Exception as e:
                messagebox.showerror("Error", f"Could not read file:\n{filepath}\n\nError: {e}")
                return
            
            # Get lyrics - prioritize .txt file if it exists, then metadata
            txt_path = os.path.splitext(filepath)[0] + ".txt"
            
            # First, check for .txt file
            if os.path.exists(txt_path):
                try:
                    with open(txt_path, 'r', encoding='utf-8') as f:
                        current_lyrics = f.read()
                except Exception as e:
                    print(f"Error reading lyrics from .txt file: {e}")
            
            # If no .txt file or empty, check metadata
            if not current_lyrics or current_lyrics.strip() == '':
                current_lyrics = song_meta.get('lyrics', '')
        
        # Create Dialog
        dialog = tk.Toplevel(self.winfo_toplevel())
        dialog.title(f"Lyrics: {song_title}")
        dialog.geometry("750x650")
        dialog.configure(bg=self.bg_dark)
        dialog.transient(self.winfo_toplevel())
        dialog.resizable(True, True)
        
        # Make sure dialog appears on top
        dialog.lift()
        dialog.focus_force()
        
        # Main container with proper layout
        main_container = tk.Frame(dialog, bg=self.bg_dark)
        main_container.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Text Area Frame (takes most space)
        text_frame = tk.Frame(main_container, bg=self.bg_card, padx=2, pady=2)
        text_frame.pack(side=tk.TOP, fill="both", expand=True, pady=(0, 20))
        
        # Text area with scrollbar
        text_scroll_frame = tk.Frame(text_frame, bg=self.bg_card)
        text_scroll_frame.pack(fill="both", expand=True)
        
        scrollbar = ttk.Scrollbar(text_scroll_frame, orient="vertical")
        scrollbar.pack(side=tk.RIGHT, fill="y")
        
        text_area = tk.Text(text_scroll_frame, font=("Segoe UI", 11), bg=self.bg_input, 
                           fg=self.fg_primary, wrap="word", relief="flat",
                           insertbackground=self.fg_primary, yscrollcommand=scrollbar.set)
        text_area.pack(side=tk.LEFT, fill="both", expand=True)
        scrollbar.config(command=text_area.yview)
        text_area.insert("1.0", current_lyrics)
        text_area.focus_set()
        
        # Buttons Frame (fixed at bottom)
        btn_frame = tk.Frame(main_container, bg=self.bg_dark)
        btn_frame.pack(side=tk.BOTTOM, fill="x")
        
        def save():
            new_lyrics = text_area.get("1.0", "end-1c")
            
            # --- S3 Save ---
            if self.use_s3:
                try:
                    s3_bucket = self.config_manager.get("s3_bucket", "")
                    key = filepath.replace(f"s3://{s3_bucket}/", "")
                    txt_key = os.path.splitext(key)[0] + ".txt"
                    
                    s3_client = self._get_s3_client()
                    if s3_client:
                        s3_client.put_object(
                            Bucket=s3_bucket,
                            Key=txt_key,
                            Body=new_lyrics.encode('utf-8'),
                            ContentType='text/plain'
                        )
                        messagebox.showinfo("Success", "Lyrics saved to S3 successfully!")
                        dialog.destroy()
                except Exception as e:
                     messagebox.showerror("Error", f"Failed to save lyrics to S3: {e}")
                return

            # --- Local Save ---
            normalized_filepath = os.path.normpath(filepath)
            
            # Save to .txt file first
            txt_path = os.path.splitext(normalized_filepath)[0] + ".txt"
            txt_saved = False
            try:
                with open(txt_path, 'w', encoding='utf-8') as f:
                    f.write(new_lyrics)
                txt_saved = True
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save lyrics to .txt file:\n{txt_path}\n\nError: {e}")
                return
            
            # Also save to audio file metadata
            metadata_saved = False
            if new_lyrics.strip():  # Only save to metadata if there's content
                success, message = save_lyrics_to_file(normalized_filepath, new_lyrics)
                metadata_saved = success
                if not success:
                    # Warn but don't fail if metadata save fails
                    print(f"Warning: Could not save lyrics to metadata: {message}")
            
            if txt_saved:
                # Update cache
                if 'song_meta' in locals() and song_meta:
                    song_meta['lyrics'] = new_lyrics
                
                # Verify .txt file was written correctly
                try:
                    with open(txt_path, 'r', encoding='utf-8') as f:
                        saved_lyrics = f.read()
                    
                    if saved_lyrics.replace('\r\n', '\n').strip() == new_lyrics.replace('\r\n', '\n').strip():
                        messagebox.showinfo("Success", "Lyrics saved successfully!\n\n" + 
                                          f"Saved to: {os.path.basename(txt_path)}" + 
                                          (f"\nAnd embedded in audio file metadata." if metadata_saved else ""))
                        dialog.destroy()
                    else:
                        messagebox.showwarning("Verification Failed", 
                            f".txt file saved but read-back verification failed.\n\n"
                            f"Expected length: {len(new_lyrics)}\n"
                            f"Actual on disk: {len(saved_lyrics)}\n\n"
                            f"File: {txt_path}")
                except Exception as e:
                    messagebox.showerror("Verification Error", f"Error reading back .txt file: {e}")
            else:
                messagebox.showerror("Error", f"Failed to save lyrics to .txt file:\n{txt_path}\n\nIf the song is playing, please STOP playback and try again.")
        
        cancel_btn = tk.Button(btn_frame, text="Cancel", command=dialog.destroy,
                              bg=self.bg_card, fg=self.fg_primary, font=("Segoe UI", 10),
                              relief="flat", padx=20, pady=10, cursor="hand2")
        cancel_btn.pack(side=tk.RIGHT, padx=(10, 0))
        
        save_btn = tk.Button(btn_frame, text="Save Lyrics", command=save,
                            bg=self.accent_purple, fg="white", font=("Segoe UI", 10, "bold"),
                            relief="flat", padx=20, pady=10, cursor="hand2")
        save_btn.pack(side=tk.RIGHT)
        
        # Ensure dialog is visible
        dialog.update_idletasks()
        dialog.deiconify()

    def tag_selected(self, tag):
        """Tag the selected song."""
        filepath = self.get_selected_filepath()
        if not filepath:
            messagebox.showwarning("No Selection", "Please select a song first.")
            return
        
        # Normalize filepath
        filepath = os.path.normpath(filepath)
        
        # Find song in all_songs to get UUID
        song = next((s for s in self.all_songs if os.path.normpath(s['filepath']) == filepath), None)
        if not song:
            # Try using filepath as UUID if song not found
            uuid = filepath
        else:
            uuid = song.get('id') or os.path.normpath(song['filepath'])
        
        if tag:
            self.tags[uuid] = tag
        else:
            # Remove tag
            if uuid in self.tags:
                del self.tags[uuid]
        
        # Save tags
        if self.tags_file:
            try:
                with open(self.tags_file, 'w', encoding='utf-8') as f:
                    json.dump(self.tags, f)
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save tag: {e}")
                return
        
        # Update UI
        self.update_tree()
        
        # Show confirmation
        tag_name = {"keep": "👍 Keep", "star": "⭐ Star", "trash": "🗑️ Trash"}.get(tag, tag)
        if tag:
            messagebox.showinfo("Tagged", f"Tagged as: {tag_name}")
        else:
            messagebox.showinfo("Tag Removed", "Tag removed successfully")
    
    def delete_selected(self):
        """Delete selected song."""
        filepath = self.get_selected_filepath()
        if not filepath:
            return
        
        if messagebox.askyesno("Delete", f"Delete this file?\n{os.path.basename(filepath)}"):
            try:
                os.remove(filepath)
                self.refresh_library()
                messagebox.showinfo("Deleted", "File deleted successfully")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to delete file:\n{e}")
    
    @staticmethod
    def format_duration(seconds):
        """Format duration as MM:SS."""
        if seconds == 0:
            return "--:--"
        mins = seconds // 60
        secs = seconds % 60
        return f"{mins}:{secs:02d}"
    
    @staticmethod
    def format_size(bytes):
        """Format file size."""
        if bytes == 0:
            return "0 KB"
        
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes < 1024.0:
                return f"{bytes:.1f} {unit}"
            bytes /= 1024.0
        
        return f"{bytes:.1f} TB"


if __name__ == "__main__":
    # Test the library tab standalone
    root = tk.Tk()
    root.title("Library Test")
    root.geometry("900x600")
    
    library = LibraryTab(root, download_path="Suno_Downloads")
    library.pack(fill="both", expand=True)
    
    root.mainloop()
