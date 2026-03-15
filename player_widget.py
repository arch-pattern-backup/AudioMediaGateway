import tkinter as tk
from tkinter import ttk
import os
import sys
VLC_IMPORT_ERROR = None

def setup_vlc():
    """Attempt to find and configure VLC before importing."""
    import ctypes
    
    # Common paths for libvlc on Linux/Mac/Windows
    possible_paths = [
        # Linux
        "/usr/lib/x86_64-linux-gnu/libvlc.so.5",
        "/usr/lib/libvlc.so.5",
        "/usr/lib/libvlc.so",
        "/usr/local/lib/libvlc.so",
        # Windows (common default installs)
        r"C:\Program Files\VideoLAN\VLC\libvlc.dll",
        r"C:\Program Files (x86)\VideoLAN\VLC\libvlc.dll",
        # Mac
        "/Applications/VLC.app/Contents/MacOS/lib/libvlc.dylib"
    ]
    
    # If standard import works, great
    try:
        import vlc
        # Test if it actually works
        i = vlc.Instance('--no-xlib')
        i.release()
        return True, vlc
    except:
        pass # Continue to manual search

    # Try setting VLC_PLUGIN_PATH or LD_LIBRARY_PATH if we find the lib
    found_lib = None
    for path in possible_paths:
        if os.path.exists(path):
            found_lib = path
            break
            
    if found_lib:
        print(f"DEBUG: Found libvlc at {found_lib}")
        directory = os.path.dirname(found_lib)
        
        # Windows: Add to PATH
        if sys.platform.startswith('win'):
            os.environ['PATH'] = directory + ';' + os.environ['PATH']
            if hasattr(os, 'add_dll_directory'):
                os.add_dll_directory(directory)
                
        # Linux: Try ctypes load global AND set plugin path
        if sys.platform.startswith('linux'):
            try:
                ctypes.CDLL(found_lib, mode=ctypes.RTLD_GLOBAL)
            except Exception as e:
                print(f"DEBUG: Failed to load libvlc via ctypes: {e}")
            
            # Try to guess plugin path
            # /usr/lib/x86_64-.../libvlc.so.5 -> /usr/lib/x86_64-.../vlc/plugins
            lib_dir = os.path.dirname(found_lib)
            plugin_path = os.path.join(lib_dir, 'vlc', 'plugins')
            if not os.path.exists(plugin_path):
                # Try one level up (e.g. /usr/lib/vlc/plugins)
                plugin_path = os.path.join(os.path.dirname(lib_dir), 'vlc', 'plugins')
            
            if os.path.exists(plugin_path):
                print(f"DEBUG: Setting VLC_PLUGIN_PATH to {plugin_path}")
                os.environ['VLC_PLUGIN_PATH'] = plugin_path

    try:
        import vlc
        return True, vlc
    except (ImportError, OSError) as e:
        return False, e

VLC_AVAILABLE, _vlc_result = setup_vlc()
if VLC_AVAILABLE:
    vlc = _vlc_result
    print(f"DEBUG: VLC module imported successfully. Version: {vlc.__version__}")
else:
    VLC_IMPORT_ERROR = str(_vlc_result)
    print(f"DEBUG: VLC Import Failed: {VLC_IMPORT_ERROR}")

import time
import json
import random
import math
from threading import Thread
from suno_utils import open_file

class AudioVisualizer(tk.Canvas):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, highlightthickness=0, **kwargs)
        self.bars = 32
        self.bar_data = [0] * self.bars
        self.target_data = [0] * self.bars
        self.is_active = False
        self.accent_color = "#8b5cf6"
        self.after(50, self._update_viz)

    def set_active(self, active):
        self.is_active = active

    def _update_viz(self):
        if self.is_active:
            # Generate pseudo-spectral data for visual interest
            # In a full implementation, we'd pull FFT data from VLC
            for i in range(self.bars):
                # Simulated movement
                change = random.randint(-20, 20)
                self.target_data[i] = max(5, min(80, self.target_data[i] + change))
                # Smooth interpolation
                self.bar_data[i] += (self.target_data[i] - self.bar_data[i]) * 0.3
        else:
            # Smoothly drop bars to zero
            for i in range(self.bars):
                self.bar_data[i] *= 0.8
                if self.bar_data[i] < 1: self.bar_data[i] = 0

        self.draw()
        self.after(40, self._update_viz)

    def draw(self):
        self.delete("all")
        w = self.winfo_width()
        h = self.winfo_height()
        if w < 10: return

        bar_w = w / self.bars
        for i in range(self.bars):
            val = self.bar_data[i]
            # Draw symmetrical bars from center
            bar_h = (val / 100) * h
            x0 = i * bar_w + 2
            y0 = h - bar_h
            x1 = (i + 1) * bar_w - 2
            y1 = h
            
            # Gradient effect
            alpha = int((val / 100) * 255)
            self.create_rectangle(x0, y0, x1, y1, fill=self.accent_color, outline="", tags="bar")
            
            # Top cap
            self.create_rectangle(x0, y0, x1, y0+2, fill="#a78bfa", outline="")

class PlayerWidget(tk.Frame):
    """Audio player widget with playback controls."""
    
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        
        # VLC instance

        # VLC instance
        self.vlc_error = None
        if VLC_AVAILABLE:
            try:
                print("DEBUG: Attempting to initialize VLC instance...")
                # Add network-related flags for S3/MinIO streaming
                # --no-xlib: Headless mode
                # --network-caching: Buffer for unstable connections
                # --http-referrer: Some S3 setups might need this
                vlc_args = [
                    '--no-xlib',
                    '--network-caching=3000', # 3s buffer
                    '--no-video' # Audio only
                ]
                self.instance = vlc.Instance(*vlc_args)
                print(f"DEBUG: VLC Instance created: {self.instance}")
                
                if self.instance is None:
                    raise Exception("Failed to create VLC Instance (libvlc could not be initialized).")

                self.player = self.instance.media_player_new()
                print(f"DEBUG: VLC Player created: {self.player}")
            except Exception as e:
                print(f"ERROR: VLC initialization failed: {e}")
                self.vlc_error = str(e)
                import traceback
                traceback.print_exc()
                # VLC not available or failed to initialize
                self.player = None
        else:
            print("DEBUG: VLC_AVAILABLE is False")
            self.vlc_error = VLC_IMPORT_ERROR or "VLC module not available"
            self.player = None
        
        # Player state
        self.current_file = None
        self.is_playing = False
        self.duration = 0
        self.playlist = []
        self.current_index = -1
        self.tags = {}
        self.tags_file = None
        self.library_tab = None  # Reference to library tab for tag operations
        
        # Playback modes
        self.shuffle_mode = False
        self.repeat_mode = 0  # 0: Off, 1: All, 2: One
        
        # Theme colors
        self.bg_dark = "#1a1a1a"
        self.bg_card = "#2d2d2d"
        self.fg_primary = "#e0e0e0"
        self.fg_secondary = "#9ca3af"
        self.accent_purple = "#8b5cf6"
        
        self.configure(bg=self.bg_dark)
        
        # Prevent the widget from being resized by children
        self.pack_propagate(False)
        # Set a fixed height that won't shrink - make it taller to prevent squishing
        self.config(height=160)  # Increased height for better visibility
        
        self.create_widgets()
        self.start_update_loop()
    
    def create_widgets(self):
        """Create player UI."""
        # Main container - ensure it fills the player widget properly
        container = tk.Frame(self, bg=self.bg_card)
        container.pack(fill="both", expand=True, padx=10, pady=6)
        # Don't use pack_propagate on container - let it fill naturally
        
        # --- Left: Song Info ---
        info_frame = tk.Frame(container, bg=self.bg_card, width=250)
        info_frame.pack(side=tk.LEFT, fill="y", padx=10)
        
        # Now playing label
        self.now_playing_label = tk.Label(info_frame, text="No song playing",
                                         bg=self.bg_card, fg=self.fg_primary,
                                         font=("Segoe UI", 11, "bold"),
                                         anchor="w")
        self.now_playing_label.pack(fill="x", pady=(15, 0))
        
        if not self.player:
            status_text = "⚠️ VLC Player not found"
            self.now_playing_label.config(text=status_text, fg="#ef4444")
            
            error_details = getattr(self, 'vlc_error', "Unknown error")
            lbl = tk.Label(info_frame, text="Playback disabled", 
                    bg=self.bg_card, fg=self.fg_secondary,
                    font=("Segoe UI", 9))
            lbl.pack(fill="x")
            
            # Create a tooltip with the actual error
            from suno_utils import create_tooltip
            create_tooltip(self.now_playing_label, f"Error details: {error_details}\n\nTip: Ensure 'python-vlc' is installed and VLC Media Player is on your system.")
        
        # Artist label
        self.artist_label = tk.Label(info_frame, text="",
                                     bg=self.bg_card, fg=self.fg_secondary,
                                     font=("Segoe UI", 9),
                                     anchor="w")
        self.artist_label.pack(fill="x")

        # --- Right: Volume ---
        volume_frame = tk.Frame(container, bg=self.bg_card)
        volume_frame.pack(side=tk.RIGHT, padx=10)
        
        tk.Label(volume_frame, text="🔊", bg=self.bg_card, fg=self.fg_primary,
                font=("Segoe UI", 14)).pack(side=tk.LEFT, padx=(0, 5))
        
        self.volume_var = tk.IntVar(value=70)
        self.volume_slider = ttk.Scale(volume_frame, from_=0, to=100,
                                      orient="horizontal",
                                      variable=self.volume_var,
                                      command=self.on_volume_change,
                                      length=100)
        self.volume_slider.pack(side=tk.LEFT)

        # --- Center: Controls & Seek ---
        center_frame = tk.Frame(container, bg=self.bg_card)
        center_frame.pack(side=tk.LEFT, fill="both", expand=True, padx=10)
        
        # Controls (Buttons)
        controls_frame = tk.Frame(center_frame, bg=self.bg_card)
        controls_frame.pack(side=tk.TOP, pady=(5, 2))
        
        # --- VISUALIZER ---
        self.visualizer = AudioVisualizer(center_frame, bg=self.bg_card, height=40)
        self.visualizer.pack(fill="x", padx=40, pady=(0, 5))
        
        btn_style = {
            "bg": self.bg_dark,
            "fg": self.fg_primary,
            "font": ("Segoe UI", 16),
            "relief": "flat",
            "cursor": "hand2",
            "width": 3,
            "height": 1
        }
        
        # Shuffle button
        self.shuffle_btn = tk.Button(controls_frame, text="🔀", **btn_style,
                                    command=self.toggle_shuffle)
        self.shuffle_btn.pack(side=tk.LEFT, padx=5)
        self.shuffle_btn.config(fg=self.fg_secondary) # Default off

        # Previous button
        self.prev_btn = tk.Button(controls_frame, text="⏮", **btn_style,
                                  command=self.previous_song)
        self.prev_btn.pack(side=tk.LEFT, padx=5)
        
        # Play/Pause button (larger font)
        play_style = btn_style.copy()
        play_style["font"] = ("Segoe UI", 20)
        self.play_btn = tk.Button(controls_frame, text="▶", **play_style,
                                  command=self.toggle_playback)
        self.play_btn.pack(side=tk.LEFT, padx=5)
        
        # Stop button
        self.stop_btn = tk.Button(controls_frame, text="⏹", **btn_style,
                                  command=self.stop)
        self.stop_btn.pack(side=tk.LEFT, padx=5)
        
        # Next button
        self.next_btn = tk.Button(controls_frame, text="⏭", **btn_style,
                                  command=self.next_song)
        self.next_btn.pack(side=tk.LEFT, padx=5)

        # Repeat button
        self.repeat_btn = tk.Button(controls_frame, text="🔁", **btn_style,
                                   command=self.toggle_repeat)
        self.repeat_btn.pack(side=tk.LEFT, padx=5)
        self.repeat_btn.config(fg=self.fg_secondary) # Default off
        
        # Tagging buttons
        tag_frame = tk.Frame(controls_frame, bg=self.bg_card)
        tag_frame.pack(side=tk.LEFT, padx=(20, 0))
        
        self.tag_btns = {}
        tags = [("👍", "keep", "#22c55e"), ("🗑️", "trash", "#ef4444"), ("⭐", "star", "#eab308")]
        
        for icon, tag, color in tags:
            btn = tk.Button(tag_frame, text=icon, **btn_style,
                           command=lambda t=tag: self.toggle_tag(t))
            btn.pack(side=tk.LEFT, padx=2)
            self.tag_btns[tag] = btn
            self.tag_colors = {tag: color for _, tag, color in tags}
            
        # Seek bar frame
        seek_frame = tk.Frame(center_frame, bg=self.bg_card)
        seek_frame.pack(side=tk.TOP, fill="x", padx=20)
        
        # Current time
        self.time_label = tk.Label(seek_frame, text="0:00",
                                   bg=self.bg_card, fg=self.fg_secondary,
                                   font=("Segoe UI", 8))
        self.time_label.pack(side=tk.LEFT, padx=(0, 5))
        
        # Seek slider
        self.seek_var = tk.IntVar(value=0)
        self.seek_slider = ttk.Scale(seek_frame, from_=0, to=100,
                                     orient="horizontal",
                                     variable=self.seek_var,
                                     command=self.on_seek)
        self.seek_slider.pack(side=tk.LEFT, fill="x", expand=True)
        
        # Duration time
        self.duration_label = tk.Label(seek_frame, text="0:00",
                                       bg=self.bg_card, fg=self.fg_secondary,
                                       font=("Segoe UI", 8))
        self.duration_label.pack(side=tk.LEFT, padx=(5, 0))
        
        # Set initial volume
        if self.player:
            self.player.audio_set_volume(70)

    def set_tags_file(self, filepath):
        """Set path to tags JSON file and load it."""
        self.tags_file = filepath
        self._load_tags()

    def _load_tags(self):
        if self.tags_file and os.path.exists(self.tags_file):
            try:
                with open(self.tags_file, 'r', encoding='utf-8') as f:
                    self.tags = json.load(f)
            except:
                self.tags = {}

    def _save_tags(self):
        if self.tags_file:
            try:
                # Ensure directory exists
                tags_dir = os.path.dirname(self.tags_file)
                if tags_dir and not os.path.exists(tags_dir):
                    os.makedirs(tags_dir, exist_ok=True)
                
                with open(self.tags_file, 'w', encoding='utf-8') as f:
                    json.dump(self.tags, f, indent=2)
            except Exception as e:
                print(f"Error saving tags to {self.tags_file}: {e}")
                import traceback
                traceback.print_exc()
                raise  # Re-raise so toggle_tag can catch it

    def set_playlist(self, songs, start_index=0):
        """Set the current playlist and start playing."""
        self.playlist = songs
        self.current_index = start_index
        if 0 <= self.current_index < len(self.playlist):
            self.play_song_at_index(self.current_index)

    def play_song_at_index(self, index):
        """Play song at specific playlist index."""
        if not 0 <= index < len(self.playlist):
            return
            
        self.current_index = index
        song = self.playlist[index]
        
        # Normalize filepath before playing
        filepath = os.path.normpath(song['filepath'])
        
        # Play the file
        success = self.play_file(filepath)
        
        if success:
            self.update_tag_ui(song.get('id'))
            
            # Emit track changed event after a small delay to ensure current_file is set
            self.after(100, lambda: self.event_generate("<<TrackChanged>>"))

    def set_library_tab(self, library_tab):
        """Set reference to library tab for tag operations."""
        self.library_tab = library_tab
    
    def toggle_tag(self, tag):
        """Toggle a tag for the current song (playing or selected in library)."""
        # Try to get song from currently playing track
        uuid = None
        filepath = None
        
        if self.current_index >= 0 and self.current_index < len(self.playlist):
            # Song is playing
            song = self.playlist[self.current_index]
            uuid = song.get('id')
            filepath = song.get('filepath')
        elif hasattr(self, 'library_tab') and self.library_tab:
            # Try to get from library selection
            filepath = self.library_tab.get_selected_filepath()
            if filepath:
                # Normalize filepath for comparison
                filepath = os.path.normpath(filepath)
                # Find song in library to get UUID (normalize both for comparison)
                song = next((s for s in self.library_tab.all_songs if os.path.normpath(s.get('filepath', '')) == filepath), None)
                if song:
                    uuid = song.get('id') or os.path.normpath(song.get('filepath', ''))
                else:
                    uuid = filepath
        
        if not uuid and not filepath:
            # No song available
            import tkinter.messagebox as messagebox
            messagebox.showinfo("No Song Selected", "Please select a song from the library or play a song first.")
            return
        
        if not uuid:
            uuid = os.path.normpath(filepath) if filepath else None
            
        if not uuid:
            return
            
        # Normalize UUID for consistent lookup
        if os.path.sep in uuid:
            uuid = os.path.normpath(uuid)
            
        current_tag = self.tags.get(uuid)
        
        if current_tag == tag:
            # Untag
            if uuid in self.tags:
                del self.tags[uuid]
        else:
            # Set tag
            self.tags[uuid] = tag
        
        try:
            self._save_tags()
            self.update_tag_ui(uuid)
            
            # Notify library to update UI (use after() with delay to ensure thread safety)
            self.after(100, lambda: self.event_generate("<<TagsUpdated>>"))
        except Exception as e:
            import tkinter.messagebox as messagebox
            import traceback
            error_msg = f"Error saving tag: {e}\n\n{traceback.format_exc()}"
            messagebox.showerror("Tag Error", error_msg)
            print(f"TAG ERROR: {error_msg}")

    def update_tag_ui(self, uuid=None):
        """Update tag buttons state."""
        # If no UUID provided, try to get from current song or library selection
        if not uuid:
            if self.current_index >= 0 and self.current_index < len(self.playlist):
                song = self.playlist[self.current_index]
                uuid = song.get('id') or song.get('filepath')
            elif hasattr(self, 'library_tab') and self.library_tab:
                filepath = self.library_tab.get_selected_filepath()
                if filepath:
                    # Normalize filepath for comparison
                    filepath = os.path.normpath(filepath)
                    song = next((s for s in self.library_tab.all_songs if os.path.normpath(s.get('filepath', '')) == filepath), None)
                    if song:
                        uuid = song.get('id') or filepath
                    else:
                        uuid = filepath
        
        # Normalize UUID if it's a filepath
        if uuid and os.path.sep in str(uuid):
            uuid = os.path.normpath(uuid)
        
        current_tag = self.tags.get(uuid) if uuid else None
        
        # If we still don't have a tag, try with alternative path format
        if not current_tag and uuid and os.path.sep in str(uuid):
            uuid_alt = str(uuid).replace('\\', '/')
            current_tag = self.tags.get(uuid_alt)
            if current_tag:
                # Update tags dict to use normalized path
                self.tags[uuid] = current_tag
                if uuid_alt in self.tags:
                    del self.tags[uuid_alt]
        
        for tag, btn in self.tag_btns.items():
            if tag == current_tag:
                btn.config(bg=self.tag_colors[tag], fg="white")
            else:
                btn.config(bg=self.bg_dark, fg=self.fg_primary)

    def play_file(self, filepath):
        """Play a specific file or URL."""
        if not filepath:
            return False
            
        is_url = filepath.lower().startswith(('http://', 'https://', 's3://'))
        
        # Normalize ONLY if it's a local file path
        if not is_url:
            filepath = os.path.normpath(filepath)
        
        if not VLC_AVAILABLE:
            import tkinter.messagebox as messagebox
            messagebox.showerror("VLC Not Available", "VLC is not installed or not available.\nPlease install python-vlc to use the audio player.")
            return False
        
        if not self.player or not self.instance:
            import tkinter.messagebox as messagebox
            messagebox.showerror("Player Error", "VLC player failed to initialize.")
            return False
        
        if not is_url and not os.path.exists(filepath):
            import tkinter.messagebox as messagebox
            messagebox.showerror("File Not Found", f"File does not exist:\n{filepath}")
            return False
        
        # Set current_file
        self.current_file = filepath

        try:
            # Stop current playback if any
            if self.is_playing:
                self.player.stop()
            
            # Load media - use media_new_location for URLs
            if is_url:
                print(f"DEBUG: Loading media location: {filepath}")
                media = self.instance.media_new_location(filepath)
            else:
                print(f"DEBUG: Loading media file: {filepath}")
                media = self.instance.media_new_path(filepath)
                
            if not media:
                import tkinter.messagebox as messagebox
                messagebox.showerror("Media Error", f"Failed to load media from:\n{filepath}")
                return False
                
            self.player.set_media(media)
            
            # Start playback
            result = self.player.play()
            if result != 0:
                import tkinter.messagebox as messagebox
                messagebox.showerror("Playback Error", f"VLC play() returned error code: {result}\nFile: {filepath}")
                return False
            
            self.is_playing = True
            self.visualizer.set_active(True)
            self.play_btn.config(text="⏸")
            
            # Wait for media to parse (with timeout)
            for _ in range(50):  # 5 seconds max
                time.sleep(0.1)
                length = self.player.get_length()
                if length > 0:
                    self.duration = length // 1000
                    break
            else:
                self.duration = 0
            
            # Update UI
            filename = os.path.basename(filepath)
            title = os.path.splitext(filename)[0].replace('_', ' ')
            self.now_playing_label.config(text=title)
            self.artist_label.config(text=f"Playing from: {os.path.dirname(filepath)}")
            
            # Update duration label
            self.duration_label.config(text=self.format_time(self.duration))
            return True
        except Exception as e:
            import tkinter.messagebox as messagebox
            import traceback
            error_msg = f"Failed to play file:\n{filepath}\n\nError: {e}\n\n{traceback.format_exc()}"
            messagebox.showerror("Playback Error", error_msg)
            print(f"PLAYBACK ERROR: {error_msg}")  # Also print to console
            return False
    
    def toggle_playback(self):
        """Toggle play/pause."""
        if not self.player: return
        
        if not self.current_file:
            return
        
        if self.is_playing:
            self.player.pause()
            self.is_playing = False
            self.visualizer.set_active(False)
            self.play_btn.config(text="▶")
        else:
            self.player.play()
            self.is_playing = True
            self.visualizer.set_active(True)
            self.play_btn.config(text="⏸")
    
    def stop(self):
        """Stop playback."""
        if not self.player: return

        self.player.stop()
        self.is_playing = False
        self.visualizer.set_active(False)
        self.play_btn.config(text="▶")
        self.seek_var.set(0)
        self.time_label.config(text="0:00")
    
    def on_seek(self, value):
        """Handle seek slider change."""
        if not self.player: return

        if not self.current_file or not self.is_playing:
            return
        
        # Convert slider value (0-100) to position (0.0-1.0)
        position = float(value) / 100.0
        self.player.set_position(position)
    
    def on_volume_change(self, value):
        """Handle volume slider change."""
        if not self.player: return

        volume = int(float(value))
        self.player.audio_set_volume(volume)
    
    def toggle_shuffle(self):
        """Toggle shuffle mode."""
        self.shuffle_mode = not self.shuffle_mode
        if self.shuffle_mode:
            self.shuffle_btn.config(fg=self.accent_purple)
        else:
            self.shuffle_btn.config(fg=self.fg_secondary)

    def toggle_repeat(self):
        """Toggle repeat mode: Off -> All -> One -> Off."""
        self.repeat_mode = (self.repeat_mode + 1) % 3
        
        if self.repeat_mode == 0: # Off
            self.repeat_btn.config(text="🔁", fg=self.fg_secondary)
        elif self.repeat_mode == 1: # All
            self.repeat_btn.config(text="🔁", fg=self.accent_purple)
        elif self.repeat_mode == 2: # One
            self.repeat_btn.config(text="🔂", fg=self.accent_purple)

    def previous_song(self):
        """Play previous song."""
        if not self.playlist: return
        
        # If playing > 3 seconds, restart song
        if self.player and self.player.get_time() > 3000:
            self.player.set_time(0)
            return

        if self.shuffle_mode:
            # Random previous isn't standard, usually we go back in history. 
            # For simplicity, just random or previous index.
            # Let's just go to previous index for now, shuffle usually only affects 'next'
            pass
            
        new_index = self.current_index - 1
        if new_index < 0:
            if self.repeat_mode == 1: # Loop all
                new_index = len(self.playlist) - 1
            else:
                return # Stop at start
                
        self.play_song_at_index(new_index)
    
    def next_song(self):
        """Play next song."""
        if not self.playlist: return
        
        if self.repeat_mode == 2: # Repeat One
            self.play_song_at_index(self.current_index)
            return

        if self.shuffle_mode:
            # Pick random index
            new_index = random.randint(0, len(self.playlist) - 1)
            # Try not to pick same song unless playlist is size 1
            if len(self.playlist) > 1 and new_index == self.current_index:
                new_index = (new_index + 1) % len(self.playlist)
            self.play_song_at_index(new_index)
            return

        new_index = self.current_index + 1
        if new_index >= len(self.playlist):
            if self.repeat_mode == 1: # Loop all
                new_index = 0
            else:
                return # Stop at end
                
        self.play_song_at_index(new_index)
    
    def start_update_loop(self):
        """Start the UI update loop using after() for thread safety."""
        self._update_ui()
        
    def _update_ui(self):
        """Update UI elements. Must run on main thread."""
        if self.is_playing and self.duration > 0 and self.player:
            try:
                # Update seek bar and time
                position = self.player.get_position()
                if position >= 0:
                    current_time = int(position * self.duration)
                    # Only update if not dragging (optional optimization, but simple set is fine)
                    self.seek_var.set(int(position * 100))
                    self.time_label.config(text=self.format_time(current_time))
                
                # Check if song ended
                state = self.player.get_state()
                if state == vlc.State.Ended:
                    self.is_playing = False
                    self.visualizer.set_active(False)
                    self.play_btn.config(text="▶")
                    self.next_song()  # Auto-play next
            except Exception:
                pass
        
        # Schedule next update
        self.after(500, self._update_ui)
    
    @staticmethod
    def format_time(seconds):
        """Format time as M:SS."""
        if seconds < 0:
            return "0:00"
        mins = seconds // 60
        secs = seconds % 60
        return f"{mins}:{secs:02d}"


if __name__ == "__main__":
    # Test the player standalone
    root = tk.Tk()
    root.title("Player Test")
    root.geometry("800x120")
    root.configure(bg="#1a1a1a")
    
    player = PlayerWidget(root)
    player.pack(fill="both", expand=True)
    
    # Test with a file (replace with actual path)
    test_file = "Suno_Downloads/test.mp3"  # Change this
    if os.path.exists(test_file):
        player.play_file(test_file)
    
    root.mainloop()
