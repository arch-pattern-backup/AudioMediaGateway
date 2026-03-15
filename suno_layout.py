"""Layout builders and dialog helpers for SunoSync GUI."""
import os
import tkinter as tk
from tkinter import ttk
from tkinter import filedialog, messagebox
import webbrowser
import pyperclip
from suno_widgets import RoundedButton, CollapsibleCard, FilterPopup, WorkspaceBrowser

def create_auth_card(parent, app):
    """Create the authorization card with token input."""
    card = CollapsibleCard(parent, title="Authorization", bg_color=app.card_bg,
                          corner_radius=12, padding=12, collapsed=False)
    card.pack(fill="x", pady=(0, 12))
    body = card.body
    tk.Label(body, text="Bearer Token", font=("Segoe UI", 9, "bold"),
            bg=app.bg_card, fg=app.fg_secondary).pack(anchor="w", padx=12, pady=(8, 4))
    
    # Container for Input + Button
    row = tk.Frame(body, bg=app.bg_card)
    row.pack(fill="x", padx=12, pady=(0, 12))
    
    # Input Field
    token_container = tk.Frame(row, bg=app.bg_input, highlightbackground=app.border_subtle, highlightthickness=1)
    token_container.pack(side=tk.LEFT, fill="x", expand=True, padx=(0, 10))
    
    app.token_var = tk.StringVar()
    app.token_entry = tk.Entry(token_container, textvariable=app.token_var, show="●",
                               font=("Segoe UI", 10), bg=app.bg_input, fg=app.fg_primary,
                               insertbackground=app.fg_primary, relief="flat", bd=0, highlightthickness=0)
    app.token_entry.pack(side=tk.LEFT, fill="x", expand=True, padx=10, pady=8)
    
    # Get Token Button (Small, Secondary)
    get_token_btn = RoundedButton(row, "Get Token", app.get_token_logic,
                                  bg_color=app.bg_input, fg_color=app.accent_purple,
                                  hover_color=app.bg_dark, font=("Segoe UI", 9, "bold"),
                                  width=90, height=36, corner_radius=8, border_color=app.border_subtle)
    get_token_btn.pack(side=tk.RIGHT)
    app.create_tooltip(get_token_btn, "Click to open instructions for getting your token.")
    
    return card


def create_settings_card(parent, app, base_path):
    """Create the settings card with path and toggles."""
    card = CollapsibleCard(parent, title="Download Settings", bg_color=app.card_bg,
                          corner_radius=12, padding=12, collapsed=False)
    card.pack(fill="x", pady=(0, 12))
    body = card.body
    
    # --- Path Selection ---
    tk.Label(body, text="Download Folder", font=("Segoe UI", 9, "bold"),
            bg=app.bg_card, fg=app.fg_secondary).pack(anchor="w", padx=12, pady=(8, 4))
            
    path_row = tk.Frame(body, bg=app.bg_card)
    path_row.pack(fill="x", padx=12, pady=(0, 12))
    
    app.path_var = tk.StringVar(value=os.path.join(base_path, "Suno_Downloads"))
    app.path_display_var = tk.StringVar()
    
    path_container = tk.Frame(path_row, bg=app.bg_input, highlightbackground=app.border_subtle, highlightthickness=1)
    path_container.pack(side=tk.LEFT, fill="x", expand=True, padx=(0, 8))
    
    path_entry = tk.Entry(path_container, textvariable=app.path_display_var, state="readonly",
                         font=("Segoe UI", 9), bg=app.bg_input, fg=app.fg_secondary,
                         relief="flat", bd=0, highlightthickness=0, readonlybackground=app.bg_input)
    path_entry.pack(fill="x", padx=10, pady=8)
    app.create_tooltip(path_entry, "Full path: " + app.path_var.get())
    
    browse_btn = RoundedButton(path_row, "Browse", app.browse_folder,
                              bg_color=app.bg_input, fg_color=app.fg_primary,
                              hover_color=app.bg_dark, font=("Segoe UI", 9),
                              width=80, height=36, corner_radius=8, border_color=app.border_subtle)
    browse_btn.pack(side=tk.LEFT)

    # --- Toggles Grid ---
    toggles_frame = tk.Frame(body, bg=app.bg_card)
    toggles_frame.pack(fill="x", padx=12, pady=5)
    
    # Configure grid columns to have minimum widths
    toggles_frame.columnconfigure(0, weight=1, minsize=180)
    toggles_frame.columnconfigure(1, weight=1, minsize=180)
    
    # Helper for grid items
    def add_toggle(row, col, text, var, tooltip):
        f = tk.Frame(toggles_frame, bg=app.bg_card)
        f.grid(row=row, column=col, sticky="w", padx=(0, 20), pady=6)
        app.create_toggle_option(f, text, var) # Uses existing toggle logic but packed into grid frame
        app.create_tooltip(f, tooltip)

    app.embed_thumb_var = tk.BooleanVar(value=True)
    add_toggle(0, 0, "Embed Metadata", app.embed_thumb_var, "Adds ID3 tags and album art")
    
    app.download_wav_var = tk.BooleanVar(value=False)
    add_toggle(0, 1, "Prefer WAV", app.download_wav_var, "Download WAV if available")
    
    app.organize_var = tk.BooleanVar(value=False)
    add_toggle(1, 0, "Monthly Folders", app.organize_var, "Sort into YYYY-MM folders")
    
    app.save_lyrics_var = tk.BooleanVar(value=True)
    add_toggle(1, 1, "Save Lyrics (.txt)", app.save_lyrics_var, "Save lyrics to a separate text file")

    app.track_folder_var = tk.BooleanVar(value=False)
    add_toggle(2, 0, "Stem Track Folder", app.track_folder_var, "Create a folder for each track")

    app.smart_resume_var = tk.BooleanVar(value=False)
    add_toggle(2, 1, "Smart Resume", app.smart_resume_var, "Stop scanning after consecutive pages with no new songs (adaptive: 2-20 pages based on library size)")
    
    app.disable_sounds_var = tk.BooleanVar(value=False)
    add_toggle(3, 0, "Disable Notification Sounds", app.disable_sounds_var, "Turn off Windows alert notification sounds")
    
    # --- Storage Settings ---
    tk.Label(body, text="Storage Location", font=("Segoe UI", 9, "bold"),
            bg=app.bg_card, fg=app.fg_secondary).pack(anchor="w", padx=12, pady=(12, 4))
            
    storage_row = tk.Frame(body, bg=app.bg_card)
    storage_row.pack(fill="x", padx=12, pady=(0, 8))
    
    # Radio Buttons for Storage Type
    def create_radio(parent, text, value, var):
        r = tk.Radiobutton(parent, text=text, variable=var, value=value,
                          bg=app.bg_card, fg=app.fg_primary, selectcolor=app.bg_card,
                          activebackground=app.bg_card, activeforeground=app.fg_primary,
                          font=("Segoe UI", 9))
        r.pack(side=tk.LEFT, padx=(0, 15))
        return r

    create_radio(storage_row, "Local Folder", "local", app.storage_type_var)
    create_radio(storage_row, "S3 / MinIO", "s3", app.storage_type_var)
    
    # --- ADDED: Sync Button and Label next to Storage Type ---
    app.sync_btn = RoundedButton(storage_row, "Full Sync", app.start_s3_sync_thread,
                               bg_color=app.accent_purple, fg_color="white",
                               hover_color=app.bg_dark, font=("Segoe UI", 8, "bold"),
                               width=100, height=28, corner_radius=6, border_color=app.border_subtle)
    app.sync_btn.pack(side=tk.LEFT, padx=(5, 0))
    app.create_tooltip(app.sync_btn, "Full S3 Inventory Sync (Authoritative Rebuild)")

    app.sync_label_frame = tk.Frame(storage_row, bg=app.bg_card)
    app.sync_label_frame.pack(side=tk.LEFT, padx=10)
    tk.Label(app.sync_label_frame, text="Last Sync:", font=("Segoe UI", 7), bg=app.bg_card, fg=app.fg_secondary).pack(anchor="w")
    tk.Label(app.sync_label_frame, textvariable=app.last_full_sync_var, font=("Segoe UI", 7, "bold"), bg=app.bg_card, fg=app.fg_primary).pack(anchor="w")

    # S3 Settings Container
    app.s3_settings_frame = tk.Frame(body, bg=app.bg_card)
    # Default packed if S3 is selected
    if app.storage_type_var.get() == "s3":
        app.s3_settings_frame.pack(fill="x", padx=12, pady=(0, 12))
    
    # Grid for S3 inputs
    s3_grid = tk.Frame(app.s3_settings_frame, bg=app.bg_card)
    s3_grid.pack(fill="x")
    s3_grid.columnconfigure(0, weight=1)
    s3_grid.columnconfigure(1, weight=1)
    
    def add_s3_input(row, col, label, var, show=None):
        f = tk.Frame(s3_grid, bg=app.bg_card)
        f.grid(row=row, column=col, sticky="ew", padx=4, pady=4)
        
        tk.Label(f, text=label, font=("Segoe UI", 8), bg=app.bg_card, fg=app.fg_secondary).pack(anchor="w")
        
        c = tk.Frame(f, bg=app.bg_input, highlightbackground=app.border_subtle, highlightthickness=1)
        c.pack(fill="x", pady=(2, 0))
        
        e = tk.Entry(c, textvariable=var, font=("Segoe UI", 9), bg=app.bg_input, fg=app.fg_primary,
                    insertbackground=app.fg_primary, relief="flat", bd=0, highlightthickness=0, show=show)
        e.pack(fill="x", padx=6, pady=4, ipady=3)
        return f

    add_s3_input(0, 0, "Endpoint URL", app.s3_endpoint_var)
    add_s3_input(0, 1, "Bucket Name", app.s3_bucket_var)
    add_s3_input(1, 0, "Access Key", app.s3_access_key_var)
    add_s3_input(1, 1, "Secret Key", app.s3_secret_key_var, show="●")
    add_s3_input(2, 0, "Region (Optional)", app.s3_region_var)
    add_s3_input(2, 1, "Public Endpoint (Optional)", app.s3_public_endpoint_var)
    add_s3_input(3, 0, "Path Prefix (Optional)", app.s3_path_prefix_var)

    # Migration Button
    migrate_row = tk.Frame(app.s3_settings_frame, bg=app.bg_card)
    migrate_row.pack(fill="x", pady=(12, 0))
    
    migrate_btn = RoundedButton(migrate_row, "Migrate Local to S3", app.start_migration_thread,
                                bg_color=app.bg_input, fg_color=app.accent_purple,
                                hover_color=app.bg_dark, font=("Segoe UI", 9, "bold"),
                                width=160, height=32, corner_radius=8, border_color=app.border_subtle)
    app.migrate_btn = migrate_btn
    migrate_btn.pack(side=tk.RIGHT)
    app.create_tooltip(migrate_btn, "Scan local folder and upload all files to the configured S3 bucket")

    return card


def create_scraping_card(parent, app):
    """Create the scraping options card."""
    card = CollapsibleCard(parent, title="Scraping Options", bg_color=app.card_bg,
                          corner_radius=12, padding=12, collapsed=False)
    card.pack(fill="x", pady=(0, 12))
    body = card.body
    
    # Horizontal layout for controls
    row = tk.Frame(body, bg=app.bg_card)
    row.pack(fill="x", padx=12, pady=12)
    
    # Helper for labeled inputs
    def add_input(parent_frame, label, var, width=5, tooltip=""):
        f = tk.Frame(parent_frame, bg=app.bg_card)
        f.pack(side=tk.LEFT, padx=(0, 24))
        tk.Label(f, text=label, font=("Segoe UI", 9, "bold"), bg=app.bg_card, fg=app.fg_secondary).pack(anchor="w")
        
        c = tk.Frame(f, bg=app.bg_input, highlightbackground=app.border_subtle, highlightthickness=1)
        c.pack(fill="x", pady=(4, 0))
        
        s = tk.Spinbox(c, from_=0, to=999, textvariable=var, font=("Segoe UI", 9), width=width,
                       bg=app.bg_input, fg=app.fg_primary, relief="flat", bd=0, highlightthickness=0,
                       buttonbackground=app.bg_input)
        s.pack(padx=8, pady=6)
        if tooltip: app.create_tooltip(s, tooltip)
        return s

    app.rate_limit_var = tk.DoubleVar(value=0.5)
    add_input(row, "Delay (s)", app.rate_limit_var, width=5, tooltip="Seconds between downloads")
    
    app.start_page_var = tk.IntVar(value=1)
    add_input(row, "Start Page", app.start_page_var, width=5, tooltip="Page to start from")
    
    app.max_pages_var = tk.IntVar(value=0)
    add_input(row, "Max Pages", app.max_pages_var, width=5, tooltip="0 = All Pages")

    # Filters Button
    filter_frame = tk.Frame(body, bg=app.bg_card)
    filter_frame.pack(fill="x", padx=12, pady=(0, 12))
    
    app.filter_btn = RoundedButton(filter_frame, "Filters", app.open_filters,
                                  bg_color=app.bg_input, fg_color=app.fg_primary,
                                  hover_color=app.bg_dark, font=("Segoe UI", 9),
                                  width=95, height=36, corner_radius=8, border_color=app.border_subtle)
    app.filter_btn.pack(side=tk.LEFT, fill="x", expand=True, padx=(0, 4))
    app.create_tooltip(app.filter_btn, "Configure advanced download filters")

    app.workspace_btn = RoundedButton(filter_frame, "Workspaces", app.open_workspaces,
                                     bg_color=app.bg_input, fg_color=app.fg_primary,
                                     hover_color=app.bg_dark, font=("Segoe UI", 9),
                                     width=95, height=36, corner_radius=8, border_color=app.border_subtle)
    app.workspace_btn.pack(side=tk.LEFT, fill="x", expand=True, padx=(4, 4))
    app.create_tooltip(app.workspace_btn, "Select a specific workspace to download from")

    app.playlist_btn = RoundedButton(filter_frame, "Playlists", app.open_playlists,
                                     bg_color=app.bg_input, fg_color=app.fg_primary,
                                     hover_color=app.bg_dark, font=("Segoe UI", 9),
                                     width=95, height=36, corner_radius=8, border_color=app.border_subtle)
    app.playlist_btn.pack(side=tk.LEFT, fill="x", expand=True, padx=(4, 0))
    app.create_tooltip(app.playlist_btn, "Select a specific playlist to download from")

    # Preload Button
    preload_frame = tk.Frame(body, bg=app.bg_card)
    preload_frame.pack(fill="x", padx=12, pady=(0, 12))
    
    app.preload_btn = RoundedButton(preload_frame, "Preload List", app.preload_songs,
                                   bg_color=app.bg_input, fg_color=app.accent_pink,
                                   hover_color=app.bg_dark, font=("Segoe UI", 9, "bold"),
                                   width=200, height=36, corner_radius=8, border_color=app.border_subtle)
    app.preload_btn.pack(fill="x")
    app.create_tooltip(app.preload_btn, "Fetch song list without downloading to select specific tracks")

    return card


def create_action_area(parent, app):
    """Create the action buttons area."""
    frame = tk.Frame(parent, bg=app.bg_dark)
    frame.pack(fill="x", pady=10)
    
    # Start Button (Primary)
    app.start_btn = RoundedButton(frame, "Start Download", app.start_download_thread,
                                 bg_color=app.accent_purple, fg_color="white",
                                 hover_color="#7c3aed", font=("Segoe UI", 11, "bold"),
                                 width=180, height=45, corner_radius=8)
    app.start_btn.pack(side=tk.LEFT, padx=(0, 10), fill="x", expand=True)
    
    # Stop Button (Destructive/Secondary)
    app.stop_btn = RoundedButton(frame, "Stop", app.stop_download,
                                bg_color=app.bg_dark, fg_color=app.accent_red,
                                hover_color=app.bg_input, font=("Segoe UI", 11, "bold"),
                                width=100, height=45, corner_radius=8, border_color=app.accent_red)
    app.stop_btn.pack(side=tk.LEFT)
    app.stop_btn.config_state("disabled")
    
    return frame


def create_token_dialog(app):
    """Create and show the token acquisition dialog."""
    app.log("Opening Suno in your default browser...", "info")
    webbrowser.open("https://suno.com")
    
    dialog = tk.Toplevel(app.winfo_toplevel())
    dialog.title("Get Token")
    dialog.geometry("600x450")
    dialog.configure(bg=app.bg_dark)
    dialog.transient(app.winfo_toplevel())
    dialog.grab_set()
    
    parent_window = app.winfo_toplevel()
    
    def close_dialog():
        """Properly close dialog and return focus to parent."""
        dialog.grab_release()
        parent_window.focus_set()
        dialog.destroy()
    
    dialog.protocol("WM_DELETE_WINDOW", close_dialog)
    
    tk.Label(dialog, text="INSTRUCTIONS", font=("Segoe UI", 14, "bold"),
            bg=app.bg_dark, fg=app.fg_primary).pack(pady=15)
    
    steps = (
        "1. Log in to Suno in the opened browser tab.\n"
        "2. Press F12 to open Developer Tools.\n"
        "3. Go to the 'Console' tab.\n"
        "4. Copy the code below and paste it, then press Enter."
    )
    tk.Label(dialog, text=steps, justify=tk.LEFT, font=("Segoe UI", 10),
            bg=app.bg_dark, fg=app.fg_primary).pack(pady=10, padx=20, anchor="w")
    
    code = "window.Clerk.session.getToken().then(t => prompt('Copy this token:', t))"
    
    code_container = tk.Frame(dialog, bg=app.bg_input, highlightbackground=app.border_subtle, highlightthickness=1)
    code_container.pack(fill="x", padx=20, pady=10)
    
    code_entry = tk.Entry(code_container, font=("Consolas", 10), fg=app.accent_purple,
                         bg=app.bg_input, relief="flat", bd=0, highlightthickness=0)
    code_entry.insert(0, code)
    code_entry.config(state="readonly")
    code_entry.pack(side=tk.LEFT, fill="x", expand=True, padx=10, pady=10)
    
    def copy_code():
        pyperclip.copy(code)
        btn_copy.set_text("Copied!")
        dialog.after(2000, lambda: btn_copy.set_text("Copy"))
    
    btn_copy = RoundedButton(code_container, "Copy", copy_code,
                            bg_color=app.accent_purple, fg_color="white",
                            hover_color="#9d6fff", font=("Segoe UI", 9, "bold"),
                            width=80, height=30, corner_radius=6)
    btn_copy.pack(side=tk.LEFT, padx=10)
    copy_code()
    
    tk.Label(dialog, text="5. Copy the token from the browser popup.\n6. Paste it below:",
            font=("Segoe UI", 10), bg=app.bg_dark, fg=app.fg_primary,
            justify=tk.LEFT).pack(pady=15, padx=20, anchor="w")
    
    token_container = tk.Frame(dialog, bg=app.bg_input, highlightbackground=app.border_subtle, highlightthickness=1)
    token_container.pack(fill="x", padx=20, pady=5)
    
    token_input = tk.Entry(token_container, bg=app.bg_input, fg=app.fg_primary,
                          insertbackground=app.fg_primary, relief="flat", bd=0, highlightthickness=0)
    token_input.pack(fill="x", padx=5, pady=5)
    token_input.focus_set()
    
    def submit():
        t = token_input.get().strip()
        if t:
            app.token_var.set(t)
            app.log("Token set successfully!", "success")
            app.save_config()
            close_dialog()
        else:
            messagebox.showwarning("Input Required", "Please paste the token.")
    
    submit_btn = RoundedButton(dialog, "Submit Token", submit,
                              bg_color=app.accent_purple, fg_color="white",
                              hover_color="#9d6fff", font=("Segoe UI", 11, "bold"),
                              width=200, height=45, corner_radius=8)
    submit_btn.pack(pady=15)
    
    app.winfo_toplevel().wait_window(dialog)
