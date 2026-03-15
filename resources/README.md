# SunoSync-Nexus Image Resources

This folder contains the graphical assets used by the application. Below are the supported formats, sizes, and their placement within the UI.

## Active Resources

### 1. Application Icon (`icon.ico`)
*   **Format**: `.ico` (Multi-size icon file)
*   **Optimal Sizes**: 16x16, 32x32, 48x48, 64x64, 128x128, 256x256 pixels.
*   **Placement**: 
    *   Displayed in the window title bar.
    *   Displayed in the Taskbar (Windows/Linux).
    *   Used as the executable icon during compilation (`SunoApi.spec`).
*   **Note**: For Linux systems, the app falls back to a generic icon if the `.ico` cannot be parsed by the window manager, but `main.py` attempts to load it via PIL.

### 2. Splash Screen (`splash.png`)
*   **Format**: `.png` (supports transparency)
*   **Optimal Size**: 800x500 pixels (or similar 16:10 / 4:3 ratio).
*   **Placement**: 
    *   Displayed as a centered overlay immediately upon application startup for 2 seconds.
*   **Behavior**: The application automatically scales this image to fit the startup window size while maintaining aspect ratio. A minimum width of 600px is recommended for legibility.

---

## Legacy/Optional Resources

### 3. Logo (`logo.png`)
*   **Format**: `.png`
*   **Current Usage**: Not actively referenced in the core UI (`main.py` or `suno_layout.py`).
*   **Recommended Use**: Can be used for custom documentation or future branding in the About dialog.

---

## Technical Notes
*   **Loading Mechanism**: All resources are loaded using the `resource_path()` helper in `main.py`, which ensures compatibility with PyInstaller's temporary extraction directory (`_MEIxxxx`).
*   **Transparency**: PNG files with alpha channels are supported and recommended for the splash screen to match the dark theme aesthetics.
