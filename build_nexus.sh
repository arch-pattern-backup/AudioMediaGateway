#!/bin/bash
# SunoSync-Nexus Build Script

echo "=== Starting SunoSync-Nexus Build Process ==="

# 1. Clean previous build artifacts
echo "[1/3] Cleaning old build files..."
rm -rf build/ dist/SunoSync-Nexus

# 2. Run PyInstaller
echo "[2/3] Compiling SunoSync-Nexus executable..."
pyinstaller --noconfirm SunoApi.spec

# 3. Verify Build
if [ -f "dist/SunoSync-Nexus" ]; then
    echo "=== [3/3] Build Successful! ==="
    echo "Executable is located at: dist/SunoSync-Nexus"
else
    echo "!!! ERROR: Build failed. Check logs above. !!!"
    exit 1
fi
