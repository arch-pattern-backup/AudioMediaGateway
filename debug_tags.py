from mutagen.wave import WAVE
import sys

path = "/tmp/check_metadata.wav"

try:
    audio = WAVE(path)
    print(f"--- Tags for {path} ---")
    if audio.tags:
        for key, value in audio.tags.items():
            print(f"Key: {key}")
            if key.startswith("USLT"):
                print("  Has Lyrics")
            if key.startswith("APIC"):
                print("  Has Cover Art")
    else:
        print("No tags found.")
except Exception as e:
    print(f"Error: {e}")
