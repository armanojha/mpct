import multiprocessing
import threading
import sys
import os
import time
import webview
import uvicorn
from src.main import app

def resource_path(relative_path):
    """Get absolute path to resource, works for dev and for PyInstaller."""
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


def run_server():
    """Runs the FastAPI server in a background thread."""
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")

if __name__ == '__main__':
    # CRITICAL: This is required for PyInstaller to handle multiprocessing on Windows
    multiprocessing.freeze_support()
    
    # Start the backend API
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # Wait for Uvicorn to fully bind to port 8000
    time.sleep(2)
    
    # Open the frontend in a native Windows GUI
    webview.create_window(
        title="MPCT-AP | Treasury Extraction Portal",
        url="http://127.0.0.1:8000",
        width=1280,
        height=800,
        min_size=(960, 600)
    )
    webview.start()
