import sys
import os
import multiprocessing
import threading
import time
import urllib.request
import logging
import socket
import webview
import uvicorn
from src.main import app

# Setup basic logging for desktop startup
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FROZEN EXE SAFETY: redirect stdout/stderr to devnull SAFELY.
# ---------------------------------------------------------------------------
# PyInstaller --windowed builds have no console, so any write to the real
# stdout/stderr file descriptors raises "Bad file descriptor" and can crash
# the process before it even reaches multiprocessing.freeze_support().
# We wrap this in try/except so a failure here never kills the launch.
if sys.platform == "win32" and getattr(sys, 'frozen', False):
    try:
        sys.stdout = open(os.devnull, 'w')
        sys.stderr = open(os.devnull, 'w')
    except Exception:
        pass

# 2. THE CURE: Isolate WebView2 so it doesn't fight Playwright
# By giving it a private folder, it gets its own secure GPU broker.
app_data = os.getenv('LOCALAPPDATA', os.path.expanduser('~'))
webview_data_dir = os.path.join(app_data, 'MPCT-AP', 'WebView2Data')
os.makedirs(webview_data_dir, exist_ok=True)
os.environ["WEBVIEW2_USER_DATA_FOLDER"] = webview_data_dir

def is_port_available(port=8000, host="127.0.0.1"):
    """Check if a port is available for binding (works around TIME_WAIT)."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # SO_REUSEADDR allows rebinding even in TIME_WAIT state (SO_REUSEPORT on newer systems)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.close()
        return True
    except OSError:
        return False


def run_server():
    """Runs the FastAPI/Uvicorn server in a background thread.

    KEY DECISIONS
    -------------
    - host="127.0.0.1"  : loopback only; the WebView2 window fetches from here.
    - loop="asyncio"    : forces the Proactor event loop on Windows (required
                          for Playwright's subprocess management).
    - log_level="critical" : completely silent startup.
    - access_log=False  : completely disable access logging for faster startup.
    - Port availability is checked in main thread BEFORE starting this thread.
    """
    try:
        uvicorn.run(
            app,
            host="127.0.0.1",
            port=8000,
            loop="asyncio",
            log_level="critical",
            access_log=False,
            server_header=False,
        )
    except Exception as e:
        logger.error(f"[SERVER] Backend crashed: {e}", exc_info=True)

if __name__ == '__main__':
    multiprocessing.freeze_support()

    # CRITICAL FIX: Check port availability in MAIN THREAD before starting server.
    # This handles TCP TIME_WAIT state gracefully without depending on thread
    # exception handling (which can silently fail).
    logger.info("[STARTUP] Checking if port 8000 is available...")
    max_port_wait = 30.0
    port_deadline = time.time() + max_port_wait
    port_available = False
    
    while time.time() < port_deadline:
        if is_port_available(8000):
            port_available = True
            logger.info("[STARTUP] Port 8000 is available. Starting backend thread.")
            break
        else:
            elapsed = time.time() - (port_deadline - max_port_wait)
            logger.warning(
                f"[STARTUP] Port 8000 still in use (TIME_WAIT). "
                f"Waiting... ({elapsed:.1f}s elapsed, trying again in 2s)"
            )
            time.sleep(2.0)
    
    if not port_available:
        logger.error(
            f"[STARTUP] Port 8000 is still in use after {max_port_wait} seconds. "
            f"Another process may be using it. Please check with: netstat -ano | findstr :8000"
        )
        import sys
        sys.exit(1)
    
    # Now that port is confirmed available, start the server thread.
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()

    # OPTIMIZATION: Use the fast /ping endpoint for startup checks.
    # If the server port is in TIME_WAIT, it may retry binding up to 5 times
    # (2 seconds apart), so we increase the deadline to 25 seconds to be safe.
    ping_endpoint = "http://127.0.0.1:8000/ping"
    # 15s total timeout for backend startup
    deadline = time.time() + 15.0
    server_ready = False
    
    logger.info("[STARTUP] Waiting for backend to respond to /ping...")
    while time.time() < deadline:
        try:
            # Aggressive polling: short timeout, fail fast if backend not ready
            response = urllib.request.urlopen(ping_endpoint, timeout=0.1)
            response.close()
            server_ready = True
            elapsed = time.time() - (deadline - 15.0)
            logger.info("[STARTUP] Backend is READY after %.1f seconds. Opening window.", elapsed)
            break
        except Exception:
            time.sleep(0.05)  # Poll every 50ms for faster response detection
    
    if not server_ready:
        elapsed = time.time() - (deadline - 25.0)
        logger.error(
            "[STARTUP] Backend failed to respond after %.1f seconds. "
            "Port may still be locked or backend crashed.", elapsed
        )
    else:
        logger.info("[STARTUP] Backend connection confirmed. Launching UI...")

    # Point PyWebView at the FastAPI server root.
    #
    # WHY http://127.0.0.1:8000/ INSTEAD OF A LOCAL FILE PATH
    # ---------------------------------------------------------
    # The previous code passed os.path.join(frontend_dir, "index.html") as
    # the URL together with http_server=True.  PyWebView's internal HTTP
    # server expects a *directory* path, not a file path, and WebView2
    # silently fails when given a filesystem path as a URL — producing the
    # fatal black screen.  Routing through FastAPI's FileResponse at GET /
    # (already defined in src/main.py) is the correct pattern: one HTTP
    # server, one port, no PyWebView internal server conflicts.
    #
    # WHY NO http_server=True
    # -----------------------
    # http_server=True spins up a second HTTP server inside PyWebView to
    # serve local files.  With WebView2, this second server competes for
    # the same GPU broker as the main WebView2 surface, which is the
    # DirectComposition conflict that killed the rendering surface.  Since
    # FastAPI already serves the frontend at GET /, we need zero internal
    # PyWebView servers.
    webview.create_window(
        title="MP-Cyber Treasury",
        url="http://127.0.0.1:8000/",
        width=1280,
        height=800,
        min_size=(960, 600),
    )

    # gui="edgechromium" explicitly selects the WebView2 (Edge/Chromium)
    # backend.  Without this, PyWebView may fall back to MSHTML (Trident /
    # IE11) on older Windows machines, which cannot render React + modern CSS.
    # NEVER pass http_server=True here — see the note above.
    webview.start(gui="edgechromium")