"""
start_server.py
===============
Definitive Windows-safe development server launcher for MPCT-AP.
Team: algoRoute | Project: MPCT-AP

WHY THIS FILE EXISTS
---------------------
The Windows `NotImplementedError` when launching Chromium via Playwright
comes from a race condition between Uvicorn and the asyncio event loop:

  1. On Windows, spawning a subprocess (like Chromium) requires the
     `ProactorEventLoop`.  The default on Python 3.8+ is `ProactorEventLoop`,
     BUT only when asyncio.run() is the entry point.

  2. Uvicorn's `--reload` flag starts a Watchdog child process.  That child
     imports the application module (src/main.py) and then starts its OWN
     event loop — often a `SelectorEventLoop` — BEFORE any application-level
     policy code can run.  At that point, the policy set in main.py is too
     late.

  3. The fix: this file runs FIRST, sets the policy BEFORE any event loop
     exists, then calls uvicorn.run() with reload=False.  The policy is
     guaranteed to be in effect for every subprocess Uvicorn or Playwright
     ever spawns.

HOW TO USE
----------
Development (no hot-reload, safe):
    python start_server.py

Development (with hot-reload via watchfiles, Windows-safe alternative):
    pip install watchfiles
    python start_server.py --reload

Production (Docker / Cloud Run) — use uvicorn directly in the Dockerfile:
    uvicorn src.main:app --host 0.0.0.0 --port 8080 --workers 1

DO NOT use `uvicorn src.main:app --reload` directly on Windows.
Use this launcher instead.
"""

import os
import sys

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Set the Proactor policy BEFORE anything else.
#
# This is the single most important line in this file.  It must run before
# any import that touches asyncio internals (including uvicorn itself).
# ─────────────────────────────────────────────────────────────────────────────
if sys.platform == "win32":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    print("[start_server] WindowsProactorEventLoopPolicy SET (before uvicorn import).")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Now it is safe to import uvicorn.
# ─────────────────────────────────────────────────────────────────────────────
import uvicorn  # noqa: E402  (import not at top — intentional, see above)


def main() -> None:
    reload_flag = "--reload" in sys.argv

    if reload_flag and sys.platform == "win32":
        # watchfiles-based reload is Windows-safe because it does NOT fork
        # a subprocess with its own event loop; it reloads in the same process.
        try:
            import watchfiles  # noqa: F401
            reload_impl = "watchfiles"
        except ImportError:
            print(
                "[start_server] WARNING: --reload requested but 'watchfiles' is not installed.\n"
                "  Install it with:  pip install watchfiles\n"
                "  Falling back to reload=False for safety.\n"
            )
            reload_flag = False
            reload_impl = None
    else:
        reload_impl = None

    config = dict(
        app       = "src.main:app",
        host      = os.getenv("HOST", "0.0.0.0"),
        port      = int(os.getenv("PORT", "8000")),
        reload    = reload_flag,
        workers   = 1,          # MUST be 1 — Playwright manages its own pool
        loop      = "asyncio",  # tell Uvicorn to use asyncio (Proactor on Win32)
        log_level = os.getenv("LOG_LEVEL", "info").lower(),
    )

    if reload_impl:
        config["reload_impl"] = reload_impl  # type: ignore[assignment]

    print(f"[start_server] Launching MPCT-AP  |  reload={reload_flag}  |  port={config['port']}")
    uvicorn.run(**config)


if __name__ == "__main__":
    main()
