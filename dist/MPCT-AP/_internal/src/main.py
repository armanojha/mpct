"""
src/main.py
============
Application Bootstrap & FastAPI Lifecycle Management
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
This is the entry point for the entire MPCT-AP backend.  It:
  1. Creates the FastAPI application instance.
  2. Registers the ASGI middleware stack (payload size enforcer, CORS).
  3. Defines the lifespan context manager that starts and stops the
     AutomationSupervisor cleanly on boot and shutdown.
  4. Mounts the v1 API router at /api/v1.
  5. Provides a root GET / health probe for the load balancer.

To run locally:
    uvicorn src.main:app --reload --port 8000

To run in production (Phase 5 Docker):
    uvicorn src.main:app --host 0.0.0.0 --port 8080 --workers 1

WHY --workers 1? (For algoRoute)
──────────────────────────────────
Playwright manages Chromium processes internally.  Running multiple Uvicorn
worker processes would spawn multiple independent Supervisors and browser
pools, multiplying RAM usage unpredictably.  We run ONE worker process and
rely on asyncio concurrency within that process for throughput.  Horizontal
scaling (multiple container instances) is handled by Cloud Run in Phase 5.

FASTAPI LIFESPAN (For algoRoute)
──────────────────────────────────
FastAPI's `lifespan` parameter accepts an async context manager that runs:
  • BEFORE the app starts accepting requests: `async with lifespan(app):` → yield
  • AFTER the app stops: the code after `yield` runs

This is where we start the Supervisor (so it is ready before request 1
arrives) and stop it cleanly (so in-flight jobs are flushed before the
process exits).

Think of it as:  __init__ and __del__ for the entire web application.
"""

import logging
import os
import sys
import asyncio
from contextlib import asynccontextmanager

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles


def resource_path(relative_path):
    """Get absolute path to resource, works for dev and for PyInstaller."""
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

from src.api.v1.endpoints import router as v1_router
from src.automation.supervisor import supervisor
from src.core.feature_flags import ENABLE_RATE_LIMITING
from src.core.policies.security import MAX_PAYLOAD_SIZE_BYTES

# ---------------------------------------------------------------------------
# LOGGING CONFIGURATION
# ---------------------------------------------------------------------------
# Configure the root logger once here.  All module-level loggers created with
# `logging.getLogger(__name__)` inherit this configuration automatically.
# In production, replace StreamHandler with a structured JSON handler
# (e.g. python-json-logger) for ingestion by Cloud Logging.
logging.basicConfig(
    level   = logging.getLevelName(os.getenv("LOG_LEVEL", "INFO")),
    format  = "%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt = "%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# GLOBAL STATE FOR BACKGROUND SUPERVISOR STARTUP
# ──────────────────────────────────────────────────────────────────────────────

supervisor_task = None
supervisor_ready = asyncio.Event()  # Set when supervisor.start() completes


async def _supervisor_startup_background():
    """Runs supervisor.start() in background; allows /ping to respond immediately."""
    global supervisor_task
    try:
        logger.info("[SUPERVISOR] Starting in background (non-blocking)...")
        await supervisor.start()
        logger.info("[SUPERVISOR] Started and ready to accept jobs.")
        supervisor_ready.set()
    except Exception as e:
        logger.error(f"[SUPERVISOR] Background startup failed: {e}")
        raise


# ──────────────────────────────────────────────────────────────────────────────
# LIFESPAN CONTEXT MANAGER
# ──────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Runs setup code BEFORE the app accepts connections, and teardown code
    AFTER it stops.

    CRITICAL CHANGE: Supervisor now starts in BACKGROUND, NOT blocking the
    app startup. This allows /ping to respond immediately (~1 second) instead
    of waiting 14 seconds for Supervisor initialization.

    API endpoints that need Supervisor (e.g. /api/v1/extract) will wait for
    supervisor_ready event before processing requests.

    ASYNC CONTEXT MANAGER PATTERN (For algoRoute)
    ───────────────────────────────────────────────
    `@asynccontextmanager` turns an `async def` generator function into an
    object usable with `async with`.

    The code BEFORE `yield` is the __enter__ phase (startup).
    The code AFTER  `yield` is the __exit__ phase (shutdown).
    The `yield` itself is where FastAPI serves requests.

    WINDOWS PROACTOR FIX (For algoRoute)
    ──────────────────────────────────────
    We re-assert the ProactorEventLoop policy HERE, inside the already-running
    event loop, as a final guard.  Uvicorn (especially with --reload) uses a
    watchdog that re-imports application modules in a child process; the policy
    set at module top-level may not have been applied before Uvicorn started its
    own loop.  Calling set_event_loop_policy() inside the lifespan guarantees
    that any subprocess Playwright spawns from this point forward (i.e. Chromium)
    will inherit a Proactor-compatible environment.
    """
    global supervisor_task
    
    # ── STARTUP ──────────────────────────────────────────────────────────
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        logger.info("[LIFESPAN] WindowsProactorEventLoopPolicy enforced inside lifespan.")

    logger.info("=== MPCT-AP API Gateway starting up ===")
    logger.info("MAX_PAYLOAD_SIZE: %d bytes", MAX_PAYLOAD_SIZE_BYTES)

    # Start Supervisor in BACKGROUND (doesn't block /ping response)
    supervisor_task = asyncio.create_task(_supervisor_startup_background())

    # Hand control to FastAPI — requests are accepted from here until shutdown.
    # /ping responds immediately without waiting for supervisor_ready
    yield

    # ── SHUTDOWN ─────────────────────────────────────────────────────────
    logger.info("=== MPCT-AP API Gateway shutting down ===")
    
    # Wait for supervisor background task to complete if it's still running
    if supervisor_task and not supervisor_task.done():
        try:
            await asyncio.wait_for(supervisor_task, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("[SHUTDOWN] Supervisor startup didn't complete in time.")
    
    await supervisor.stop()
    logger.info("AutomationSupervisor stopped cleanly.")


# ──────────────────────────────────────────────────────────────────────────────
# APPLICATION FACTORY
# ──────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "MPCT-AP API Gateway",
    description = (
        "MP Cyber Treasury Automated Pipeline — secure extraction of "
        "financial disbursement records from the Madhya Pradesh State Treasury."
    ),
    version     = "1.0.0",
    lifespan    = lifespan,
    # Disable the default /docs and /redoc in production; enable for dev.
    docs_url    = "/docs"  if os.getenv("ENABLE_SWAGGER_UI", "true") == "true" else None,
    redoc_url   = "/redoc" if os.getenv("ENABLE_SWAGGER_UI", "true") == "true" else None,
)


# ──────────────────────────────────────────────────────────────────────────────
# MIDDLEWARE
# ──────────────────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)


# PAYLOAD SIZE ENFORCEMENT at the ASGI layer (second line of defence after
# the FastAPI dependency in deps.py — belt-and-suspenders).
@app.middleware("http")
async def enforce_max_body_size(request: Request, call_next):
    """
    Reject requests whose Content-Length header exceeds the limit BEFORE
    FastAPI even tries to parse the body.  This protects against clients
    that send a correct Content-Length header.

    Note: clients sending chunked Transfer-Encoding without Content-Length
    are handled by the enforce_payload_size dependency in deps.py.
    """
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_PAYLOAD_SIZE_BYTES:
        return JSONResponse(
            status_code = status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            content     = {
                "detail": (
                    f"Request body exceeds {MAX_PAYLOAD_SIZE_BYTES // 1024} KB limit."
                )
            },
        )
    return await call_next(request)


# ──────────────────────────────────────────────────────────────────────────────
# GLOBAL EXCEPTION HANDLER
# ──────────────────────────────────────────────────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Catch-all handler for any unhandled exception that escapes route handlers.

    NEVER expose internal stack traces to clients in production — they leak
    implementation details that attackers can exploit.  We log the full
    traceback server-side and return a sanitized generic message.
    """
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
        content     = {
            "detail": "An internal server error occurred. Please retry later."
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES
# ──────────────────────────────────────────────────────────────────────────────

# Mount the v1 router.  All routes in endpoints.py are prefixed with /api/v1.
app.include_router(v1_router, prefix="/api/v1")


# ---------------------------------------------------------------------------
# STATIC FILES  –  serve the entire frontend/ directory over HTTP
# ---------------------------------------------------------------------------
# WHY THIS IS REQUIRED
# --------------------
# index.html loads vendor JS via relative paths (./vendor/react.development.js
# etc.).  Without a StaticFiles mount, FastAPI only knows about GET / and has
# no handler for GET /vendor/*.js  →  404  →  React never loads  →  black screen.
#
# Mount order matters: the router (api/v1) is mounted first, then the catch-all
# static mount at "/frontend".  We keep GET / as an explicit route so the root
# URL always resolves to index.html even if the static mount is reordered.
#
# check_dir=False is safe here — resource_path() already points into _MEIPASS
# when frozen, and the PyInstaller --add-data flag guarantees the directory
# exists; we just don't want a crash if the path is evaluated before _MEIPASS
# is fully extracted.
frontend_dir = resource_path("frontend")
if os.path.isdir(frontend_dir):
    app.mount("/frontend", StaticFiles(directory=frontend_dir), name="frontend_static")
    # Also mount vendor at the root /vendor path so ./vendor/ relative links resolve.
    vendor_dir = os.path.join(frontend_dir, "vendor")
    if os.path.isdir(vendor_dir):
        app.mount("/vendor", StaticFiles(directory=vendor_dir), name="vendor_static")


@app.get("/", include_in_schema=False)
async def serve_ui():
    """Serves the React frontend to the PyWebView desktop window."""
    html_path = resource_path(os.path.join("frontend", "index.html"))
    return FileResponse(html_path)


@app.get("/ping", include_in_schema=False)
async def ping():
    """Ultra-fast ping endpoint for startup health checks (before Supervisor is ready)."""
    return {"status": "ok"}


# ──────────────────────────────────────────────────────────────────────────────
# LOCAL DEVELOPMENT ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    # CRITICAL (Windows): The ProactorEventLoop policy MUST be set before
    # uvicorn.run() creates any event loop.  This __main__ block runs in the
    # main process, so setting it here guarantees the correct loop type is
    # active for the lifetime of the server.
    #
    # reload=False is MANDATORY on Windows.  The Uvicorn reloader forks a
    # watchdog child process that creates its own SelectorEventLoop BEFORE
    # our policy line runs, which is exactly the race condition that caused
    # the original NotImplementedError.  Use the start_server.py launcher
    # (or set UVICORN_RELOAD=false) for development hot-reloading instead.
    uvicorn.run(
        "src.main:app",
        host      = "0.0.0.0",
        port      = int(os.getenv("PORT", "8000")),
        reload    = False,   # MUST be False on Windows — see comment above
        workers   = 1,       # MUST be 1 — see module docstring
        loop      = "asyncio",  # explicitly select asyncio (Proactor) loop
        log_level = os.getenv("LOG_LEVEL", "info").lower(),
    )
