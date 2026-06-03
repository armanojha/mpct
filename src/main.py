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
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

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
# LIFESPAN CONTEXT MANAGER
# ──────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Runs setup code BEFORE the app accepts connections, and teardown code
    AFTER it stops.

    ASYNC CONTEXT MANAGER PATTERN (For algoRoute)
    ───────────────────────────────────────────────
    `@asynccontextmanager` turns an `async def` generator function into an
    object usable with `async with`.

    The code BEFORE `yield` is the __enter__ phase (startup).
    The code AFTER  `yield` is the __exit__ phase (shutdown).
    The `yield` itself is where FastAPI serves requests.
    """
    # ── STARTUP ──────────────────────────────────────────────────────────
    logger.info("=== MPCT-AP API Gateway starting up ===")
    logger.info("MAX_PAYLOAD_SIZE: %d bytes", MAX_PAYLOAD_SIZE_BYTES)

    await supervisor.start()
    logger.info("AutomationSupervisor started and ready to accept jobs.")

    # Hand control to FastAPI — requests are accepted from here until shutdown.
    yield

    # ── SHUTDOWN ─────────────────────────────────────────────────────────
    logger.info("=== MPCT-AP API Gateway shutting down ===")
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

# CORS — allow the React SPA to call this API from its origin.
# In production, replace "*" with the actual frontend domain.
_allowed_origins = os.getenv("CORS_ALLOWED_ORIGINS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins     = _allowed_origins,
    allow_credentials = True,
    allow_methods     = ["POST", "GET"],
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


@app.get("/", include_in_schema=False)
async def root_health_probe():
    """
    Minimal root probe for Cloud Run / load balancer health checks.
    Returns 200 immediately without touching the Supervisor.
    """
    return {"status": "ok", "service": "MPCT-AP API Gateway"}


# ──────────────────────────────────────────────────────────────────────────────
# LOCAL DEVELOPMENT ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.main:app",
        host    = "0.0.0.0",
        port    = int(os.getenv("PORT", "8000")),
        reload  = True,   # auto-reload on file changes (dev only)
        workers = 1,      # MUST be 1 — see docstring above
        log_level = os.getenv("LOG_LEVEL", "info").lower(),
    )
