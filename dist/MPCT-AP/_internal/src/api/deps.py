"""
src/api/deps.py
================
FastAPI Dependency Injection — Rate Limiting, Idempotency, Payload Guard
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
FastAPI's dependency injection system (the `Depends()` mechanism) lets us
attach reusable "pre-flight checks" to any route without repeating code.

DEPENDENCY INJECTION FOR algoRoute
────────────────────────────────────
Instead of calling security checks inside every endpoint function, we
declare them as dependencies.  FastAPI resolves them automatically before
the route handler runs:

    @router.post("/extract")
    async def extract(
        request: Request,
        body: ExtractionRequest,
        _size: None = Depends(enforce_payload_size),          ← runs first
        _hmac: str   = Depends(resolve_idempotency_hmac),     ← runs second
        _rate: None  = Depends(rate_limit_check),             ← runs third
    ):
        ...   # route handler only runs if all Depends passed

If ANY dependency raises an HTTPException, FastAPI returns that error
response immediately — the route handler is never called.

Dependencies in this file:
  • enforce_payload_size       — rejects bodies > MAX_PAYLOAD_SIZE_BYTES (§5)
  • resolve_idempotency_hmac   — accepts any client UUID in X-Idempotency-Key
                                 and returns the SERVER-COMPUTED HMAC of the
                                 canonical payload for use as the internal
                                 deduplication key in the Supervisor queue.
  • get_supervisor             — returns the shared AutomationSupervisor instance

DESIGN RATIONALE — WHY THE CLIENT NO LONGER SENDS THE HMAC (For algoRoute)
────────────────────────────────────────────────────────────────────────────
The original design required the client to compute:
    hmac(SECRET_KEY, canonical_payload)
and send that as the X-Idempotency-Key header.

Problem: the SECRET_KEY must NEVER leave the server.  The React frontend
running in a browser has no access to the server's .env, so it was forced
to produce a fake placeholder that always failed HMAC validation → 401.

Corrected flow:
  1. Client sends a random UUID in X-Idempotency-Key (proves the request
     is not a copy-paste replay; does NOT carry the HMAC secret).
  2. Server receives the request, parses the body, and INDEPENDENTLY
     computes the HMAC from (IFSC | account | year | months) + SECRET_KEY.
  3. That server-computed HMAC becomes the internal deduplication key used
     when submitting the job to the Supervisor queue.
  4. Two requests for the exact same extraction (same IFSC, account, year,
     months) will produce the same server HMAC and can be deduplicated in
     the queue — even if their client UUIDs are different.

This preserves full idempotency semantics on the server without ever
exposing the HMAC secret to the frontend.
"""

import logging

from fastapi import Depends, Header, HTTPException, Request, status

from src.core.policies.security import MAX_PAYLOAD_SIZE_BYTES
from src.core.security import (
    CanonicalizeError,
    build_idempotency_key,
    canonicalize_account_number,
    canonicalize_ifsc,
    generate_idempotency_key,
)

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# PAYLOAD SIZE GUARD
# ──────────────────────────────────────────────────────────────────────────────

async def enforce_payload_size(request: Request) -> None:
    """
    Reject any request whose body exceeds MAX_PAYLOAD_SIZE_BYTES.

    WHY AT THE DEPENDENCY LEVEL AND NOT MIDDLEWARE? (For algoRoute)
    ──────────────────────────────────────────────────────────────────
    ASGI middleware runs for EVERY request (static files, health checks,
    docs).  A FastAPI dependency runs only for routes where it is declared,
    which is more precise and avoids false positives on routes that
    legitimately accept larger bodies (e.g. a file upload endpoint).

    `request.headers.get("content-length")` reads the Content-Length
    header the client sends.  If absent (chunked encoding), we fall through
    — the actual body size is checked later by Pydantic's model validator.
    """
    content_length = request.headers.get("content-length")
    if content_length:
        size = int(content_length)
        if size > MAX_PAYLOAD_SIZE_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=(
                    f"Request body exceeds the maximum allowed size of "
                    f"{MAX_PAYLOAD_SIZE_BYTES} bytes "
                    f"({MAX_PAYLOAD_SIZE_BYTES // 1024} KB)."
                ),
            )


# ──────────────────────────────────────────────────────────────────────────────
# IDEMPOTENCY KEY RESOLUTION
# ──────────────────────────────────────────────────────────────────────────────

async def resolve_idempotency_hmac(
    request: Request,
    x_idempotency_key: str = Header(
        ...,
        alias="X-Idempotency-Key",
        description=(
            "A client-generated UUID (crypto.randomUUID()) that uniquely "
            "identifies this request attempt.  The server does NOT validate "
            "this value cryptographically; it is used only as a client-side "
            "correlation handle.  The server independently computes the HMAC "
            "of the canonical payload and uses that as the internal "
            "deduplication key in the job queue."
        ),
    ),
) -> str:
    """
    Accept the client's idempotency UUID and return the SERVER-COMPUTED
    HMAC of the canonical payload.

    FLOW (For algoRoute)
    ────────────────────
    1. FastAPI enforces that X-Idempotency-Key is present (required header).
       Any non-empty string is accepted — no cryptographic check performed.
    2. We parse the request body to extract (ifsc, account, year, months).
    3. We call build_idempotency_key() — the same server-side HMAC function
       used in Phase 4 — on the canonical payload.
    4. We return the resulting 64-char HMAC hex digest.
    5. The route handler receives this as `_hmac` and tags the ExtractionJob
       with it, enabling the Supervisor to detect duplicate jobs.

    Client UUID vs Server HMAC:
    ─────────────────────────────────────────────────────────────────
    • x_idempotency_key  → client-generated UUID, accepted as-is, logged
                           for correlation / debugging.
    • return value       → server-computed HMAC, used for deduplication.

    This means two retries from the same browser tab will have DIFFERENT
    client UUIDs but produce the SAME server HMAC for the same payload,
    allowing the Supervisor to identify and coalesce them.

    Returns
    -------
    The server-computed 64-character HMAC-SHA256 hex digest.

    Raises
    ------
    HTTPException 400 if the request body is not valid JSON.
    HTTPException 422 if inputs fail canonicalization (invalid IFSC etc.).
    """
    logger.debug(
        "[DEPS] Received X-Idempotency-Key (client UUID): %s",
        x_idempotency_key,
    )

    # Parse the body to extract the fields we need to canonicalize.
    # FastAPI has already read and cached the body for Pydantic at this
    # point, so calling request.json() a second time is safe and free.
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Request body is not valid JSON.",
        )

    try:
        ifsc = body.get("ifsc_code", "")
        account_number = body.get("account_number", "")
        years_config = body.get("years_config")

        if years_config is not None:
            c_ifsc = canonicalize_ifsc(ifsc)
            c_account = canonicalize_account_number(account_number)
            year_parts: list[str] = []

            for item in years_config:
                year = int(item.get("year", 0))
                months = item.get("months", [])
                months_str = ",".join(str(m) for m in sorted(set(months)))
                year_parts.append(f"{year}:{months_str}")

            canonical_payload = (
                f"{c_ifsc}|{c_account}|"
                f"{';'.join(sorted(year_parts))}"
            )
            server_hmac = generate_idempotency_key(canonical_payload)
        else:
            year = body.get("year", 0)
            months = body.get("months", [])
            server_hmac = build_idempotency_key(
                ifsc=ifsc,
                account_number=account_number,
                year=year,
                months=months,
            )
    except CanonicalizeError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Input canonicalization failed: {exc}",
        )
    except (TypeError, ValueError, AttributeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid idempotency payload: {exc}",
        )

    logger.debug(
        "[DEPS] Server-computed deduplication HMAC: %s…  (client UUID: %s)",
        server_hmac[:12],
        x_idempotency_key,
    )

    return server_hmac


# ──────────────────────────────────────────────────────────────────────────────
# SUPERVISOR ACCESSOR
# ──────────────────────────────────────────────────────────────────────────────

def get_supervisor():
    """
    FastAPI dependency that returns the shared AutomationSupervisor instance.

    SINGLETON PATTERN IN FASTAPI (For algoRoute)
    ──────────────────────────────────────────────
    We import the module-level `supervisor` object from supervisor.py.
    Because Python caches module imports, every call to `get_supervisor()`
    returns the SAME object — there is exactly one Supervisor managing the
    one job queue.

    This is injected into route handlers via:
        supervisor = Depends(get_supervisor)
    """
    from src.automation.supervisor import supervisor as _supervisor
    return _supervisor
