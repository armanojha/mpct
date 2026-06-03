"""
src/api/deps.py
================
FastAPI Dependency Injection — Rate Limiting, HMAC Validation, Payload Guard
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
        _size: None = Depends(enforce_payload_size),   ← runs first
        _hmac: None = Depends(verify_request_hmac),    ← runs second
        _rate: None = Depends(rate_limit_check),       ← runs third
    ):
        ...   # route handler only runs if all Depends passed

If ANY dependency raises an HTTPException, FastAPI returns that error
response immediately — the route handler is never called.

Dependencies in this file:
  • enforce_payload_size  — rejects bodies > MAX_PAYLOAD_SIZE_BYTES (§5)
  • verify_request_hmac   — validates X-Idempotency-Key header (§5)
  • get_supervisor        — returns the shared AutomationSupervisor instance
"""

import logging

from fastapi import Depends, Header, HTTPException, Request, status

from src.core.feature_flags import ENABLE_HMAC_VALIDATION
from src.core.policies.security import MAX_PAYLOAD_SIZE_BYTES
from src.core.security import CanonicalizeError, verify_idempotency_key

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
# HMAC IDEMPOTENCY KEY VALIDATION
# ──────────────────────────────────────────────────────────────────────────────

async def verify_request_hmac(
    request: Request,
    x_idempotency_key: str = Header(
        ...,
        alias="X-Idempotency-Key",
        description=(
            "HMAC-SHA256 digest of the canonicalized request payload. "
            "Computed as: hmac(SECRET, 'IFSC|ACCOUNT|YEAR|MONTHS').hexdigest()"
        ),
    ),
) -> str:
    """
    Validate the X-Idempotency-Key header against a server-recomputed HMAC.

    IDEMPOTENCY SEMANTICS (For algoRoute)
    ────────────────────────────────────────
    Idempotency means: performing the same operation multiple times has
    the same effect as performing it once.  HTTP POST is not idempotent
    by default — if the client retries a failed POST, the server may run
    the job twice.

    Our idempotency key:
      1. The client computes: hmac(SECRET, canonical_payload)
      2. The client sends the key in every retry of the same request.
      3. The server recomputes the key from the body.
      4. If keys match → request is authentic and deduplicated.
      5. If keys differ → 401 Unauthorized (tampered or wrong secret).

    In Phase 4 we validate the key but leave deduplication (caching seen
    keys with a short TTL) for Phase 5 when Redis is introduced.

    Returns
    -------
    The validated idempotency key string (used by the route handler to tag
    the ExtractionJob).

    Raises
    ------
    HTTPException 401 if the key is invalid.
    HTTPException 422 if inputs fail canonicalization.
    """
    if not ENABLE_HMAC_VALIDATION:
        logger.debug("[DEPS] HMAC validation disabled by feature flag.")
        return x_idempotency_key

    # We need the parsed body to recompute the key.  FastAPI has already
    # read and cached the body at this point; `request.json()` is safe.
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Request body is not valid JSON.",
        )

    ifsc           = body.get("ifsc_code", "")
    account_number = body.get("account_number", "")
    year           = body.get("year", 0)
    months         = body.get("months", [])

    try:
        valid = verify_idempotency_key(
            ifsc=ifsc,
            account_number=account_number,
            year=year,
            months=months,
            provided_key=x_idempotency_key,
        )
    except CanonicalizeError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Input canonicalization failed: {exc}",
        )

    if not valid:
        logger.warning(
            "[DEPS] HMAC mismatch for request. Possible tampered payload or wrong secret."
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=(
                "X-Idempotency-Key does not match the server-recomputed HMAC. "
                "Ensure you are using the correct HMAC secret and canonical format."
            ),
        )

    logger.debug("[DEPS] HMAC validation passed.")
    return x_idempotency_key


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
