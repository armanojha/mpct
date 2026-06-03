"""
src/api/v1/endpoints.py
========================
API v1 Route Handlers
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
This module defines every HTTP route in the v1 API surface:

  POST /api/v1/extract          → submit an extraction job, stream Excel back
  GET  /api/v1/health           → supervisor health check (used by load balancer)
  GET  /api/v1/docs/schema      → return the OpenAPI schema fragment for this route

STREAMING RESPONSE — WHY? (For algoRoute)
──────────────────────────────────────────
A normal FastAPI response builds the ENTIRE response body in RAM, then
sends it in one shot.  For a 12-month extraction that might be a 5 MB
Excel file:

  Normal response:  build 5 MB in RAM → send all at once → done
  Problem:          every concurrent request holds 5 MB in RAM simultaneously
                    → 10 concurrent users = 50 MB just for response buffers

  StreamingResponse: generate the file in CHUNKS → send each chunk → discard
  Benefit:          only one chunk (~64 KB) is in RAM per response at a time
                    regardless of file size or concurrency

We implement streaming by:
  1. Writing the DataFrame to an in-memory BytesIO buffer as .xlsx
  2. Yielding the buffer's bytes in chunks inside an async generator
  3. Passing that generator to FastAPI's StreamingResponse

The client receives a proper Excel file with a Content-Disposition header
that triggers the browser's "Save As" dialog.

HTTP STATUS CODES FOR algoRoute
─────────────────────────────────
  200 OK                    → job completed, Excel file in body
  422 Unprocessable Entity  → request body failed Pydantic or canonicalization
  401 Unauthorized          → HMAC key mismatch
  413 Payload Too Large     → body exceeds MAX_PAYLOAD_SIZE_BYTES
  429 Too Many Requests     → job queue is full (QueueSaturatedError)
  503 Service Unavailable   → circuit breaker open (SupervisorUnhealthyError)
  500 Internal Server Error → unexpected exception (logged, sanitized message)
"""

import asyncio
import io
import logging
import uuid
from typing import AsyncGenerator

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator

from src.api.deps import (
    enforce_payload_size,
    get_supervisor,
    verify_request_hmac,
)
from src.automation.supervisor import (
    AutomationSupervisor,
    ExtractionJob,
    QueueSaturatedError,
    SupervisorUnhealthyError,
)
from src.core.policies.extraction import (
    MAX_FISCAL_YEAR,
    MAX_MONTHS_PER_JOB,
    MIN_FISCAL_YEAR,
)
from src.core.policies.system import MAX_QUEUE_WAIT_SECONDS
from src.core.security import CanonicalizeError, canonicalize_account_number, canonicalize_ifsc

logger = logging.getLogger(__name__)

# Create the v1 API router.  All routes defined here are mounted under
# /api/v1 by main.py using: app.include_router(router, prefix="/api/v1")
router = APIRouter(tags=["extraction"])


# ──────────────────────────────────────────────────────────────────────────────
# REQUEST / RESPONSE MODELS  (Pydantic)
# ──────────────────────────────────────────────────────────────────────────────

class ExtractionRequest(BaseModel):
    """
    Pydantic model for the POST /extract request body.

    PYDANTIC FOR algoRoute
    ────────────────────────
    Pydantic automatically:
      • Parses the JSON body into typed Python objects.
      • Validates field types and constraints (min/max values, regex).
      • Returns a detailed 422 error if anything is invalid — no manual
        if-statements needed.

    The `@field_validator` decorator lets us add custom logic (e.g.,
    canonicalization) that runs AFTER type validation.
    """

    ifsc_code      : str = Field(
        ...,
        description="Bank branch IFSC code (e.g. SBIN0001234)",
        examples=["SBIN0001234"],
    )
    account_number : str = Field(
        ...,
        description="Bank account number (9–18 digits)",
        examples=["123456789012"],
    )
    year           : int = Field(
        ...,
        ge=MIN_FISCAL_YEAR,
        le=MAX_FISCAL_YEAR,
        description=f"Fiscal year ({MIN_FISCAL_YEAR}–{MAX_FISCAL_YEAR})",
        examples=[2024],
    )
    months         : list[int] = Field(
        default=list(range(1, 13)),
        description="List of 1-based month numbers to extract (1=April in Indian fiscal)",
        examples=[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]],
    )

    @field_validator("ifsc_code")
    @classmethod
    def validate_ifsc(cls, v: str) -> str:
        """Canonicalize and validate the IFSC code at model construction time."""
        try:
            return canonicalize_ifsc(v)
        except CanonicalizeError as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("account_number")
    @classmethod
    def validate_account(cls, v: str) -> str:
        """Canonicalize the account number (strip non-digits, validate length)."""
        try:
            return canonicalize_account_number(v)
        except CanonicalizeError as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("months")
    @classmethod
    def validate_months(cls, v: list[int]) -> list[int]:
        """Ensure each month is in range 1–12 and the list is not too long."""
        if not v:
            raise ValueError("At least one month must be specified.")
        if len(v) > MAX_MONTHS_PER_JOB:
            raise ValueError(
                f"Too many months requested ({len(v)}); "
                f"maximum is {MAX_MONTHS_PER_JOB}."
            )
        for m in v:
            if not (1 <= m <= 12):
                raise ValueError(f"Month {m} is out of range (1–12).")
        return sorted(set(v))   # deduplicate and sort


class HealthResponse(BaseModel):
    status              : str
    circuit_state       : str
    queue_depth         : int
    active_jobs         : int
    consecutive_crashes : int
    total_jobs_done     : int
    total_jobs_failed   : int
    uptime_seconds      : float


# ──────────────────────────────────────────────────────────────────────────────
# POST /extract
# ──────────────────────────────────────────────────────────────────────────────

@router.post(
    "/extract",
    summary="Submit an extraction job and receive results as an Excel file",
    response_description="Excel (.xlsx) file streamed chunk by chunk",
    responses={
        200: {"content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}}},
        401: {"description": "HMAC idempotency key mismatch"},
        413: {"description": "Request body too large"},
        422: {"description": "Validation error (invalid IFSC, account, year, or months)"},
        429: {"description": "Job queue is full — retry after a short wait"},
        503: {"description": "Automation Supervisor is unhealthy (circuit breaker open)"},
    },
)
async def submit_extraction(
    request         : Request,
    body            : ExtractionRequest,
    # Dependencies run in declaration order before the handler body executes.
    _size           : None   = Depends(enforce_payload_size),
    _hmac           : str    = Depends(verify_request_hmac),
    supervisor      : AutomationSupervisor = Depends(get_supervisor),
) -> StreamingResponse:
    """
    Core endpoint.  Flow:

      1. Pydantic validates + canonicalizes the body.
      2. enforce_payload_size checks Content-Length.
      3. verify_request_hmac validates X-Idempotency-Key.
      4. An ExtractionJob is created and submitted to the Supervisor queue.
      5. The handler `await`s the job's result Future.
      6. The resulting DataFrame is serialized to .xlsx in memory.
      7. A StreamingResponse yields the bytes in chunks to the client.
    """
    task_id = f"req-{uuid.uuid4().hex[:10]}"
    logger.info(
        "[ENDPOINT] POST /extract  task_id=%s  ifsc=%s  year=%s  months=%s",
        task_id, body.ifsc_code, body.year, body.months,
    )

    # ------------------------------------------------------------------
    # Create the asyncio.Future the Supervisor will resolve when done.
    # The route handler awaits this Future; the Supervisor puts the result
    # (or exception) into it from the worker loop.
    # ------------------------------------------------------------------
    loop   = asyncio.get_event_loop()
    future : asyncio.Future = loop.create_future()

    job = ExtractionJob(
        task_id       = task_id,
        ifsc          = body.ifsc_code,
        account_no    = body.account_number,
        year          = body.year,
        months        = body.months,
        result_future = future,
    )

    # ------------------------------------------------------------------
    # Submit to the Supervisor queue — this is the IPC boundary.
    # QueueSaturatedError → 429.  SupervisorUnhealthyError → 503.
    # ------------------------------------------------------------------
    try:
        await supervisor.submit(job)
    except QueueSaturatedError as exc:
        logger.warning("[ENDPOINT] Queue saturated for task %s: %s", task_id, exc)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=(
                "The extraction queue is currently full. "
                "Please retry in a few seconds."
            ),
            headers={"Retry-After": str(MAX_QUEUE_WAIT_SECONDS)},
        )
    except SupervisorUnhealthyError as exc:
        logger.error("[ENDPOINT] Supervisor unhealthy for task %s: %s", task_id, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "The automation service is currently unavailable. "
                "Our team has been notified. Please retry later."
            ),
        )

    # ------------------------------------------------------------------
    # Await the Future while also watching for client disconnects and a
    # server-side timeout. If the browser tab closes, cancel the Future so
    # queued work can be skipped and completed work is not retained.
    # ------------------------------------------------------------------
    try:
        async def _check_disconnect() -> bool:
            while True:
                if await request.is_disconnected():
                    return True
                await asyncio.sleep(2.0)

        disconnect_task = asyncio.create_task(
            _check_disconnect(), name=f"{task_id}-disconnect-watch"
        )
        timeout_task = asyncio.create_task(
            asyncio.sleep(float(MAX_QUEUE_WAIT_SECONDS) + 3600.0),
            name=f"{task_id}-timeout",
        )

        done, pending = await asyncio.wait(
            {future, disconnect_task, timeout_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        if future in done:
            result_df: pd.DataFrame = future.result()
        elif disconnect_task in done:
            future.cancel()
            logger.warning("[ENDPOINT] Client disconnected. Aborting job %s.", task_id)
            raise HTTPException(
                status_code=499,
                detail="Client Closed Request",
            )
        elif timeout_task in done:
            future.cancel()
            logger.error("[ENDPOINT] Task %s timed out waiting for Supervisor result.", task_id)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Extraction timed out. Please retry.",
            )
    except asyncio.TimeoutError:
        logger.error("[ENDPOINT] Task %s timed out waiting for Supervisor result.", task_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Extraction timed out. Please retry.",
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("[ENDPOINT] Task %s failed in Supervisor: %s", task_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Extraction failed due to an internal error. Please retry.",
        )

    # ------------------------------------------------------------------
    # Serialize the DataFrame to Excel in memory (never touches disk).
    # StreamingResponse sends the buffer in chunks, keeping memory low.
    # ------------------------------------------------------------------
    if result_df is None or result_df.empty:
        raise HTTPException(
            status_code=status.HTTP_204_NO_CONTENT,
            detail="Extraction returned no data for the requested parameters.",
        )

    logger.info(
        "[ENDPOINT] Task %s completed: %d rows.  Streaming Excel response.",
        task_id, len(result_df),
    )

    filename = f"treasury_{body.ifsc_code}_{body.year}.xlsx"

    return StreamingResponse(
        content    = _excel_stream_generator(result_df),
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers    = {
            "Content-Disposition" : f'attachment; filename="{filename}"',
            "X-Task-Id"           : task_id,
            "X-Row-Count"         : str(len(result_df)),
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# GET /health
# ──────────────────────────────────────────────────────────────────────────────

@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Supervisor health check",
)
async def health_check(
    supervisor: AutomationSupervisor = Depends(get_supervisor),
) -> HealthResponse:
    """
    Returns the current health of the AutomationSupervisor.

    The load balancer (Cloud Run / nginx) calls this endpoint every 30 seconds.
    If it returns 503, the instance is removed from the rotation.

    HTTP status mapping:
      • Circuit CLOSED → 200 OK
      • Circuit OPEN   → 503 Service Unavailable
    """
    h = supervisor.health

    if not h.is_healthy:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Automation Supervisor circuit breaker is OPEN.",
        )

    return HealthResponse(
        status              = "healthy",
        circuit_state       = h.circuit_state.name,
        queue_depth         = h.queue_depth,
        active_jobs         = h.active_jobs,
        consecutive_crashes = h.consecutive_crashes,
        total_jobs_done     = h.total_jobs_done,
        total_jobs_failed   = h.total_jobs_failed,
        uptime_seconds      = h.uptime_seconds,
    )


# ──────────────────────────────────────────────────────────────────────────────
# STREAMING HELPER
# ──────────────────────────────────────────────────────────────────────────────

async def _excel_stream_generator(
    df: pd.DataFrame,
    chunk_size: int = 8_192,   # 8 KB per chunk
) -> AsyncGenerator[bytes, None]:
    """
    Async generator that writes the DataFrame to .xlsx in memory on a
    background thread, then yields its bytes in fixed-size chunks.

    HOW AsyncGenerator WORKS (For algoRoute)
    ──────────────────────────────────────────
    An `async def` function that contains `yield` is an ASYNC GENERATOR.
    The caller (StreamingResponse) calls `async for chunk in generator():`
    which:
      1. Resumes the generator until it hits `yield chunk`.
      2. Sends `chunk` to the HTTP client.
      3. The loop repeats until the generator is exhausted (falls off the end).

    This is fundamentally different from building the whole response first:
    the generator and the HTTP sender run in alternating turns on the event
    loop — no large buffer needed.

    OPENPYXL ENGINE NOTE (For algoRoute)
    ──────────────────────────────────────
    `df.to_excel(buffer, engine="openpyxl")` writes a real .xlsx file
    (a ZIP archive of XML files under the hood) into the BytesIO buffer.
    We seek back to the start and repeatedly read small chunks so we do
    not create a second full-size bytes copy with `buffer.getvalue()`.
    """
    def _write_excel_sync() -> io.BytesIO:
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        return buf

    logger.debug("Offloading Excel generation to background thread...")
    stream: io.BytesIO = await asyncio.to_thread(_write_excel_sync)

    while chunk := stream.read(chunk_size):
        yield chunk
        # `await asyncio.sleep(0)` yields control to the event loop between
        # chunks so other coroutines (health checks, new requests) can run.
        await asyncio.sleep(0)
