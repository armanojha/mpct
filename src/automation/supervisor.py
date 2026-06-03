"""
src/automation/supervisor.py
=============================
Phase 3 – IPC Isolation & The Automation Supervisor
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
This module is the operational core of the entire backend architecture.
Its single responsibility: be an always-running worker loop that sits
BETWEEN the fragile Playwright browser runtime and the stable FastAPI
gateway, ensuring a browser crash NEVER propagates upward into a 500 error.

Architecture §2 describes it as:
    "A dedicated process that sits above the Playwright runtime.
     Responsibilities: Restarts crashed Playwright instances, recycles
     the browser pool periodically to prevent memory leaks, detects hung
     contexts (zombies), and exposes internal health metrics back to the
     API runtime."

═══════════════════════════════════════════════════════════════
CONCEPTUAL OVERVIEW FOR algoRoute (first-semester students)
═══════════════════════════════════════════════════════════════

1. THE EVENT LOOP
─────────────────
Python's `asyncio` module runs everything on a single thread using a
construct called the EVENT LOOP.  Think of it as a very fast task
dispatcher that sits in an infinite loop asking: "Is any coroutine ready
to continue right now?"

Normal Python:                 asyncio Python:
  do_A()  ← blocks            await do_A()  ← yields control while waiting
  do_B()  ← waits for A       await do_B()  ← can run while A is waiting
  do_C()  ← waits for B       await do_C()  ← all three interleaved

This means we can have 10 concurrent browser tabs in-flight without 10
threads — the event loop interleaves them, switching between coroutines
whenever one hits an `await`.

2. asyncio.Queue  (THE INTER-LAYER COMMUNICATION BUS)
───────────────────────────────────────────────────────
An asyncio.Queue is a thread-safe, async-safe FIFO (First In, First Out)
data structure:

    Producer (FastAPI endpoint) → puts(job)  → Queue
    Consumer (Supervisor)       → gets(job)  ← Queue

The producer and consumer never call each other directly.  This is the
IPC (Inter-Process Communication) boundary described in Architecture §2.

Benefits:
  • FastAPI never touches Playwright — browser crashes are invisible to it.
  • The Queue naturally buffers bursts (10 requests arrive, processed 1 by 1).
  • `maxsize` sets a hard cap; a full queue returns 429 to the API (Phase 4).

3. THE CRASH-RECOVERY LOOP
────────────────────────────
    while True:
        try:
            result = await run_extraction(...)   ← Playwright runs here
            break                                ← success → exit retry loop
        except Exception:
            resume_month = get_resume_month(state)  ← read checkpoint
            # restart Playwright from resume_month

This is the fundamental resilience pattern.  The Supervisor wraps the
fragile Playwright call in a retry loop.  If Playwright crashes (raises any
exception), the loop catches it, reads the last saved Parquet checkpoint
to know which month was completed, and restarts — picking up exactly where
it left off.

4. SUPERVISOR HEALTH STATE (CIRCUIT BREAKER)
──────────────────────────────────────────────
A circuit breaker is an electrical analogy:
  CLOSED  → current flows normally     → requests processed normally
  OPEN    → circuit broken             → all requests immediately rejected (503)

We track consecutive crash counts.  If the browser crashes more than
MAX_CONSECUTIVE_CRASHES times in a row, the circuit breaker OPENS and the
Supervisor reports itself as unhealthy.  The API Gateway can then return 503
instead of queueing more work into a broken system.

5. THE REAPER BACKGROUND TASK
───────────────────────────────
asyncio.create_task() spawns a coroutine as a BACKGROUND task — it runs
concurrently on the same event loop without blocking the main worker loop.
We use this to run `reap_expired_sessions()` every REAP_INTERVAL_SECONDS
without requiring a separate thread or process.
"""

import asyncio
import logging
import time
import traceback
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

import pandas as pd

# Phase 1 modules
from src.automation.engine import (
    ExtractionError,
    extract_via_dom,
    extract_via_excel_stream,
    run_extraction,
)

# Phase 2 modules
from src.automation.state_machine import (
    StateMachineError,
    TaskState,
    checkpoint_load,
    checkpoint_save,
    cleanup_session,
    create_session,
    get_resume_month,
    reap_expired_sessions,
)
from src.automation.workflow_state import WorkflowState
from src.processing.heuristics import ConfidenceReport, evaluate_confidence, update_baseline
from src.processing.transformer import normalize

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────

# Maximum number of pending jobs allowed in the queue before the API returns 429.
# Setting this too high wastes RAM; too low causes spurious 429s under burst load.
MAX_QUEUE_SIZE: int = 50

# Maximum number of months in a single annual extraction job.
TOTAL_MONTHS: int = 12

# How many times the Supervisor will retry a single month before giving up
# and marking that month as permanently failed.
MAX_MONTH_RETRIES: int = 3

# After this many consecutive job-level crashes the circuit breaker opens.
MAX_CONSECUTIVE_CRASHES: int = 5

# Seconds between each TTL reaper scan of the /tmp checkpoint directory.
REAP_INTERVAL_SECONDS: int = 60

# Seconds to wait between crash-recovery retries.  Exponential backoff is
# applied: wait = RETRY_BACKOFF_BASE ** attempt_number  (1s, 2s, 4s …)
RETRY_BACKOFF_BASE: float = 2.0

# Maximum wait between retries (caps the exponential backoff).
MAX_RETRY_BACKOFF_SECONDS: float = 30.0

# How long (seconds) to wait for a Playwright extraction before declaring it hung.
EXTRACTION_TIMEOUT_SECONDS: float = 300.0   # 5 minutes per month


# ──────────────────────────────────────────────────────────────────────────────
# JOB DATACLASS  (the message that travels through the Queue)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ExtractionJob:
    """
    Represents a single unit of work submitted by the API Gateway.

    DATACLASS REMINDER (algoRoute): Python automatically generates __init__,
    __repr__, and __eq__ from the annotated fields below.

    Fields
    ------
    task_id         : unique job identifier (set by the API, echoed in responses)
    year            : fiscal year to extract
    months          : list of month numbers to extract (default: all 12)
    result_future   : an asyncio.Future the API awaits to receive the result.
                      When the Supervisor finishes, it puts the DataFrame (or
                      an exception) into this Future so the API endpoint can
                      stream it back to the client.
    submitted_at    : Unix timestamp when the job entered the queue
    """
    task_id       : str
    year          : int
    months        : list[int]              = field(default_factory=lambda: list(range(1, 13)))
    result_future : Optional[asyncio.Future] = field(default=None, repr=False)
    submitted_at  : float                  = field(default_factory=time.time)


# ──────────────────────────────────────────────────────────────────────────────
# SUPERVISOR HEALTH  (exposed to the API Gateway for 429 / 503 semantics)
# ──────────────────────────────────────────────────────────────────────────────

class CircuitState(Enum):
    """
    CLOSED → system healthy, jobs processed normally.
    OPEN   → too many consecutive crashes; reject all incoming jobs with 503.
    HALF_OPEN → a probe job is in-flight to test if the system recovered.
    """
    CLOSED    = auto()
    OPEN      = auto()
    HALF_OPEN = auto()


@dataclass
class SupervisorHealth:
    """
    Snapshot of the Supervisor's current operational state.
    The FastAPI gateway reads this object to decide 200 / 429 / 503.
    """
    circuit_state       : CircuitState = CircuitState.CLOSED
    queue_depth         : int          = 0
    active_jobs         : int          = 0
    consecutive_crashes : int          = 0
    total_jobs_done     : int          = 0
    total_jobs_failed   : int          = 0
    last_crash_at       : Optional[float] = None
    uptime_start        : float        = field(default_factory=time.time)

    @property
    def is_healthy(self) -> bool:
        return self.circuit_state == CircuitState.CLOSED

    @property
    def uptime_seconds(self) -> float:
        return time.time() - self.uptime_start


# ──────────────────────────────────────────────────────────────────────────────
# THE AUTOMATION SUPERVISOR CLASS
# ──────────────────────────────────────────────────────────────────────────────

class AutomationSupervisor:
    """
    Async worker that consumes ExtractionJob objects from a shared Queue,
    orchestrates Playwright extraction with crash recovery, integrates the
    heuristics engine, manages encrypted Parquet checkpoints, and exposes
    health metrics to the API layer.

    USAGE (from main.py / FastAPI lifespan):

        supervisor = AutomationSupervisor()
        await supervisor.start()            # starts the worker loop
        ...
        job = ExtractionJob(task_id="req-abc", year=2024)
        await supervisor.submit(job)        # enqueue from any coroutine
        result_df = await job.result_future # await the result
        ...
        await supervisor.stop()             # graceful shutdown
    """

    def __init__(self) -> None:
        # The asyncio.Queue is the IPC boundary between the API and this Supervisor.
        # maxsize enforces back-pressure: put() will block (or raise) if full.
        self._queue: asyncio.Queue[ExtractionJob] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)

        # Live health snapshot — read by the API without any locks needed because
        # asyncio is single-threaded (no concurrent mutation possible).
        self.health = SupervisorHealth()

        # Background task handles (asyncio.Task objects) kept alive during run.
        self._worker_task  : Optional[asyncio.Task] = None
        self._reaper_task  : Optional[asyncio.Task] = None

        # Shutdown signal — set by stop() to break the worker loop cleanly.
        self._stop_event   : asyncio.Event = asyncio.Event()

    # ──────────────────────────────────────────────────────────────────────
    # PUBLIC INTERFACE
    # ──────────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Spawn the worker loop and the TTL reaper as independent background tasks.

        asyncio.create_task() vs await
        ─────────────────────────────────
        `await coro()` runs the coroutine and blocks until it finishes.
        `asyncio.create_task(coro())` schedules the coroutine to run
        concurrently on the event loop and returns immediately.  The task
        runs in the background while other coroutines continue.

        We MUST keep a reference to the returned Task object; if it is
        garbage-collected, Python cancels the task silently.
        """
        logger.info("[SUPERVISOR] Starting Automation Supervisor…")
        self._stop_event.clear()
        self._worker_task = asyncio.create_task(
            self._worker_loop(), name="supervisor-worker"
        )
        self._reaper_task = asyncio.create_task(
            self._reaper_loop(), name="supervisor-reaper"
        )
        logger.info("[SUPERVISOR] Worker and reaper tasks created.")

    async def stop(self) -> None:
        """
        Signal the worker loop to stop and wait for it to drain gracefully.

        We set the stop event first (non-blocking) then `await` the tasks
        so we don't return until background work is truly finished.
        """
        logger.info("[SUPERVISOR] Shutdown requested.")
        self._stop_event.set()

        for task in (self._worker_task, self._reaper_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass   # expected — we cancelled it ourselves

        logger.info("[SUPERVISOR] Shutdown complete.")

    async def submit(self, job: ExtractionJob) -> None:
        """
        Enqueue a job from the API Gateway.

        This is a thin wrapper around Queue.put_nowait() that translates
        the QueueFull exception into meaningful HTTP semantic errors for
        the API layer.

        Queue.put_nowait() vs Queue.put()
        ───────────────────────────────────
        `put_nowait(item)` raises QueueFull immediately if the queue is at
        capacity — useful because the API endpoint must respond quickly.
        `put(item)` would block the caller until space is available — bad
        for a web endpoint that needs to return 429 promptly.

        Raises
        ------
        QueueSaturatedError   : queue is full (API should return 429)
        SupervisorUnhealthyError : circuit breaker is open (API should return 503)
        """
        if self.health.circuit_state == CircuitState.OPEN:
            raise SupervisorUnhealthyError(
                "Circuit breaker is OPEN: Chromium pool is unresponsive. "
                "Returning 503 to client."
            )

        try:
            self._queue.put_nowait(job)
            self.health.queue_depth = self._queue.qsize()
            logger.info(
                "[SUPERVISOR] Job enqueued: task_id=%s  queue_depth=%d",
                job.task_id, self.health.queue_depth,
            )
        except asyncio.QueueFull:
            raise QueueSaturatedError(
                f"Job queue is full ({MAX_QUEUE_SIZE} slots). "
                "Returning 429 to client."
            )

    # ──────────────────────────────────────────────────────────────────────
    # WORKER LOOP
    # ──────────────────────────────────────────────────────────────────────

    async def _worker_loop(self) -> None:
        """
        THE MAIN LOOP.  Runs forever until _stop_event is set.

        Pattern (simplified):
            while not stopping:
                job = await queue.get()   ← blocks until a job arrives
                await self._execute_job(job)
                queue.task_done()

        `await queue.get()` is a key asyncio pattern: this coroutine
        SUSPENDS (yields control back to the event loop) until an item
        arrives, costing ZERO CPU while idle.  Compare to a busy-wait:
            while queue.empty(): pass   ← wastes 100% of one CPU core!

        asyncio.wait_for adds a timeout so we never hang indefinitely on
        queue.get() when a stop is requested.
        """
        logger.info("[SUPERVISOR] Worker loop started.")

        while not self._stop_event.is_set():
            try:
                # Wait up to 1 second for a job; loop back to check stop_event.
                job: ExtractionJob = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                # No job arrived in 1s — loop and check stop_event again.
                continue
            except asyncio.CancelledError:
                logger.info("[SUPERVISOR] Worker loop cancelled.")
                break

            self.health.queue_depth = self._queue.qsize()
            self.health.active_jobs += 1

            try:
                await self._execute_job(job)
                # Reset crash counter on any success.
                self.health.consecutive_crashes = 0
                if self.health.circuit_state != CircuitState.CLOSED:
                    logger.info("[SUPERVISOR] Circuit breaker CLOSED (recovery confirmed).")
                    self.health.circuit_state = CircuitState.CLOSED
                self.health.total_jobs_done += 1

            except Exception as exc:
                # Something unrecoverable happened at the job level.
                self._handle_job_failure(job, exc)

            finally:
                self.health.active_jobs -= 1
                self._queue.task_done()

        logger.info("[SUPERVISOR] Worker loop exited.")

    # ──────────────────────────────────────────────────────────────────────
    # JOB EXECUTION  (crash-recovery loop around Playwright)
    # ──────────────────────────────────────────────────────────────────────

    async def _execute_job(self, job: ExtractionJob) -> None:
        """
        Orchestrate a complete multi-month extraction job with:
          • Per-month retry loop with exponential backoff
          • Heuristic confidence scoring after each month
          • Encrypted Parquet checkpointing after each successful month
          • Crash recovery: resume from last checkpoint on Playwright failure

        This is the heart of Architecture §3 (Transactional Resumability).

        OVERALL FLOW PER JOB
        ─────────────────────
        1. Create a TaskState (session + Fernet key)
        2. If a checkpoint exists, load it to get already-completed months
        3. For each pending month:
             a. Run DOM extraction (Phase 1 engine)
             b. Normalise rows → DataFrame (Phase 1 transformer)
             c. Score confidence (Phase 2 heuristics)
             d. If score < 0.90 → run stream fallback
             e. Merge month DataFrame into cumulative DataFrame
             f. Save encrypted Parquet checkpoint
        4. Deliver final cumulative DataFrame via job.result_future
        5. Cleanup session directory
        """
        logger.info(
            "[SUPERVISOR] Executing job: task_id=%s  year=%s  months=%s",
            job.task_id, job.year, job.months,
        )

        # Create a fresh session (new Fernet key, new /tmp dir).
        task_state: TaskState = create_session(year=job.year, task_id=job.task_id)

        # Cumulative DataFrame — grows as months are appended.
        cumulative_df: pd.DataFrame = pd.DataFrame()

        try:
            # Determine start month (may be > 1 if resuming after crash).
            start_month = get_resume_month(task_state, total_months=max(job.months))

            for month in job.months:
                if month < start_month:
                    # This month was already completed in a previous run.
                    logger.info(
                        "[SUPERVISOR] Skipping month %d (already in checkpoint).", month
                    )
                    continue

                month_df = await self._extract_month_with_retry(
                    task_state=task_state,
                    year=job.year,
                    month=month,
                )

                # Merge this month's data into the running total.
                cumulative_df = _append_month(cumulative_df, month_df)

                # Save checkpoint — cumulative so far (not just this month).
                if not cumulative_df.empty:
                    checkpoint_save(task_state, cumulative_df, month=month)
                    task_state.advance_state(WorkflowState.CHECKPOINT_SAVED)
                    # Reset state machine to NAVIGATED ready for the next month.
                    task_state.current_workflow_state = WorkflowState.NAVIGATED

            logger.info(
                "[SUPERVISOR] Job %s complete.  Total rows: %d",
                job.task_id, len(cumulative_df),
            )

            # Deliver result to the awaiting API endpoint via the Future.
            if job.result_future and not job.result_future.done():
                job.result_future.set_result(cumulative_df)

        except Exception as exc:
            logger.error(
                "[SUPERVISOR] Job %s failed: %s\n%s",
                job.task_id, exc, traceback.format_exc(),
            )
            if job.result_future and not job.result_future.done():
                job.result_future.set_exception(exc)
            raise   # re-raise so _worker_loop can update crash counters

        finally:
            # ALWAYS clean up the /tmp directory and zero the key,
            # even if an exception occurred.
            cleanup_session(task_state)

    async def _extract_month_with_retry(
        self,
        task_state: TaskState,
        year: int,
        month: int,
    ) -> pd.DataFrame:
        """
        Extract a single month, retrying up to MAX_MONTH_RETRIES times on
        Playwright failure with exponential backoff between attempts.

        EXPONENTIAL BACKOFF (For algoRoute)
        ────────────────────────────────────
        If a server or browser keeps failing, hammering it with rapid retries
        often makes things worse.  Exponential backoff increases the wait
        time after each failure:

            Attempt 1 fails → wait 2^1 =  2 seconds
            Attempt 2 fails → wait 2^2 =  4 seconds
            Attempt 3 fails → wait 2^3 =  8 seconds   (capped at 30s)

        This gives transient problems (slow network, portal hiccup) time to
        resolve before we try again.

        HEURISTICS INTEGRATION
        ─────────────────────────
        After a successful DOM extraction, we score the resulting DataFrame.
        If the score is below the 0.90 threshold, we discard the DOM result
        and run the stream (Excel binary) engine instead — same month, no retry
        needed because stream data is structurally more reliable.
        """
        last_exception: Optional[Exception] = None

        for attempt in range(1, MAX_MONTH_RETRIES + 1):
            try:
                logger.info(
                    "[SUPERVISOR] month=%02d  attempt=%d/%d",
                    month, attempt, MAX_MONTH_RETRIES,
                )

                # ── STEP A: Run DOM engine (timeout guard) ──
                task_state.advance_state(WorkflowState.BROWSER_READY)
                task_state.advance_state(WorkflowState.NAVIGATED)
                task_state.advance_state(WorkflowState.FORM_FILLED)
                task_state.advance_state(WorkflowState.SUBMIT_CLICKED)

                try:
                    dom_rows, _ = await asyncio.wait_for(
                        run_extraction(year=year, month=month, headless=True),
                        timeout=EXTRACTION_TIMEOUT_SECONDS,
                    )
                    task_state.advance_state(WorkflowState.RESULTS_LOADING)
                    task_state.advance_state(WorkflowState.SEARCH_RESULTS_READY)
                except asyncio.TimeoutError:
                    raise ExtractionError(
                        f"DOM extraction timed out after {EXTRACTION_TIMEOUT_SECONDS}s "
                        f"for month {month}."
                    )

                # ── STEP B: Normalise DOM rows ──
                dom_df = normalize(dom_rows, engine_tag="dom")

                # ── STEP C: Score confidence ──
                report: ConfidenceReport = evaluate_confidence(
                    dom_df, year=year, month=month
                )
                logger.info(
                    "[SUPERVISOR] Confidence score for month %02d: %.4f  (%s)",
                    month, report.score, "PASS" if report.passed else "FAIL",
                )

                if report.passed:
                    # DOM data is trustworthy — use it.
                    final_df = dom_df
                    update_baseline(year, month, len(final_df))
                    task_state.advance_state(WorkflowState.EXTRACTION_COMPLETE)

                else:
                    # ── STEP D: Heuristics failed → stream fallback ──
                    logger.warning(
                        "[SUPERVISOR] Confidence below threshold (%.4f < 0.90). "
                        "Switching to Excel stream engine for month %02d.",
                        report.score, month,
                    )
                    for warning in report.warnings:
                        logger.warning("[SUPERVISOR]   %s", warning)

                    task_state.advance_state(WorkflowState.EXPORT_CLICKED)

                    try:
                        stream_rows, _ = await asyncio.wait_for(
                            run_extraction(
                                year=year, month=month,
                                headless=True, prefer_stream=True,
                            ),
                            timeout=EXTRACTION_TIMEOUT_SECONDS,
                        )
                        task_state.advance_state(WorkflowState.EXPORT_AVAILABLE)
                    except asyncio.TimeoutError:
                        raise ExtractionError(
                            f"Stream extraction timed out after {EXTRACTION_TIMEOUT_SECONDS}s "
                            f"for month {month}."
                        )

                    final_df = normalize(stream_rows, engine_tag="stream")
                    update_baseline(year, month, len(final_df))
                    task_state.advance_state(WorkflowState.EXTRACTION_COMPLETE)

                logger.info(
                    "[SUPERVISOR] Month %02d extracted successfully via %s engine (%d rows).",
                    month, final_df[final_df.columns[-1]].iloc[0]
                    if not final_df.empty and "_extraction_engine" in final_df.columns
                    else "unknown",
                    len(final_df),
                )
                return final_df

            except (ExtractionError, StateMachineError, asyncio.TimeoutError) as exc:
                last_exception = exc
                logger.warning(
                    "[SUPERVISOR] Attempt %d/%d failed for month %02d: %s",
                    attempt, MAX_MONTH_RETRIES, month, exc,
                )

                # Reset state machine to INIT for the retry.
                task_state.current_workflow_state = WorkflowState.INIT

                if attempt < MAX_MONTH_RETRIES:
                    backoff = min(
                        RETRY_BACKOFF_BASE ** attempt,
                        MAX_RETRY_BACKOFF_SECONDS,
                    )
                    logger.info(
                        "[SUPERVISOR] Backing off %.1fs before retry…", backoff
                    )
                    # asyncio.sleep yields control to the event loop while
                    # waiting — does NOT block other jobs or coroutines.
                    await asyncio.sleep(backoff)

            except Exception as exc:
                # Unexpected exception (e.g. Playwright segfault propagated
                # through the async boundary) — do not swallow.
                last_exception = exc
                logger.error(
                    "[SUPERVISOR] Unexpected exception on month %02d attempt %d: %s\n%s",
                    month, attempt, exc, traceback.format_exc(),
                )
                task_state.current_workflow_state = WorkflowState.FAILED
                break

        # All retries exhausted.
        raise ExtractionError(
            f"Month {month} failed after {MAX_MONTH_RETRIES} attempts. "
            f"Last error: {last_exception}"
        ) from last_exception

    # ──────────────────────────────────────────────────────────────────────
    # CRASH HANDLING & CIRCUIT BREAKER
    # ──────────────────────────────────────────────────────────────────────

    def _handle_job_failure(self, job: ExtractionJob, exc: Exception) -> None:
        """
        Update health metrics after a job-level failure and open the circuit
        breaker if the crash threshold is exceeded.

        CIRCUIT BREAKER PATTERN (For algoRoute)
        ──────────────────────────────────────────
        Named after the physical device in an electrical panel, a software
        circuit breaker prevents "cascade failure" — the situation where a
        broken downstream service (here: Chromium) causes all callers to
        pile up waiting for it, consuming memory and threads until everything
        crashes.

        States:
          CLOSED   → normal. Requests flow through. Crash counter ticks up.
          OPEN     → breaker tripped. ALL new requests get 503 immediately.
                     No Playwright is attempted. Saves server resources.
          HALF_OPEN → after a cooldown, ONE probe request is allowed through.
                      If it succeeds → CLOSED. If it fails → back to OPEN.
        """
        self.health.consecutive_crashes += 1
        self.health.total_jobs_failed   += 1
        self.health.last_crash_at        = time.time()

        logger.error(
            "[SUPERVISOR] Job %s FAILED (consecutive crashes: %d): %s",
            job.task_id, self.health.consecutive_crashes, exc,
        )

        if self.health.consecutive_crashes >= MAX_CONSECUTIVE_CRASHES:
            if self.health.circuit_state == CircuitState.CLOSED:
                logger.critical(
                    "[SUPERVISOR] Circuit breaker OPENED after %d consecutive crashes. "
                    "All new jobs will receive 503 until the browser pool recovers.",
                    self.health.consecutive_crashes,
                )
                self.health.circuit_state = CircuitState.OPEN

        # Deliver the exception to the waiting API endpoint Future
        # so the HTTP response is not left hanging.
        if job.result_future and not job.result_future.done():
            job.result_future.set_exception(exc)

    # ──────────────────────────────────────────────────────────────────────
    # BACKGROUND TTL REAPER
    # ──────────────────────────────────────────────────────────────────────

    async def _reaper_loop(self) -> None:
        """
        Periodically scan the /tmp checkpoint directory and delete any
        session folder older than TTL_MINUTES (15 minutes per Architecture §3).

        This is the SECONDARY cleanup backstop.  Primary cleanup happens in
        _execute_job's `finally` block.  The reaper handles orphaned sessions
        left behind by processes that were killed (SIGKILL) before cleanup ran.

        WHY A BACKGROUND TASK AND NOT A CRON JOB? (For algoRoute)
        ─────────────────────────────────────────────────────────────
        In a cloud container environment (Phase 5: Cloud Run), we can't rely
        on OS-level cron.  Spawning a background asyncio task is portable,
        requires no extra infrastructure, and lives and dies with the Python
        process cleanly.

        asyncio.sleep(REAP_INTERVAL_SECONDS) suspends the coroutine without
        blocking anything else — the event loop handles other work while this
        task sleeps.
        """
        logger.info("[SUPERVISOR] Reaper loop started (interval=%ds).", REAP_INTERVAL_SECONDS)

        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(REAP_INTERVAL_SECONDS)
                reaped = reap_expired_sessions()
                if reaped:
                    logger.info("[SUPERVISOR] Reaper removed %d expired session(s).", reaped)
            except asyncio.CancelledError:
                logger.info("[SUPERVISOR] Reaper loop cancelled.")
                break
            except Exception as exc:
                # Reaper errors must NEVER crash the supervisor — log and continue.
                logger.error("[SUPERVISOR] Reaper encountered an error: %s", exc)

        logger.info("[SUPERVISOR] Reaper loop exited.")


# ──────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────

def _append_month(cumulative: pd.DataFrame, monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Vertically concatenate a new monthly DataFrame onto the cumulative one.

    pd.concat with axis=0 stacks rows (like stacking spreadsheet rows):

        cumulative  (shape N × C)
        monthly     (shape M × C)
        ─────────────────────────
        result      (shape N+M × C)

    ignore_index=True renumbers rows 0…(N+M-1) cleanly.
    """
    if cumulative.empty:
        return monthly.copy()
    if monthly.empty:
        return cumulative

    result = pd.concat([cumulative, monthly], axis=0, ignore_index=True)
    return result


# ──────────────────────────────────────────────────────────────────────────────
# CUSTOM EXCEPTIONS
# ──────────────────────────────────────────────────────────────────────────────

class QueueSaturatedError(RuntimeError):
    """
    Raised by submit() when the job queue is full.
    The API Gateway translates this to HTTP 429 Too Many Requests.
    """


class SupervisorUnhealthyError(RuntimeError):
    """
    Raised by submit() when the circuit breaker is OPEN.
    The API Gateway translates this to HTTP 503 Service Unavailable.
    """


# ──────────────────────────────────────────────────────────────────────────────
# MODULE-LEVEL SINGLETON  (imported by main.py and api/deps.py in Phase 4)
# ──────────────────────────────────────────────────────────────────────────────

# A single shared Supervisor instance.  FastAPI's lifespan hook will call
# supervisor.start() on startup and supervisor.stop() on shutdown.
supervisor = AutomationSupervisor()


# ──────────────────────────────────────────────────────────────────────────────
# QUICK LOCAL TEST  (python -m src.automation.supervisor)
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import logging as _logging
    import uuid as _uuid

    _logging.basicConfig(
        level=_logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )

    async def _smoke_test():
        print("\n=== Supervisor Smoke Test ===\n")

        sup = AutomationSupervisor()
        await sup.start()

        loop = asyncio.get_event_loop()
        fut  = loop.create_future()

        job = ExtractionJob(
            task_id       = f"smoke-{_uuid.uuid4().hex[:6]}",
            year          = 2024,
            months        = [1, 2],    # only 2 months for speed in smoke test
            result_future = fut,
        )

        await sup.submit(job)
        print(f"Job submitted: {job.task_id}")
        print(f"Queue depth  : {sup.health.queue_depth}")

        try:
            result_df = await asyncio.wait_for(fut, timeout=600.0)
            print(f"\n✓ Result DataFrame shape: {result_df.shape}")
        except Exception as exc:
            print(f"\n✗ Job failed (expected in offline test): {exc}")

        print(f"\nHealth: {sup.health}")
        await sup.stop()
        print("\n✓ Supervisor shut down cleanly.")

    asyncio.run(_smoke_test())
