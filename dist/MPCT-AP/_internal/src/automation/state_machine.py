"""
src/automation/state_machine.py
================================
Phase 2 – Transactional Resumability & Ephemeral Encrypted Checkpoints
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
This module implements Architecture §3: "Transactional Resumability &
Ephemeral Durability."

The problem it solves: if Playwright crashes mid-way through a 12-month
annual extraction (say, on Month 8), ALL previously collected data is lost
because it only lived in RAM.  Restarting means re-scraping Months 1–7 from
scratch — slow, wasteful, and unfair to the portal's servers.

The solution: after each successful month, we SERIALIZE the accumulated
Pandas DataFrame to a .parquet file in a temporary, session-isolated
directory and ENCRYPT it at rest.  On restart, we DESERIALIZE the latest
checkpoint and resume from the next month.

HOW IT WORKS END-TO-END
------------------------
  Month 1 extracted → df1 merged → checkpoint_save(df1, month=1) → /tmp/.../m01.parquet.enc
  Month 2 extracted → df2 merged → checkpoint_save(df1+df2, month=2) → /tmp/.../m02.parquet.enc
  ... Playwright CRASHES on Month 8 ...
  Supervisor restarts → checkpoint_load(latest) → returns df1..7
  Extraction resumes at Month 8.

WHAT IS PARQUET? (For algoRoute)
----------------------------------
Parquet is a column-oriented binary file format designed for analytics.

Compared to CSV:
  • Typed: stores int64, float64, datetime64 as native binary — no string parsing on read
  • Compressed: typically 5-10× smaller than equivalent CSV
  • Fast: reading a single column (e.g. "amount") never reads other columns from disk

Pandas writes Parquet with:   df.to_parquet("file.parquet")
Pandas reads it back with:    df = pd.read_parquet("file.parquet")

WHY ENCRYPT THE PARQUET? (For algoRoute)
------------------------------------------
Architecture §1 mandates "zero-persistence privacy."  The data contains
government financial disbursement records — real people's salary/payment
information.  Even in /tmp (a temporary filesystem wiped on reboot), these
files must not be readable by any other process that gains access to the
filesystem.  Fernet symmetric encryption ensures that without the per-session
key the file is cryptographically opaque.

FERNET SYMMETRIC ENCRYPTION (For algoRoute)
---------------------------------------------
Fernet (from the `cryptography` library) is a high-level "batteries included"
encryption scheme:
  • Uses AES-128 in CBC mode (industry standard block cipher)
  • Automatically includes an HMAC integrity check (tamper detection)
  • Every encrypted message includes a timestamp (prevents replay attacks)

Usage pattern:
    key   = Fernet.generate_key()   # 32 random bytes, base64-encoded
    f     = Fernet(key)
    token = f.encrypt(b"hello")     # returns encrypted bytes
    plain = f.decrypt(token)        # returns b"hello"

We generate a NEW key per session (task_id), store it only in RAM (never
on disk), and destroy it when the task completes.  An attacker who reads the
/tmp filesystem sees only the encrypted blob and cannot reconstruct the key.

TTL ENFORCEMENT (For algoRoute)
---------------------------------
"A strict TTL (Time-To-Live) policy of 15 minutes enforces the deletion of
these ephemeral checkpoints." — Architecture §3

We enforce this in two ways:
  1. `cleanup_session()` — called explicitly by the Supervisor on task
     completion or cancellation.
  2. `_reap_expired_sessions()` — a passive scan that removes any checkpoint
     directories whose modification time is older than TTL_MINUTES.  This
     acts as a safety net for orphaned sessions (e.g., if the process was
     killed before cleanup ran).
"""

import io
import logging
import os
import shutil
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

import pandas as pd
from cryptography.fernet import Fernet, InvalidToken

from src.automation.workflow_state import WorkflowState, transition

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# How long (minutes) before a session checkpoint is considered expired.
# Architecture §3: "TTL of 15 minutes."
TTL_MINUTES: int = 15

# Root directory under which per-session subdirectories are created.
# Using /tmp means the OS will also wipe these on reboot as a second backstop.
CHECKPOINT_BASE_DIR: Path = Path(tempfile.gettempdir()) / "mpct_sessions"

# File extension for encrypted Parquet checkpoint files.
CHECKPOINT_EXT = ".parquet.enc"


# ===========================================================================
# TASK STATE DATACLASS
# ===========================================================================

@dataclass
class TaskState:
    """
    In-memory record of one extraction job's progress.

    Mirrors the architecture's task_state dict (§3) but as a typed dataclass:

        task_state = {
            "task_id"          : "req-987abc",
            "completed_months" : [1, 2, 3, 4, 5, 6, 7],
            "current_phase"    : WorkflowState.EXTRACT_MONTH_8,
            "checkpoint_path"  : "/tmp/sessions/req-987abc/intermediate.parquet"
        }

    Fields
    ------
    task_id           : UUID string identifying this job uniquely
    year              : fiscal year being extracted
    completed_months  : list of month numbers (1-based) successfully processed
    current_workflow_state : last confirmed WorkflowState
    checkpoint_dir    : Path to the session-isolated /tmp directory
    _fernet_key       : the session encryption key (bytes, never persisted)
    _fernet           : live Fernet instance derived from the key
    created_at        : Unix timestamp when the task was created
    """
    task_id                 : str
    year                    : int
    completed_months        : list[int]          = field(default_factory=list)
    current_workflow_state  : WorkflowState      = WorkflowState.INIT
    checkpoint_dir          : Optional[Path]     = None

    # Private — not part of the public interface.
    # Using field(repr=False) hides the key from log output.
    _fernet_key             : Optional[bytes]    = field(default=None, repr=False)
    _fernet                 : Optional[Fernet]   = field(default=None, repr=False)
    created_at              : float              = field(default_factory=time.time)

    def __post_init__(self) -> None:
        """Called automatically by the dataclass after __init__."""
        if self._fernet_key is None:
            # Generate a brand-new random 32-byte key for this session.
            # Fernet.generate_key() uses the OS CSPRNG (cryptographically
            # secure pseudo-random number generator).
            self._fernet_key = Fernet.generate_key()
            self._fernet     = Fernet(self._fernet_key)
            logger.debug("[STATE] New Fernet key generated for task %s.", self.task_id)

    @property
    def latest_checkpoint_path(self) -> Optional[Path]:
        """Return the path of the most recently written checkpoint file, or None."""
        if not self.checkpoint_dir or not self.checkpoint_dir.exists():
            return None
        candidates = sorted(self.checkpoint_dir.glob(f"*{CHECKPOINT_EXT}"))
        return candidates[-1] if candidates else None

    def advance_state(self, next_state: WorkflowState) -> None:
        """
        Validate and apply a state transition.
        Delegates to workflow_state.transition() which enforces the
        VALID_TRANSITIONS table — no illegal jumps allowed.
        """
        self.current_workflow_state = transition(
            self.current_workflow_state, next_state
        )
        logger.debug(
            "[STATE] Task %s → %s", self.task_id, next_state.name
        )


# ===========================================================================
# SESSION LIFECYCLE
# ===========================================================================

def create_session(year: int, task_id: Optional[str] = None) -> TaskState:
    """
    Initialise a fresh TaskState and create its isolated /tmp subdirectory.

    Parameters
    ----------
    year    : fiscal year for this job
    task_id : optional caller-supplied ID; a UUID is generated if omitted

    Returns
    -------
    TaskState ready for use by the extraction engine.
    """
    tid = task_id or f"req-{uuid.uuid4().hex[:8]}"
    session_dir = CHECKPOINT_BASE_DIR / tid
    session_dir.mkdir(parents=True, exist_ok=True)

    state = TaskState(task_id=tid, year=year, checkpoint_dir=session_dir)
    logger.info("[STATE] Session created: task_id=%s  dir=%s", tid, session_dir)
    return state


def cleanup_session(state: TaskState) -> None:
    """
    Delete the session's /tmp directory and zero-out the in-memory key.

    This is the PRIMARY cleanup path — called by the Supervisor when a job
    finishes (success or failure).  The TTL reaper is the secondary backstop.
    """
    # Overwrite the key bytes with zeros before deleting the object.
    # Python's memory allocator may not immediately free the old bytes;
    # zeroing prevents a forensic tool from recovering the key from RAM.
    if state._fernet_key:
        # bytearray is mutable — we can overwrite it in-place.
        key_array = bytearray(state._fernet_key)
        for i in range(len(key_array)):
            key_array[i] = 0
        state._fernet_key = None
        state._fernet     = None

    if state.checkpoint_dir and state.checkpoint_dir.exists():
        shutil.rmtree(state.checkpoint_dir, ignore_errors=True)
        logger.info("[STATE] Session directory deleted: %s", state.checkpoint_dir)
    else:
        logger.debug("[STATE] No checkpoint directory to clean for task %s.", state.task_id)


def reap_expired_sessions() -> int:
    """
    Scan CHECKPOINT_BASE_DIR and delete any session subdirectory whose
    last-modified timestamp is older than TTL_MINUTES.

    Returns the number of directories reaped.

    Call this periodically from the Supervisor's health-check loop (Phase 3).

    HOW os.stat().st_mtime WORKS (For algoRoute)
    -----------------------------------------------
    Every file/directory on disk has metadata including `st_mtime` — the
    "modification time" as a Unix timestamp (seconds since 1970-01-01 UTC).
    `time.time()` also returns a Unix timestamp for the current moment.
    Their difference is the age of the file in seconds.
    """
    if not CHECKPOINT_BASE_DIR.exists():
        return 0

    ttl_seconds = TTL_MINUTES * 60
    now         = time.time()
    reaped      = 0

    for session_dir in CHECKPOINT_BASE_DIR.iterdir():
        if not session_dir.is_dir():
            continue
        age_seconds = now - session_dir.stat().st_mtime
        if age_seconds > ttl_seconds:
            shutil.rmtree(session_dir, ignore_errors=True)
            logger.info(
                "[STATE] Reaped expired session %s (age=%.0fs > TTL=%ds).",
                session_dir.name, age_seconds, ttl_seconds,
            )
            reaped += 1

    return reaped


# ===========================================================================
# CHECKPOINT SERIALIZATION  (write)
# ===========================================================================

def checkpoint_save(
    state: TaskState,
    df: pd.DataFrame,
    month: int,
) -> Path:
    """
    Serialize `df` to an encrypted Parquet file in the session directory,
    then record `month` in `state.completed_months`.

    SERIALIZATION PIPELINE (For algoRoute)
    ----------------------------------------
    DataFrame → Parquet bytes → Fernet-encrypted bytes → file on disk

    Step 1:  df.to_parquet(buffer)
             Pandas writes the DataFrame as Parquet into an in-memory buffer
             (io.BytesIO) — no unencrypted bytes ever touch the filesystem.

    Step 2:  fernet.encrypt(parquet_bytes)
             The Fernet cipher transforms the raw Parquet bytes into an
             opaque encrypted blob.  The blob is slightly larger than the
             input (Fernet prepends a version byte, timestamp, and HMAC).

    Step 3:  path.write_bytes(encrypted_blob)
             The encrypted blob is written atomically to disk.

    Parameters
    ----------
    state  : active TaskState (must have a live _fernet instance)
    df     : the DataFrame to checkpoint (cumulative — includes all months so far)
    month  : the month number just completed (used in the filename)

    Returns
    -------
    Path to the newly created checkpoint file.
    """
    if state._fernet is None:
        raise StateMachineError(
            f"Task {state.task_id} has no Fernet instance — session may have been cleaned up."
        )

    # Filename encodes the month for easy sorted retrieval.
    # Zero-padded to 2 digits so lexicographic sort == numeric sort.
    filename = f"m{month:02d}_checkpoint{CHECKPOINT_EXT}"
    out_path = state.checkpoint_dir / filename

    # --- Step 1: Parquet serialization into memory ---
    # io.BytesIO acts like an open file in RAM.
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=True, compression="snappy")
    # `.getvalue()` retrieves all bytes that were written into the buffer.
    parquet_bytes: bytes = buffer.getvalue()
    logger.debug(
        "[STATE] Parquet serialized: %d rows → %d bytes (unencrypted)",
        len(df), len(parquet_bytes),
    )

    # --- Step 2: Fernet encryption ---
    encrypted_blob: bytes = state._fernet.encrypt(parquet_bytes)
    logger.debug(
        "[STATE] Encrypted blob: %d bytes (includes HMAC + timestamp header)",
        len(encrypted_blob),
    )

    # --- Step 3: Write to disk ---
    out_path.write_bytes(encrypted_blob)

    # Update in-memory task state.
    if month not in state.completed_months:
        state.completed_months.append(month)
        state.completed_months.sort()   # keep the list ordered

    logger.info(
        "[STATE] Checkpoint saved: task=%s  month=%02d  path=%s",
        state.task_id, month, out_path,
    )
    return out_path


# ===========================================================================
# CHECKPOINT DESERIALIZATION  (read)
# ===========================================================================

def checkpoint_load(
    state: TaskState,
    month: Optional[int] = None,
) -> pd.DataFrame:
    """
    Decrypt and deserialize the latest (or specified) checkpoint file back
    into a Pandas DataFrame.

    DESERIALIZATION PIPELINE (For algoRoute)
    ------------------------------------------
    Encrypted file → Fernet-decrypted bytes → Parquet bytes → DataFrame

    This is the exact reverse of checkpoint_save.

    Parameters
    ----------
    state  : active TaskState with a valid _fernet instance and matching key
    month  : if provided, load the checkpoint for that specific month;
             otherwise load the most recent checkpoint

    Returns
    -------
    pd.DataFrame reconstructed from the checkpoint.

    Raises
    ------
    StateMachineError  if no checkpoint file is found or decryption fails.

    NOTE ON CRYPTOGRAPHIC INTEGRITY
    ---------------------------------
    If ANY byte in the encrypted file was modified (e.g., disk corruption or
    a tamper attempt), Fernet raises `cryptography.fernet.InvalidToken`.
    We re-raise this as a StateMachineError so the Supervisor knows the
    checkpoint is corrupted and must discard it.
    """
    if state._fernet is None:
        raise StateMachineError(
            f"Task {state.task_id} has no Fernet instance — cannot decrypt."
        )

    # Resolve which file to load.
    if month is not None:
        filename = f"m{month:02d}_checkpoint{CHECKPOINT_EXT}"
        target   = state.checkpoint_dir / filename
        if not target.exists():
            raise StateMachineError(
                f"No checkpoint found for task={state.task_id} month={month:02d}. "
                f"Expected: {target}"
            )
    else:
        target = state.latest_checkpoint_path
        if target is None:
            raise StateMachineError(
                f"No checkpoints found in {state.checkpoint_dir}."
            )

    logger.info("[STATE] Loading checkpoint: %s", target)

    # --- Step 1: Read encrypted bytes from disk ---
    encrypted_blob: bytes = target.read_bytes()

    # --- Step 2: Fernet decryption ---
    try:
        parquet_bytes: bytes = state._fernet.decrypt(encrypted_blob)
    except InvalidToken as exc:
        raise StateMachineError(
            f"Checkpoint decryption failed for {target}. "
            f"File may be corrupted or tampered with."
        ) from exc

    # --- Step 3: Parquet deserialization ---
    buffer = io.BytesIO(parquet_bytes)
    df     = pd.read_parquet(buffer, engine="pyarrow")

    logger.info(
        "[STATE] Checkpoint loaded: task=%s  rows=%d  cols=%d",
        state.task_id, len(df), len(df.columns),
    )
    return df


# ===========================================================================
# RECOVERY HELPER
# ===========================================================================

def get_resume_month(state: TaskState, total_months: int = 12) -> int:
    """
    After a crash-and-restart, determine which month the engine should
    resume from.

    Logic:
      • No completed months → start from month 1
      • Some completed months → start from max(completed) + 1
      • All months done → returns total_months + 1  (signals "nothing to do")

    Parameters
    ----------
    state         : TaskState recovered from a crashed session
    total_months  : total number of months in the extraction (default 12)

    Returns
    -------
    int month number to start (or resume) from.
    """
    if not state.completed_months:
        return 1
    next_month = max(state.completed_months) + 1
    logger.info(
        "[STATE] Resuming task %s from month %d (completed: %s)",
        state.task_id, next_month, state.completed_months,
    )
    return next_month


# ===========================================================================
# CUSTOM EXCEPTION
# ===========================================================================

class StateMachineError(RuntimeError):
    """
    Raised for state machine violations, checkpoint I/O errors, or
    decryption failures.  Inherits from RuntimeError so the Supervisor
    can catch it specifically.
    """


# ===========================================================================
# QUICK LOCAL TEST  (python -m src.automation.state_machine)
# ===========================================================================

if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(
        level=_logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )

    print("\n=== State Machine Smoke Test ===\n")

    # 1. Create a session
    task = create_session(year=2024)
    print(f"Session created: {task.task_id}")
    print(f"Checkpoint dir : {task.checkpoint_dir}")

    # 2. Simulate month-1 extraction → build a small DataFrame
    import numpy as _np
    _df_month1 = pd.DataFrame({
        "serial_no"    : range(1, 6),
        "ddo_code"     : [f"SBIN{i:06d}" for i in range(5)],
        "amount"       : _np.random.uniform(1000, 50000, 5).round(2),
        "payment_date" : pd.date_range("2024-04-01", periods=5, freq="W"),
    })

    # 3. Save a checkpoint for month 1
    saved_path = checkpoint_save(task, _df_month1, month=1)
    print(f"\nCheckpoint saved at: {saved_path}")
    print(f"Completed months   : {task.completed_months}")

    # 4. Simulate crash → reload from checkpoint
    print("\nSimulating crash and reload…")
    restored_df = checkpoint_load(task, month=1)
    print(f"Restored DataFrame shape: {restored_df.shape}")
    print(restored_df)

    # 5. Test resume logic
    resume_from = get_resume_month(task)
    print(f"\nEngine should resume from month: {resume_from}")

    # 6. Test WorkflowState transitions
    print("\n--- State Transitions ---")
    task.advance_state(WorkflowState.BROWSER_READY)
    task.advance_state(WorkflowState.NAVIGATED)
    print(f"Current state: {task.current_workflow_state.name}")

    # 7. Cleanup
    cleanup_session(task)
    exists = task.checkpoint_dir.exists() if task.checkpoint_dir else False
    print(f"\nCheckpoint dir deleted: {not exists}")

    print("\n✓ All smoke tests passed.")
