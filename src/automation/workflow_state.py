"""
src/automation/workflow_state.py
=================================
Phase 2 – Workflow State Enumeration
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
This module defines the WorkflowState Enum – a fixed set of named constants
that describe every meaningful "checkpoint" the browser automation can be in
at any given moment during a single extraction job.

WHAT IS A STATE MACHINE? (For algoRoute first-semester students)
-----------------------------------------------------------------
A state machine is one of the most fundamental concepts in computer science.
Think of a traffic light:

    RED → GREEN → YELLOW → RED → …

At any moment the light is in exactly ONE state.  Rules (transitions) define
which states can follow which:
  - GREEN can only go to YELLOW (never directly to RED)
  - YELLOW can only go to RED

A state machine applied to our portal automation works the same way:

    INIT → NAVIGATED → FORM_FILLED → SUBMIT_CLICKED
         → RESULTS_LOADING → SEARCH_RESULTS_READY
         → EXPORT_CLICKED → EXPORT_AVAILABLE
         → EXTRACTION_COMPLETE

At each step, the engine ASSERTS it is in the expected state before
proceeding.  If it finds itself in RESULTS_LOADING when it expected
SEARCH_RESULTS_READY, it knows something went wrong (slow network, portal
error page, unexpected redirect) and can react intelligently rather than
blindly pressing forward.

This is exactly what Architecture §4 means by:
    "assert current_state == WorkflowState.EXPORT_AVAILABLE"

WHY ENUM INSTEAD OF PLAIN STRINGS?
------------------------------------
We could write: state = "EXPORT_AVAILABLE"
But strings are error-prone – a typo like "EXPORT_AVAILBLE" would silently
create a wrong state.  An Enum raises an AttributeError immediately at import
time if a name is misspelled, catching bugs before the program even runs.
"""

from enum import Enum, auto


class WorkflowState(Enum):
    """
    Ordered enumeration of every state the extraction workflow can occupy.

    `auto()` assigns incrementing integer values automatically (1, 2, 3 …).
    The actual numbers don't matter to our code; we only compare states by
    identity  (e.g.  current == WorkflowState.EXPORT_AVAILABLE).

    Naming convention: PAST_TENSE_VERB + SUBJECT  describes what has just
    been confirmed true, making assertion logic read naturally:
        assert state == WorkflowState.FORM_FILLED
    """

    # --- Pre-browser states ---
    INIT              = auto()   # Task created, browser not yet launched
    BROWSER_READY     = auto()   # Chromium context is open and healthy

    # --- Navigation states ---
    NAVIGATED         = auto()   # Portal landing page fully loaded
    FORM_FILLED       = auto()   # Year + month dropdowns set, not submitted
    SUBMIT_CLICKED    = auto()   # Submit button clicked, awaiting response

    # --- Results states ---
    RESULTS_LOADING   = auto()   # AJAX spinner visible / table not yet painted
    SEARCH_RESULTS_READY = auto()  # Data table is visible and stable in DOM

    # --- Export states ---
    EXPORT_CLICKED    = auto()   # Export button clicked, awaiting download
    EXPORT_AVAILABLE  = auto()   # Excel binary stream has been intercepted

    # --- Terminal states ---
    EXTRACTION_COMPLETE = auto()   # Rows extracted and handed to transformer
    CHECKPOINT_SAVED    = auto()   # Parquet checkpoint written to disk
    FAILED              = auto()   # Unrecoverable error; job must restart


# ---------------------------------------------------------------------------
# VALID TRANSITIONS
# ---------------------------------------------------------------------------
# This dict maps each state to the set of states it is allowed to move into.
# The Supervisor (Phase 3) will validate transitions to catch logic bugs and
# unexpected portal behaviour early.
#
# Reading it: VALID_TRANSITIONS[current_state] = {allowed_next_states}
# ---------------------------------------------------------------------------
VALID_TRANSITIONS: dict[WorkflowState, set[WorkflowState]] = {
    WorkflowState.INIT: {
        WorkflowState.BROWSER_READY,
        WorkflowState.FAILED,
    },
    WorkflowState.BROWSER_READY: {
        WorkflowState.NAVIGATED,
        WorkflowState.FAILED,
    },
    WorkflowState.NAVIGATED: {
        WorkflowState.FORM_FILLED,
        WorkflowState.FAILED,
    },
    WorkflowState.FORM_FILLED: {
        WorkflowState.SUBMIT_CLICKED,
        WorkflowState.FAILED,
    },
    WorkflowState.SUBMIT_CLICKED: {
        WorkflowState.RESULTS_LOADING,
        WorkflowState.SEARCH_RESULTS_READY,  # fast portal may skip loading
        WorkflowState.FAILED,
    },
    WorkflowState.RESULTS_LOADING: {
        WorkflowState.SEARCH_RESULTS_READY,
        WorkflowState.FAILED,
    },
    WorkflowState.SEARCH_RESULTS_READY: {
        WorkflowState.EXPORT_CLICKED,
        WorkflowState.EXTRACTION_COMPLETE,   # DOM path skips export
        WorkflowState.FAILED,
    },
    WorkflowState.EXPORT_CLICKED: {
        WorkflowState.EXPORT_AVAILABLE,
        WorkflowState.FAILED,
    },
    WorkflowState.EXPORT_AVAILABLE: {
        WorkflowState.EXTRACTION_COMPLETE,
        WorkflowState.FAILED,
    },
    WorkflowState.EXTRACTION_COMPLETE: {
        WorkflowState.CHECKPOINT_SAVED,
        WorkflowState.FAILED,
    },
    WorkflowState.CHECKPOINT_SAVED: {
        # After saving a monthly checkpoint the machine is ready to
        # start the next month (loops back to INIT for a clean per-month
        # reset) or finish the job entirely.
        # NAVIGATED is intentionally removed: the Supervisor now resets
        # to INIT at the top of every attempt, which is cleaner than
        # jumping mid-graph to NAVIGATED.
        WorkflowState.INIT,
        WorkflowState.FAILED,
    },
    WorkflowState.FAILED: set(),   # terminal – no outgoing transitions
}


def assert_state(
    current: WorkflowState,
    expected: WorkflowState,
    context: str = "",
) -> None:
    """
    Raise a WorkflowStateError if `current` does not equal `expected`.

    Usage in engine.py:
        assert_state(state, WorkflowState.EXPORT_AVAILABLE, "before stream read")

    This is the programmatic equivalent of Architecture §4's:
        "assert current_state == WorkflowState.EXPORT_AVAILABLE"

    Using a function (rather than a bare `assert` statement) means:
      1. The error message is always descriptive.
      2. Python's optimisation flag (-O) cannot silently disable it
         the way it disables bare `assert` statements.
    """
    if current != expected:
        raise WorkflowStateError(
            f"State assertion failed{' (' + context + ')' if context else ''}. "
            f"Expected {expected.name!r} but current state is {current.name!r}."
        )


def transition(
    current: WorkflowState,
    next_state: WorkflowState,
) -> WorkflowState:
    """
    Validate and execute a state transition.

    Returns the new state on success.
    Raises WorkflowStateError if the transition is not in VALID_TRANSITIONS.
    """
    allowed = VALID_TRANSITIONS.get(current, set())
    if next_state not in allowed:
        raise WorkflowStateError(
            f"Invalid transition: {current.name!r} → {next_state.name!r}. "
            f"Allowed next states: {[s.name for s in allowed]}"
        )
    return next_state


class WorkflowStateError(RuntimeError):
    """Raised when a state assertion fails or an invalid transition is attempted."""
