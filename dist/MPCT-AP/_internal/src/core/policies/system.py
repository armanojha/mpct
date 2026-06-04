"""
src/core/policies/system.py
============================
System-Level Operational Constraints
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
All timeout and queue wait constants live here.  These govern HOW LONG the
system is willing to wait at each boundary before returning an error to the
caller.  Changing a value here instantly propagates to every module that
imports it — no hunting through code.

WHY SEPARATE FILES FOR EACH POLICY CATEGORY? (For algoRoute)
--------------------------------------------------------------
Architecture §6 mandates "strict segregation" of configuration.  The reason
is operational safety:

  • A security engineer should be able to review/change RATE_LIMIT_PER_IP
    without accidentally touching PORTAL_TIMEOUT.
  • A DevOps engineer tuning MAX_ACTIVE_CONTEXTS for a bigger server should
    never be in the same file as HEURISTIC_MIN_CONFIDENCE.
  • Feature flags (on/off switches) must NEVER be co-located with hard
    policy limits — accidentally treating a bool as an int is a real bug.

Each file is a single "concern boundary."  This pattern is called
Separation of Concerns (SoC) — a foundational software design principle.
"""

import os

# ---------------------------------------------------------------------------
# QUEUE GOVERNANCE
# ---------------------------------------------------------------------------

# Maximum seconds the API Gateway will wait for the Supervisor to accept a
# job before returning 503.  During normal load the queue accepts instantly;
# this timeout covers the race condition where the queue fills between the
# health-check and the put() call.
MAX_QUEUE_WAIT_SECONDS: int = int(os.getenv("MAX_QUEUE_WAIT_SECONDS", "10"))

# ---------------------------------------------------------------------------
# PORTAL TIMEOUTS
# ---------------------------------------------------------------------------

# Maximum seconds to allow a single Playwright page navigation before
# treating it as a hung context (zombie) and triggering a restart.
PORTAL_TIMEOUT_SECONDS: int = int(os.getenv("PORTAL_TIMEOUT_SECONDS", "60"))

# Maximum seconds for a complete single-month extraction (navigation +
# form fill + results + optional export).  Passed to asyncio.wait_for()
# in the Supervisor's per-month retry loop.
EXTRACTION_TIMEOUT_SECONDS: int = int(os.getenv("EXTRACTION_TIMEOUT_SECONDS", "300"))

# ---------------------------------------------------------------------------
# APPLICATION LIFECYCLE
# ---------------------------------------------------------------------------

# How long (seconds) the FastAPI lifespan shutdown handler waits for
# in-flight jobs to complete before forcibly cancelling them.
GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS: int = int(
    os.getenv("GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS", "30")
)

# How often (seconds) the Supervisor's background reaper wakes to scan
# for expired /tmp checkpoint sessions.
REAP_INTERVAL_SECONDS: int = int(os.getenv("REAP_INTERVAL_SECONDS", "60"))

# TTL (minutes) after which an ephemeral checkpoint session is considered
# expired and eligible for deletion.  Architecture §3: "15 minutes."
SESSION_TTL_MINUTES: int = int(os.getenv("SESSION_TTL_MINUTES", "15"))
