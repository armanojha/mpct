"""
src/core/policies/resource.py
==============================
Resource & Concurrency Constraints
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
Constants that cap how many Playwright browser contexts and queued jobs
the system will maintain simultaneously.  Exceeding these limits causes
controlled rejection (429/503) rather than unbounded memory growth.

SEMAPHORE PATTERN (For algoRoute)
-----------------------------------
A semaphore is a counter-based concurrency lock.  Think of a parking lot
with MAX_ACTIVE_CONTEXTS spaces:

    asyncio.Semaphore(MAX_ACTIVE_CONTEXTS)

  • When a browser context wants to open: `await semaphore.acquire()`
    - If spaces remain → counter decrements → context opens immediately.
    - If full → coroutine SUSPENDS until another context releases.
  • When a context closes: `semaphore.release()` → counter increments.

This is used in Phase 5's browser_pool.py to prevent spawning more
Chromium instances than the server's RAM can support.
"""

import os

# Maximum number of simultaneously open Playwright browser contexts.
# Each context uses ~100-200 MB of RAM.  Set this based on your server's
# available memory:  MAX_ACTIVE_CONTEXTS ≈ (available_RAM_MB - 500) / 150
MAX_ACTIVE_CONTEXTS: int = int(os.getenv("MAX_ACTIVE_CONTEXTS", "5"))

# Maximum number of pending jobs in the asyncio.Queue before 429 is returned.
MAX_QUEUE_SIZE: int = int(os.getenv("MAX_QUEUE_SIZE", "50"))

# After this many consecutive job crashes the circuit breaker opens → 503.
MAX_CONSECUTIVE_CRASHES: int = int(os.getenv("MAX_CONSECUTIVE_CRASHES", "5"))

# Maximum number of concurrent HTTP requests FastAPI will accept before
# the ASGI server starts queuing at the TCP layer.
# This is configured at the Uvicorn/Gunicorn level in Phase 5.
MAX_WORKER_PROCESSES: int = int(os.getenv("MAX_WORKER_PROCESSES", "2"))
