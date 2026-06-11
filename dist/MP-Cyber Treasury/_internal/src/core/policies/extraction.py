"""
src/core/policies/extraction.py
================================
Extraction & Heuristics Policy Constraints
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
Constants that govern the adaptive extraction engine and confidence scoring.
These are the "quality gates" — if extraction output doesn't meet these
thresholds it is rejected and a fallback is triggered.
"""

import os

# ---------------------------------------------------------------------------
# CONFIDENCE SCORING
# ---------------------------------------------------------------------------

# Architecture §4: "If Score < 0.90, trigger fallback."
# Expressed as a float in [0.0, 1.0].
HEURISTIC_MIN_CONFIDENCE: float = float(
    os.getenv("HEURISTIC_MIN_CONFIDENCE", "0.90")
)

# ---------------------------------------------------------------------------
# RETRY POLICY
# ---------------------------------------------------------------------------

# Maximum number of per-month retry attempts before the Supervisor marks
# that month as permanently failed.
MAX_MONTH_RETRIES: int = int(os.getenv("MAX_MONTH_RETRIES", "3"))

# Base for exponential backoff between retries (seconds).
# Wait after attempt N = min(RETRY_BACKOFF_BASE ** N, MAX_RETRY_BACKOFF_SECONDS)
RETRY_BACKOFF_BASE: float = float(os.getenv("RETRY_BACKOFF_BASE", "2.0"))
MAX_RETRY_BACKOFF_SECONDS: float = float(os.getenv("MAX_RETRY_BACKOFF_SECONDS", "30.0"))

# ---------------------------------------------------------------------------
# JOB SCOPE LIMITS
# ---------------------------------------------------------------------------

# Maximum number of months that may be requested in a single extraction job.
# Prevents a single request from monopolising the Supervisor for 12 months.
MAX_MONTHS_PER_JOB: int = int(os.getenv("MAX_MONTHS_PER_JOB", "12"))

# Minimum and maximum valid fiscal year values accepted at the API boundary.
MIN_FISCAL_YEAR: int = int(os.getenv("MIN_FISCAL_YEAR", "2015"))
MAX_FISCAL_YEAR: int = int(os.getenv("MAX_FISCAL_YEAR", "2030"))
