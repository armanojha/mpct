"""
src/core/feature_flags.py
==========================
Runtime Feature Toggle Switches
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
Feature flags are boolean on/off switches that let the team enable or
disable specific extraction strategies WITHOUT changing policy constants
or redeploying code.

WHY FEATURE FLAGS ARE SEPARATE FROM POLICIES (For algoRoute)
-------------------------------------------------------------
Policies are HARD CONSTRAINTS:  "never accept a payload > 10 KB."
Feature flags are SOFT TOGGLES:  "currently use the DOM engine."

Mixing them in the same file is dangerous:
  • A flag accidentally set to 0 (int) instead of False (bool) could be
    mistaken for a policy limit, silently disabling a feature.
  • Feature flags change frequently during rollout; policy files should
    be as stable as possible to minimise review overhead.

READING FLAGS FROM ENVIRONMENT VARIABLES
------------------------------------------
`os.getenv("VAR", default)` reads an environment variable named VAR at
IMPORT TIME, returning `default` if the variable is not set.

In production (Cloud Run, Phase 5) these are set in the container's
environment:
    ENABLE_DOM_ENGINE=true
    ENABLE_STREAM_ENGINE=true
    ENABLE_PARQUET_CHECKPOINT=true

In a canary deployment or during an incident you can disable the DOM
engine without redeploying:
    ENABLE_DOM_ENGINE=false  → only stream engine runs

The `_bool()` helper converts the string "true"/"false" to a Python bool
because os.getenv always returns a string.
"""

import os


def _bool(env_var: str, default: bool) -> bool:
    """
    Read `env_var` from the environment and convert to bool.

    "true", "1", "yes" (case-insensitive) → True
    anything else, or absent              → `default`
    """
    raw = os.getenv(env_var, "").strip().lower()
    if raw in ("true", "1", "yes"):
        return True
    if raw in ("false", "0", "no"):
        return False
    return default


# ---------------------------------------------------------------------------
# EXTRACTION ENGINE FLAGS
# ---------------------------------------------------------------------------

# Allow the primary DOM scraping engine to run.
# Disable during portal maintenance windows when the DOM structure is broken.
ENABLE_DOM_ENGINE: bool = _bool("ENABLE_DOM_ENGINE", default=True)

# Allow the secondary binary stream (Excel download) engine to run.
# Disable if the portal removes the export button (rare but possible).
ENABLE_STREAM_ENGINE: bool = _bool("ENABLE_STREAM_ENGINE", default=True)

# ---------------------------------------------------------------------------
# DURABILITY FLAGS
# ---------------------------------------------------------------------------

# Write encrypted Parquet checkpoints after each successful month.
# Disable ONLY in unit tests where /tmp writes are undesirable.
ENABLE_PARQUET_CHECKPOINT: bool = _bool("ENABLE_PARQUET_CHECKPOINT", default=True)

# ---------------------------------------------------------------------------
# SECURITY FLAGS
# ---------------------------------------------------------------------------

# Enforce HMAC idempotency key validation on every POST request.
# Disable ONLY for local development without a configured HMAC secret.
ENABLE_HMAC_VALIDATION: bool = _bool("ENABLE_HMAC_VALIDATION", default=True)

# Enforce JWT bearer token validation on protected endpoints.
ENABLE_JWT_AUTH: bool = _bool("ENABLE_JWT_AUTH", default=True)

# Enforce per-IP rate limiting via SlowAPI.
ENABLE_RATE_LIMITING: bool = _bool("ENABLE_RATE_LIMITING", default=True)

# ---------------------------------------------------------------------------
# OBSERVABILITY FLAGS
# ---------------------------------------------------------------------------

# Emit OpenTelemetry traces (Phase 5 telemetry module).
ENABLE_OTEL_TRACING: bool = _bool("ENABLE_OTEL_TRACING", default=False)

# Expose /metrics endpoint for Prometheus scraping.
ENABLE_PROMETHEUS_METRICS: bool = _bool("ENABLE_PROMETHEUS_METRICS", default=False)
