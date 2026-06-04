"""
src/core/policies/security.py
==============================
Security & Input Governance Constraints
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
All security-relevant limits live here.  A dedicated security engineer
can review this single file to audit every enforcement boundary without
reading unrelated business logic.

DEFENCE-IN-DEPTH (For algoRoute)
-----------------------------------
Security isn't a single lock on the front door — it's layers:
  Layer 1  → MAX_PAYLOAD_SIZE_KB   (stop oversized requests at the ASGI layer)
  Layer 2  → RATE_LIMIT_PER_MINUTE  (stop brute-force / DDoS at the route layer)
  Layer 3  → HMAC idempotency keys  (stop replay attacks in security.py)
  Layer 4  → Input canonicalization (stop hash-collision attacks via whitespace)
  Layer 5  → Fernet encryption      (stop filesystem reads of /tmp checkpoints)

Each layer is independent — even if one is misconfigured, the others hold.
"""

import os

# ---------------------------------------------------------------------------
# PAYLOAD SIZE GOVERNANCE
# ---------------------------------------------------------------------------

# Architecture §5: "MAX_REQUEST_BODY_SIZE_KB = 10"
# Requests larger than this are rejected at the ASGI middleware layer
# before FastAPI even parses the JSON body.
MAX_PAYLOAD_SIZE_KB: int = int(os.getenv("MAX_PAYLOAD_SIZE_KB", "10"))

# Derived byte limit for convenience.
MAX_PAYLOAD_SIZE_BYTES: int = MAX_PAYLOAD_SIZE_KB * 1024

# ---------------------------------------------------------------------------
# RATE LIMITING
# ---------------------------------------------------------------------------

# Maximum requests per IP per minute.  Enforced by the SlowAPI dependency
# injected into the FastAPI route (Phase 4 deps.py).
RATE_LIMIT_PER_MINUTE: int = int(os.getenv("RATE_LIMIT_PER_MINUTE", "10"))

# Burst allowance above RATE_LIMIT_PER_MINUTE before the limiter fires.
RATE_LIMIT_BURST: int = int(os.getenv("RATE_LIMIT_BURST", "3"))

# ---------------------------------------------------------------------------
# HMAC CONFIGURATION
# ---------------------------------------------------------------------------

# The server-side HMAC secret used to sign idempotency keys.
# MUST be set as an environment variable in production — never hardcoded.
# The fallback value below is ONLY for local development/testing.
HMAC_SECRET_KEY: str = os.getenv(
    "HMAC_SECRET_KEY",
    "dev-only-insecure-secret-CHANGE-IN-PRODUCTION",
)

# HMAC algorithm.  SHA-256 produces a 64-character hex digest.
HMAC_ALGORITHM: str = "sha256"

# ---------------------------------------------------------------------------
# JWT (for Phase 4 auth header validation)
# ---------------------------------------------------------------------------

# JWT signing secret — must match the React SPA's auth provider.
JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "dev-only-jwt-secret")
JWT_ALGORITHM: str  = "HS256"
JWT_EXPIRE_MINUTES: int = int(os.getenv("JWT_EXPIRE_MINUTES", "60"))

# ---------------------------------------------------------------------------
# INPUT VALIDATION PATTERNS
# ---------------------------------------------------------------------------

# Regex pattern an IFSC code must match before canonicalization.
# Format: 4 alpha chars (bank code) + 0 + 6 alphanumeric chars (branch code)
IFSC_PATTERN: str = r"^[A-Za-z]{4}0[A-Za-z0-9]{6}$"

# Maximum length of a bank account number (Indian standard: up to 18 digits).
MAX_ACCOUNT_NUMBER_LENGTH: int = 18
MIN_ACCOUNT_NUMBER_LENGTH: int = 9
