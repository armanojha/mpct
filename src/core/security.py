"""
src/core/security.py
=====================
Canonicalization Engine & Cryptographic Idempotency
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
This module implements Architecture §5: "Security & Canonicalization Model."

It has two responsibilities:
  1. INPUT CANONICALIZATION  — normalize raw user inputs into a single
     canonical form so that "sbin000377 ", "SBIN000377", and "Sbin000377"
     all resolve to the identical string BEFORE any hashing or database
     lookup occurs.

  2. HMAC IDEMPOTENCY KEYS  — hash the normalized payload with a server-
     side secret so that:
     a. The same logical request always produces the same key (idempotency).
     b. The key cannot be reverse-engineered to recover the raw account
        number (rainbow-table resistance).

WHY CANONICALIZE BEFORE HASHING? (For algoRoute)
──────────────────────────────────────────────────
A hash function maps data to a fixed-length fingerprint:
    sha256("SBIN000377") = "a3f9…"
    sha256("sbin000377") = "1c72…"   ← completely different!

If the same logical entity produces different hashes, idempotency breaks:
  • The client retries with the same account but different casing.
  • The server sees two different hashes → treats it as two different jobs.
  • The user gets billed / charged twice, or extraction runs twice.

Canonicalization ensures: same logical input → same canonical string → same
hash → same idempotency key → server rejects the duplicate gracefully.

HMAC vs PLAIN HASH (For algoRoute)
────────────────────────────────────
A plain SHA-256 hash is public:
    sha256("SBIN000377" + "123456789012") = "d4e5…"

An attacker with a list of common account numbers can pre-compute all their
hashes and look up any intercepted hash to find the original value.  This
is called a RAINBOW TABLE attack.

HMAC (Hash-based Message Authentication Code) adds a SECRET KEY:
    hmac(SECRET, "SBIN000377123456789012") = "9f1a…"

Without knowing the SECRET, the attacker cannot compute the lookup table.
The SECRET is stored only in the server's environment variable
(HMAC_SECRET_KEY), never sent to the client.
"""

import hashlib
import hmac
import re
from typing import Optional

from src.core.policies.security import (
    HMAC_ALGORITHM,
    HMAC_SECRET_KEY,
    IFSC_PATTERN,
    MAX_ACCOUNT_NUMBER_LENGTH,
    MIN_ACCOUNT_NUMBER_LENGTH,
)


# ──────────────────────────────────────────────────────────────────────────────
# INPUT CANONICALIZATION
# ──────────────────────────────────────────────────────────────────────────────

def canonicalize_ifsc(raw: str) -> str:
    """
    Normalize an IFSC code to its canonical form.

    Canonical form: UPPER CASE, no surrounding whitespace.
    Example: "  sbin000377 " → "SBIN000377"

    Parameters
    ----------
    raw : the IFSC string as received from the user/client

    Returns
    -------
    Canonical IFSC string.

    Raises
    ------
    CanonicalizeError if the result does not match the expected IFSC pattern.
    """
    canonical = raw.strip().upper()

    if not re.fullmatch(IFSC_PATTERN, canonical):
        raise CanonicalizeError(
            f"Invalid IFSC code after canonicalization: {canonical!r}. "
            f"Expected format: 4 alpha chars + '0' + 6 alphanumeric chars "
            f"(e.g. SBIN0001234)."
        )

    return canonical


def canonicalize_account_number(raw: str) -> str:
    """
    Normalize a bank account number to its canonical form.

    Canonical form: digits only (strips spaces, hyphens, and other
    formatting characters), leading zeros PRESERVED.

    Example: " 12-3456 789 012 " → "123456789012"

    Parameters
    ----------
    raw : account number string as received from the client

    Returns
    -------
    Canonical account number string (digits only).

    Raises
    ------
    CanonicalizeError if the cleaned number has an invalid length.
    """
    # Remove all non-digit characters.
    canonical = re.sub(r"\D", "", raw)

    if not (MIN_ACCOUNT_NUMBER_LENGTH <= len(canonical) <= MAX_ACCOUNT_NUMBER_LENGTH):
        raise CanonicalizeError(
            f"Account number has invalid length after canonicalization: "
            f"{len(canonical)} digits (expected "
            f"{MIN_ACCOUNT_NUMBER_LENGTH}–{MAX_ACCOUNT_NUMBER_LENGTH})."
        )

    return canonical


def canonicalize_payload(
    ifsc: str,
    account_number: str,
    year: int,
    months: list[int],
) -> str:
    """
    Produce the canonical string representation of a full extraction request.

    This canonical string is the INPUT to the HMAC function.  It must be:
      • Deterministic: same logical request → same canonical string always.
      • Injective: different logical requests → different canonical strings.

    Format:
        "IFSC|ACCOUNT|YEAR|MONTH1,MONTH2,…"
    Example:
        "SBIN000377|123456789012|2024|1,2,3,4,5,6,7,8,9,10,11,12"

    Months are sorted before joining to ensure month order does not affect
    the canonical form:  [3,1,2] → "1,2,3"  (same as [1,2,3]).

    Parameters
    ----------
    ifsc           : already canonicalized IFSC code
    account_number : already canonicalized account number
    year           : 4-digit fiscal year
    months         : list of 1-based month numbers

    Returns
    -------
    Canonical payload string ready for HMAC hashing.
    """
    months_str = ",".join(str(m) for m in sorted(set(months)))
    return f"{ifsc}|{account_number}|{year}|{months_str}"


# ──────────────────────────────────────────────────────────────────────────────
# HMAC IDEMPOTENCY KEY
# ──────────────────────────────────────────────────────────────────────────────

def generate_idempotency_key(canonical_payload: str) -> str:
    """
    Compute an HMAC-SHA256 idempotency key for the given canonical payload.

    Architecture §5:
        idempotency_key = hmac.new(SECRET_KEY,
                                   normalized_payload.encode(),
                                   hashlib.sha256).hexdigest()

    HOW hmac.new() WORKS (For algoRoute)
    ──────────────────────────────────────
    hmac.new(key, msg, digestmod) creates an HMAC object:
      • key       — the server-side secret (bytes)
      • msg       — the message to authenticate (bytes)
      • digestmod — the underlying hash function (hashlib.sha256)

    .hexdigest() returns the 64-character lowercase hex string of the
    resulting 32-byte HMAC digest.

    Parameters
    ----------
    canonical_payload : output of canonicalize_payload()

    Returns
    -------
    64-character hex string (HMAC-SHA256 digest).
    """
    mac = hmac.new(
        key        = HMAC_SECRET_KEY.encode("utf-8"),
        msg        = canonical_payload.encode("utf-8"),
        digestmod  = hashlib.sha256,
    )
    return mac.hexdigest()


def build_idempotency_key(
    ifsc: str,
    account_number: str,
    year: int,
    months: list[int],
) -> str:
    """
    Convenience wrapper: canonicalize inputs, then generate the HMAC key.

    This is the function called at the API boundary (deps.py) to produce
    the idempotency key for each incoming request.

    Parameters
    ----------
    ifsc           : raw IFSC string from the request body
    account_number : raw account number string from the request body
    year           : fiscal year integer
    months         : list of month integers

    Returns
    -------
    64-character HMAC-SHA256 hex digest.

    Raises
    ------
    CanonicalizeError if any input fails validation.
    """
    c_ifsc    = canonicalize_ifsc(ifsc)
    c_account = canonicalize_account_number(account_number)
    payload   = canonicalize_payload(c_ifsc, c_account, year, months)
    return generate_idempotency_key(payload)


def verify_idempotency_key(
    ifsc: str,
    account_number: str,
    year: int,
    months: list[int],
    provided_key: str,
) -> bool:
    """
    Constant-time comparison of a client-provided idempotency key against
    the server-recomputed key.

    TIMING ATTACK RESISTANCE (For algoRoute)
    ──────────────────────────────────────────
    A naive string comparison `computed == provided` exits as soon as it
    finds the first differing character.  This leaks timing information:
      • If the comparison takes 0.001ms, the first char is wrong.
      • If it takes 0.031ms, the first 31 chars are correct.

    An attacker who sends millions of guesses can use these timing
    differences to reconstruct the secret one character at a time.

    `hmac.compare_digest(a, b)` always takes the SAME amount of time
    regardless of where the strings differ — no timing information leaks.

    Parameters
    ----------
    provided_key : the idempotency key the client sent in X-Idempotency-Key header

    Returns
    -------
    True if the key matches; False otherwise.
    """
    try:
        expected = build_idempotency_key(ifsc, account_number, year, months)
    except CanonicalizeError:
        return False

    return hmac.compare_digest(expected, provided_key)


# ──────────────────────────────────────────────────────────────────────────────
# CUSTOM EXCEPTION
# ──────────────────────────────────────────────────────────────────────────────

class CanonicalizeError(ValueError):
    """
    Raised when an input fails validation during canonicalization.
    The API endpoint catches this and returns HTTP 422 Unprocessable Entity.
    """
