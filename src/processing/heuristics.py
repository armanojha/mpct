"""
src/processing/heuristics.py
=============================
Phase 2 – Confidence Scoring Engine
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
Before trusting data from the Primary (DOM) engine and handing it to the
transformer, we run it through this heuristics module.  The module assigns a
single confidence score between 0.0 and 1.0.  If that score falls below the
threshold (0.90), the system discards the DOM result and triggers the
Secondary (Excel stream) engine as a fallback.

This is Architecture §4:
    "Fallback to the secondary binary stream is governed by a weighted
     confidence score."

WHAT IS A HEURISTIC? (For algoRoute)
-------------------------------------
A heuristic is a practical rule-of-thumb that gives a "good enough" answer
quickly, even when you can't prove it is perfectly correct.

Example heuristic: "If I extracted 3 rows but historically I get ~120, something
is probably wrong."  We can't know for certain whether 3 rows is a real slow
month or a broken scrape — but it's a strong signal worth acting on.

WEIGHTED SCORING (For algoRoute)
----------------------------------
Imagine five judges scoring a gymnastics routine.  Each judge is considered
more or less important, so their scores are multiplied by a weight before
being summed.  The weights add up to 1.0 (100 %).

Our five checks and their weights (from Architecture §4):

  Check                        Weight   What it tests
  ─────────────────────────────────────────────────────────────────────
  Historical Baseline Match     0.25    Row count within expected range
  Expected Columns Present      0.25    Exact header set matches schema
  Numeric Columns Valid         0.20    Amount column contains no strings
  Row Count Reasonable          0.15    Matches HTML pagination metadata
  Duplicate Anomalies Absent    0.15    No fully identical rows

  Total                         1.00

Each check returns 1.0 (pass) or 0.0 (fail).  Partial credit is possible
for soft checks (e.g., "half the expected columns are present" → 0.5 × 0.25).

Final score = sum(check_result × weight for each check)
Threshold   = 0.90  → any score below this triggers the stream fallback.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIGURATION CONSTANTS
# ---------------------------------------------------------------------------

# Architecture §4: "If Score < 0.90, trigger fallback."
CONFIDENCE_THRESHOLD: float = 0.90

# Weights for each of the five validation checks.
# MUST sum to 1.0 — enforced by the module-level assertion below.
WEIGHT_HISTORICAL_BASELINE : float = 0.25
WEIGHT_EXPECTED_COLUMNS    : float = 0.25
WEIGHT_NUMERIC_VALID       : float = 0.20
WEIGHT_ROW_COUNT_REASONABLE: float = 0.15
WEIGHT_DUPLICATE_ABSENT    : float = 0.15

_total_weight = (
    WEIGHT_HISTORICAL_BASELINE
    + WEIGHT_EXPECTED_COLUMNS
    + WEIGHT_NUMERIC_VALID
    + WEIGHT_ROW_COUNT_REASONABLE
    + WEIGHT_DUPLICATE_ABSENT
)
assert abs(_total_weight - 1.0) < 1e-9, (
    f"Heuristic weights must sum to 1.0, got {_total_weight}"
)

# The canonical set of column names our transformer guarantees.
# A new month's DataFrame must contain AT LEAST these columns.
EXPECTED_COLUMNS: frozenset[str] = frozenset({
    "serial_no",
    "ddo_code",
    "ddo_name",
    "amount",
    "payment_date",
    "voucher_no",
})

# Column that holds monetary values — used for the numeric-validity check.
AMOUNT_COLUMN = "amount"

# How many standard deviations from the historical mean is still "normal".
# Z-score > ZSCORE_THRESHOLD → row-count anomaly detected.
ZSCORE_THRESHOLD: float = 2.5


# ===========================================================================
# RESULT DATACLASS
# ===========================================================================

@dataclass
class ConfidenceReport:
    """
    Immutable record of an individual confidence evaluation.

    DATACLASS (For algoRoute)
    -------------------------
    A dataclass is a Python class that automatically generates __init__,
    __repr__, and __eq__ for you based on the annotated fields.  It is
    perfect for plain data containers like this one.

    Fields
    ------
    score           : final weighted confidence (0.0 – 1.0)
    passed          : True if score >= CONFIDENCE_THRESHOLD
    check_scores    : individual (raw) score for each check keyed by name
    weighted_scores : each check_score × its weight
    warnings        : human-readable explanation for any failed check
    row_count       : how many rows the DataFrame contained
    """
    score           : float
    passed          : bool
    check_scores    : dict[str, float]  = field(default_factory=dict)
    weighted_scores : dict[str, float]  = field(default_factory=dict)
    warnings        : list[str]         = field(default_factory=list)
    row_count       : int               = 0

    def __str__(self) -> str:
        status = "PASS ✓" if self.passed else "FAIL ✗"
        lines = [
            f"ConfidenceReport [{status}]  score={self.score:.4f}  rows={self.row_count}",
        ]
        for name, ws in self.weighted_scores.items():
            raw = self.check_scores.get(name, 0.0)
            lines.append(f"  {name:<30s}  raw={raw:.2f}  weighted={ws:.4f}")
        for w in self.warnings:
            lines.append(f"  ⚠  {w}")
        return "\n".join(lines)


# ===========================================================================
# HISTORICAL BASELINE STORE (in-memory for Phase 2)
# ===========================================================================
# In production (Phase 4+) this would be persisted to a database.
# For now it is a module-level dictionary that accumulates baselines
# across calls within the same Python process lifetime.
#
# Structure:
#   _baselines[(year, month)] = {"mean": float, "std": float, "n": int}
# ---------------------------------------------------------------------------
_baselines: dict[tuple[int, int], dict] = {}


def update_baseline(year: int, month: int, row_count: int) -> None:
    """
    Record a successful extraction's row count so future runs can compare.

    Uses Welford's online algorithm to update mean and variance
    incrementally WITHOUT storing every historical value — O(1) memory.

    WELFORD'S ALGORITHM (simplified for algoRoute)
    ------------------------------------------------
    Normally to compute a mean you sum all values and divide.  But if you
    have millions of past observations you'd need to store all of them.
    Welford's method updates mean and variance using only the new value
    and the previously computed mean/variance — a single pass, constant
    memory.
    """
    key = (year, month)
    if key not in _baselines:
        _baselines[key] = {"mean": float(row_count), "std": 0.0, "n": 1}
        return

    b = _baselines[key]
    n_old = b["n"]
    mean_old = b["mean"]
    n_new = n_old + 1

    # Welford's online update
    delta      = row_count - mean_old
    mean_new   = mean_old + delta / n_new
    delta2     = row_count - mean_new
    # M2 (sum of squared deviations) — we store std as approximation
    m2_old     = (b["std"] ** 2) * n_old
    m2_new     = m2_old + delta * delta2
    std_new    = (m2_new / n_new) ** 0.5 if n_new > 1 else 0.0

    _baselines[key] = {"mean": mean_new, "std": std_new, "n": n_new}
    logger.debug(
        "[HEURISTICS] Baseline updated for (%s, %s): mean=%.1f std=%.1f n=%d",
        year, month, mean_new, std_new, n_new,
    )


def get_baseline(year: int, month: int) -> Optional[dict]:
    """Return the stored baseline for (year, month), or None if unseen."""
    return _baselines.get((year, month))


# ===========================================================================
# THE FIVE INDIVIDUAL CHECK FUNCTIONS
# ===========================================================================
# Each function returns a float in [0.0, 1.0].
# 1.0 = perfect pass   0.0 = hard fail   values in between = partial credit
# ===========================================================================

def _check_historical_baseline(
    df: pd.DataFrame,
    year: int,
    month: int,
) -> tuple[float, Optional[str]]:
    """
    CHECK 1 – Historical Baseline Match  (weight 0.25)

    Compare this extraction's row count against the running historical mean
    for the same (year, month) slot.

    Scoring logic:
      • No baseline stored yet → score 1.0 (benefit of the doubt on first run)
      • Row count within ZSCORE_THRESHOLD standard deviations → score 1.0
      • Outside threshold → score 0.0 and emit a warning

    Returns (score, warning_or_None)
    """
    baseline = get_baseline(year, month)

    if baseline is None or baseline["n"] < 3:
        # Not enough history to make a judgment — give full credit.
        return 1.0, None

    mean = baseline["mean"]
    std  = baseline["std"]
    n    = len(df)

    if std < 1e-6:
        # Zero variance: every past run returned the same count.
        if n == int(mean):
            return 1.0, None
        warning = (
            f"Historical baseline has zero variance (always {int(mean)} rows) "
            f"but this run returned {n} rows."
        )
        return 0.0, warning

    z_score = abs(n - mean) / std
    if z_score <= ZSCORE_THRESHOLD:
        return 1.0, None

    warning = (
        f"Row count anomaly: extracted {n} rows, "
        f"historical mean={mean:.1f} ± {std:.1f} (z={z_score:.2f}). "
        f"Threshold z={ZSCORE_THRESHOLD}."
    )
    return 0.0, warning


def _check_expected_columns(
    df: pd.DataFrame,
) -> tuple[float, Optional[str]]:
    """
    CHECK 2 – Expected Columns Present  (weight 0.25)

    Computes the fraction of EXPECTED_COLUMNS that are actually present in
    the DataFrame.  Missing columns earn zero partial credit proportionally.

    Example:
        EXPECTED = {a, b, c, d}   ACTUAL = {a, b, c, x}
        Missing  = {d}            Present fraction = 3/4 = 0.75

    A score of 0.75 × 0.25 (weight) = 0.1875 contribution to total.
    Since 0.1875 < 0.25 (max possible), the total score cannot reach 1.0,
    triggering the fallback.
    """
    actual_cols   = frozenset(df.columns)
    present       = EXPECTED_COLUMNS & actual_cols      # set intersection
    missing       = EXPECTED_COLUMNS - actual_cols      # set difference

    if not missing:
        return 1.0, None

    score = len(present) / len(EXPECTED_COLUMNS)
    warning = f"Missing expected columns: {sorted(missing)}"
    return score, warning


def _check_numeric_valid(df: pd.DataFrame) -> tuple[float, Optional[str]]:
    """
    CHECK 3 – Numeric Columns Valid  (weight 0.20)

    After the transformer has run, the `amount` column should be float64.
    Any remaining non-numeric (object dtype) values indicate the transformer
    failed to parse something — a sign the DOM table had unexpected content.

    Score:
      • Column absent entirely → 0.0  (structural failure)
      • Column present, dtype is float64 → 1.0
      • Column present but has NaN > 50 % → 0.5  (partial concern)
      • Column is not numeric dtype → 0.0
    """
    if AMOUNT_COLUMN not in df.columns:
        return 0.0, f"Column '{AMOUNT_COLUMN}' is absent — cannot validate numeric content."

    col = df[AMOUNT_COLUMN]

    if not pd.api.types.is_float_dtype(col):
        return 0.0, (
            f"Column '{AMOUNT_COLUMN}' has dtype {col.dtype!r}; "
            f"expected float64.  DOM may contain non-numeric strings."
        )

    nan_fraction = col.isna().mean()   # fraction of NaN values (0.0 – 1.0)
    if nan_fraction > 0.50:
        return 0.5, (
            f"Column '{AMOUNT_COLUMN}' is float64 but "
            f"{nan_fraction:.0%} of values are NaN (threshold: 50%)."
        )

    return 1.0, None


def _check_row_count_reasonable(
    df: pd.DataFrame,
    pagination_hint: Optional[int],
) -> tuple[float, Optional[str]]:
    """
    CHECK 4 – Row Count Reasonable  (weight 0.15)

    If the caller provides `pagination_hint` — the total row count advertised
    by the portal's pagination widget (e.g. "Showing 1–50 of 143 records") —
    we compare the actual DataFrame length against it.

    When no hint is available (None), we apply a bare sanity check:
      • 0 rows  → 0.0  (empty result is suspicious)
      • 1–10    → 0.5  (very small; might be real but warrants attention)
      • 11+     → 1.0  (reasonable)
    """
    n = len(df)

    if pagination_hint is not None:
        if n == 0:
            return 0.0, "DataFrame is empty; pagination claimed non-zero rows."

        ratio = n / pagination_hint if pagination_hint > 0 else 0.0
        if 0.95 <= ratio <= 1.05:
            # Within 5 % — pagination and extraction agree
            return 1.0, None
        warning = (
            f"Row count mismatch: extracted {n} rows, "
            f"pagination hint={pagination_hint} (ratio={ratio:.2f})."
        )
        return max(0.0, 1.0 - abs(1.0 - ratio)), warning

    # No pagination hint — heuristic-only
    if n == 0:
        return 0.0, "Extracted 0 rows; possible empty result or failed scrape."
    if n <= 10:
        return 0.5, f"Only {n} rows extracted; suspiciously low without pagination context."
    return 1.0, None


def _check_duplicate_anomalies(df: pd.DataFrame) -> tuple[float, Optional[str]]:
    """
    CHECK 5 – Duplicate Anomalies Absent  (weight 0.15)

    Exact duplicate rows indicate pagination overlap, a scraping loop bug,
    or a portal rendering glitch.

    Score:
      • 0 duplicates        → 1.0
      • ≤ 5 % duplicates    → 0.75  (minor, possibly pagination edge)
      • 5 – 20 % duplicates → 0.25  (significant; likely extraction bug)
      • > 20 % duplicates   → 0.0   (data is unreliable)
    """
    if df.empty:
        return 0.0, "DataFrame is empty."

    n_total    = len(df)
    n_unique   = df.drop_duplicates().shape[0]
    n_dupes    = n_total - n_unique
    dupe_frac  = n_dupes / n_total

    if dupe_frac == 0.0:
        return 1.0, None
    if dupe_frac <= 0.05:
        return 0.75, f"Minor duplication: {n_dupes} duplicate rows ({dupe_frac:.1%})."
    if dupe_frac <= 0.20:
        return 0.25, f"Significant duplication: {n_dupes} duplicate rows ({dupe_frac:.1%})."

    return 0.0, f"Severe duplication: {n_dupes} rows ({dupe_frac:.1%}) are exact duplicates."


# ===========================================================================
# MAIN PUBLIC FUNCTION
# ===========================================================================

def evaluate_confidence(
    df: pd.DataFrame,
    year: int,
    month: int,
    pagination_hint: Optional[int] = None,
) -> ConfidenceReport:
    """
    Run all five heuristic checks against a normalised DataFrame and return
    a ConfidenceReport.

    Parameters
    ----------
    df              : normalised DataFrame from transformer.normalize()
    year            : fiscal year of this extraction
    month           : month of this extraction
    pagination_hint : total rows advertised by portal pagination widget,
                      or None if not captured

    Returns
    -------
    ConfidenceReport  — inspect `.passed` for the go/no-go decision and
                        `.warnings` for diagnostic detail.

    Typical usage in engine.py (Phase 3 integration):

        report = evaluate_confidence(df, year=2024, month=4)
        if not report.passed:
            logger.warning("Low confidence (%s); switching to stream engine.", report.score)
            # … trigger fallback …
        else:
            update_baseline(year, month, len(df))   # record success
    """
    logger.info(
        "[HEURISTICS] Evaluating confidence for year=%s month=%s rows=%d",
        year, month, len(df),
    )

    # ------------------------------------------------------------------
    # Run all five checks.
    # Each returns (raw_score: float, warning: str | None).
    # ------------------------------------------------------------------
    checks: dict[str, tuple[float, float, Optional[str]]] = {
        #  check_name              : (raw_score, weight,    warning)
        "historical_baseline"  : (*_check_historical_baseline(df, year, month),),
        "expected_columns"     : (*_check_expected_columns(df),),
        "numeric_valid"        : (*_check_numeric_valid(df),),
        "row_count_reasonable" : (*_check_row_count_reasonable(df, pagination_hint),),
        "duplicate_absent"     : (*_check_duplicate_anomalies(df),),
    }

    weights: dict[str, float] = {
        "historical_baseline"  : WEIGHT_HISTORICAL_BASELINE,
        "expected_columns"     : WEIGHT_EXPECTED_COLUMNS,
        "numeric_valid"        : WEIGHT_NUMERIC_VALID,
        "row_count_reasonable" : WEIGHT_ROW_COUNT_REASONABLE,
        "duplicate_absent"     : WEIGHT_DUPLICATE_ABSENT,
    }

    # ------------------------------------------------------------------
    # Compute weighted score.
    #
    # MATRIX OPERATION (For algoRoute)
    # -----------------------------------
    # This is a dot product of two vectors:
    #   raw_scores = [s1, s2, s3, s4, s5]
    #   weights    = [w1, w2, w3, w4, w5]
    #   score      = s1*w1 + s2*w2 + s3*w3 + s4*w4 + s5*w5
    #
    # In linear algebra: score = raw_scores · weights
    # In numpy:          score = np.dot(raw_scores, weights)
    # ------------------------------------------------------------------
    check_scores    : dict[str, float] = {}
    weighted_scores : dict[str, float] = {}
    warnings        : list[str]        = []

    for name, (raw, warning) in checks.items():
        w  = weights[name]
        ws = raw * w
        check_scores[name]    = raw
        weighted_scores[name] = ws
        if warning:
            warnings.append(f"[{name}] {warning}")
        logger.debug(
            "[HEURISTICS]  %-28s raw=%.2f  weight=%.2f  weighted=%.4f",
            name, raw, w, ws,
        )

    final_score = sum(weighted_scores.values())
    passed      = final_score >= CONFIDENCE_THRESHOLD

    report = ConfidenceReport(
        score           = final_score,
        passed          = passed,
        check_scores    = check_scores,
        weighted_scores = weighted_scores,
        warnings        = warnings,
        row_count       = len(df),
    )

    log_fn = logger.info if passed else logger.warning
    log_fn(
        "[HEURISTICS] Final score: %.4f  (%s)  threshold=%.2f",
        final_score,
        "PASS" if passed else "FAIL – FALLBACK TRIGGERED",
        CONFIDENCE_THRESHOLD,
    )
    if warnings:
        for w in warnings:
            logger.warning("[HEURISTICS]   %s", w)

    return report


# ===========================================================================
# QUICK LOCAL TEST  (python -m src.processing.heuristics)
# ===========================================================================

if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(
        level=_logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )

    # Seed some historical data so the baseline check has context.
    for _m in range(5):
        update_baseline(2024, 4, 118 + _m * 2)

    # Build a synthetic "good" DataFrame that should score >= 0.90
    _good_df = pd.DataFrame({
        "serial_no"   : range(1, 121),
        "ddo_code"    : [f"SBIN{i:06d}" for i in range(120)],
        "ddo_name"    : [f"Office {i}" for i in range(120)],
        "amount"      : [float(1000 + i * 10) for i in range(120)],
        "payment_date": pd.date_range("2024-04-01", periods=120, freq="D"),
        "voucher_no"  : [f"VCH-{i:04d}" for i in range(120)],
    })

    print("\n=== GOOD DATA REPORT ===")
    print(evaluate_confidence(_good_df, year=2024, month=4))

    # Build a "bad" DataFrame (3 rows — huge anomaly vs 120 historical)
    _bad_df = _good_df.head(3).copy()
    _bad_df["amount"] = "invalid_string"   # also breaks numeric check

    print("\n=== BAD DATA REPORT ===")
    print(evaluate_confidence(_bad_df, year=2024, month=4))
