"""
src/processing/transformer.py
==============================
Phase 1 – Pandas Transformation & Normalization Layer
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
This module receives the raw list-of-dicts that the extraction engine
produced (from either the DOM or the binary Excel stream) and converts it
into a clean, standardized Pandas DataFrame.

"Standardized" means:
  • Column names are normalized (consistent casing, no extra spaces).
  • Date strings are parsed into proper Python datetime objects.
  • Amount/currency strings are converted to float64 numbers.
  • Completely empty rows are dropped.
  • Duplicate rows are removed.
  • A sentinel metadata column is added so downstream code always knows
    which engine produced the data.

WHY A SEPARATE TRANSFORMER?
----------------------------
The extraction engines know HOW to get data from the portal; they should
not care about HOW to clean it.  By keeping transformation isolated here
we can:
  1. Unit-test transformation logic independently of Playwright.
  2. Swap in a different portal without touching cleaning rules.
  3. Pipe the same cleaning logic regardless of which engine ran.

PANDAS MENTAL MODEL FOR FIRST-SEMESTER STUDENTS (algoRoute)
------------------------------------------------------------
A Pandas DataFrame is essentially a 2D matrix – like a spreadsheet in
memory:

       Sr No  | DDO Code  | Amount    | Date
       -------|-----------|-----------|----------
       1      | SBIN0001  | 12500.00  | 2024-01-01
       2      | SBIN0002  | 8300.50   | 2024-01-01

Rows are indexed (0, 1, 2 …) and columns are named.  Pandas lets you:
  • Apply a function to an entire column at once  (vectorised operation)
  • Filter rows with boolean masks                (df[df["col"] > 0])
  • Group and aggregate                           (df.groupby("DDO"))
  • Merge/join two DataFrames like SQL JOINs

All the heavy math happens inside NumPy C-extensions – far faster than
a Python for-loop over individual cells.
"""

import logging
import re
from typing import Optional

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Module logger
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# COLUMN NAME MAPPING  (raw portal label → our canonical column name)
# ---------------------------------------------------------------------------
# The portal may render columns with varying spacing, capitalisation or
# abbreviations.  We normalise everything to snake_case.
#
# Keys   = exact strings as they appear in the portal (after .strip())
# Values = what we rename them to inside our DataFrame
#
# Add more rows here as you discover portal label variants during testing.
# ---------------------------------------------------------------------------
COLUMN_RENAME_MAP: dict[str, str] = {
    # Common treasury column names (adjust after inspecting the live portal)
    "Sr No"              : "serial_no",
    "Sr. No."            : "serial_no",
    "S.No"               : "serial_no",
    "DDO Code"           : "ddo_code",
    "DDO Name"           : "ddo_name",
    "Treasury Code"      : "treasury_code",
    "Demand No"          : "demand_no",
    "Major Head"         : "major_head",
    "Minor Head"         : "minor_head",
    "Sub Head"           : "sub_head",
    "Amount"             : "amount",
    "Gross Amount"       : "amount",
    "Net Amount"         : "net_amount",
    "Payment Date"       : "payment_date",
    "Date"               : "payment_date",
    "Voucher No"         : "voucher_no",
    "Voucher Number"     : "voucher_no",
    "Bill No"            : "bill_no",
    "Employee Name"      : "employee_name",
    "Bank Account No"    : "bank_account_no",
    "IFSC"               : "ifsc_code",
    "Bank"               : "bank_name",
}

# Columns that should contain numeric (float) values.
# The transformer will attempt to parse these regardless of column order.
NUMERIC_COLUMNS: list[str] = [
    "amount",
    "net_amount",
    "gross_amount",
]

# Columns that should be parsed as dates.
DATE_COLUMNS: list[str] = [
    "payment_date",
]

# The sentinel column added to every output DataFrame.
EXTRACTION_ENGINE_COLUMN = "_extraction_engine"


# ===========================================================================
# PRIMARY PUBLIC FUNCTION
# ===========================================================================

def normalize(
    raw_rows: list[dict],
    engine_tag: str = "unknown",
    drop_fully_empty: bool = True,
    drop_duplicates: bool = True,
) -> pd.DataFrame:
    """
    Convert the list-of-dicts from the extraction engine into a clean,
    typed Pandas DataFrame.

    Parameters
    ----------
    raw_rows         : output of engine.extract_via_dom() or
                       engine.extract_via_excel_stream()
    engine_tag       : "dom" or "stream" – stored as a metadata column
    drop_fully_empty : if True, remove rows where every column is NaN/empty
    drop_duplicates  : if True, remove exact duplicate rows

    Returns
    -------
    pd.DataFrame with:
      - Renamed, normalised column names
      - Numeric columns as float64
      - Date columns as datetime64[ns]
      - Boolean _extraction_engine metadata column
    """
    logger.info(
        "[TRANSFORMER] Normalizing %d raw rows (engine=%s).",
        len(raw_rows), engine_tag,
    )

    # ------------------------------------------------------------------
    # STEP 1 – Build the initial DataFrame from the list of dicts.
    #
    # pd.DataFrame(list_of_dicts) creates a matrix where:
    #   • Each dict key  becomes a column header.
    #   • Each dict      becomes one row.
    #
    # If some dicts have missing keys, Pandas fills those cells with NaN
    # (Not a Number – the standard missing-value sentinel in NumPy).
    # ------------------------------------------------------------------
    if not raw_rows:
        logger.warning("[TRANSFORMER] No rows to transform; returning empty DataFrame.")
        return pd.DataFrame()

    df = pd.DataFrame(raw_rows)
    logger.debug("[TRANSFORMER] Initial shape: %s", df.shape)  # (rows, cols)

    # ------------------------------------------------------------------
    # STEP 2 – Strip leading/trailing whitespace from ALL string columns.
    #
    # .applymap (deprecated in newer Pandas → use .map on each column) OR
    # we use the vectorised approach below: select object-dtype columns and
    # apply str.strip() to every cell in one pass.
    #
    # Why not a nested for-loop?
    # Pandas routes `.str.strip()` through NumPy C-code which processes
    # the entire column as a contiguous memory block – orders of magnitude
    # faster than Python-level iteration for large tables.
    # ------------------------------------------------------------------
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())
    logger.debug("[TRANSFORMER] Whitespace stripped from %d string columns.", len(str_cols))

    # ------------------------------------------------------------------
    # STEP 3 – Rename columns using the canonical mapping.
    #
    # We first normalise each existing column header with _normalise_header()
    # so minor portal-side inconsistencies (extra spaces, mixed case) are
    # resolved before we try a lookup in COLUMN_RENAME_MAP.
    # ------------------------------------------------------------------
    df = _rename_columns(df)

    # ------------------------------------------------------------------
    # STEP 4 – Drop rows that are entirely empty (all NaN or empty string).
    #
    # How it works:
    #   df.replace("", np.nan)   → turn empty strings into NaN first
    #   df.dropna(how="all")     → remove any row where EVERY cell is NaN
    #
    # `inplace=True` mutates the DataFrame directly instead of returning a
    # new copy – a small memory optimisation for large tables.
    # ------------------------------------------------------------------
    if drop_fully_empty:
        before = len(df)
        df.replace("", np.nan, inplace=True)
        df.dropna(how="all", inplace=True)
        after = len(df)
        if before != after:
            logger.debug("[TRANSFORMER] Dropped %d fully-empty rows.", before - after)

    # Reset the integer index after dropping rows so it runs 0, 1, 2 …
    # without gaps.  `drop=True` discards the old index column.
    df.reset_index(drop=True, inplace=True)

    # ------------------------------------------------------------------
    # STEP 5 – Parse numeric columns.
    # Cells may contain strings like "₹ 12,345.67" or "12345" or "-".
    # We use a regex cleaner before pd.to_numeric so Pandas doesn't choke.
    # ------------------------------------------------------------------
    df = _coerce_numeric_columns(df)

    # ------------------------------------------------------------------
    # STEP 6 – Parse date columns.
    # `pd.to_datetime` handles many formats (dd/mm/yyyy, yyyy-mm-dd, etc.)
    # automatically.  `errors="coerce"` turns unparseable strings into NaT
    # (Not a Time – the datetime equivalent of NaN) rather than raising.
    # ------------------------------------------------------------------
    df = _coerce_date_columns(df)

    # ------------------------------------------------------------------
    # STEP 7 – Normalise the DDO code column if it exists.
    # The architecture doc §5 requires canonicalisation:
    #   "sbin000377 " → "SBIN000377"
    # ------------------------------------------------------------------
    if "ddo_code" in df.columns:
        df["ddo_code"] = df["ddo_code"].str.upper().str.strip()
        logger.debug("[TRANSFORMER] DDO codes canonicalised to UPPER CASE.")

    if "ifsc_code" in df.columns:
        df["ifsc_code"] = df["ifsc_code"].str.upper().str.strip()

    # ------------------------------------------------------------------
    # STEP 8 – Remove exact duplicate rows.
    #
    # `keep="first"` keeps the first occurrence and drops all subsequent
    # copies.  This protects against pagination overlap where the portal
    # returns the last row of page N as the first row of page N+1.
    # ------------------------------------------------------------------
    if drop_duplicates:
        before = len(df)
        df.drop_duplicates(keep="first", inplace=True)
        df.reset_index(drop=True, inplace=True)
        dropped = before - len(df)
        if dropped:
            logger.info("[TRANSFORMER] Removed %d duplicate rows.", dropped)

    # ------------------------------------------------------------------
    # STEP 9 – Attach metadata sentinel column.
    # This column stores the engine tag as a Pandas Categorical dtype
    # (memory-efficient for low-cardinality string columns).
    # ------------------------------------------------------------------
    df[EXTRACTION_ENGINE_COLUMN] = pd.Categorical(
        [engine_tag] * len(df),
        categories=["dom", "stream", "unknown"],
    )

    logger.info(
        "[TRANSFORMER] Normalisation complete. Final shape: %s (rows × cols).",
        df.shape,
    )
    return df


# ===========================================================================
# INTERNAL HELPER FUNCTIONS
# ===========================================================================

def _normalise_header(raw: str) -> str:
    """
    Lightly clean a column header string so minor portal variants resolve
    to the same lookup key in COLUMN_RENAME_MAP.

    Steps:
      1. Strip surrounding whitespace.
      2. Collapse multiple internal spaces to a single space.
      3. Title-case the result ("ddo code" → "Ddo Code").

    The title-casing step is intentionally limited; the COLUMN_RENAME_MAP
    already stores title-cased keys for exact-match lookups.
    """
    cleaned = raw.strip()
    cleaned = re.sub(r"\s+", " ", cleaned)   # collapse runs of whitespace
    return cleaned


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename DataFrame columns using COLUMN_RENAME_MAP.

    For any column not found in the map, we fall through to a generic
    snake_case conversion so the output is still consistent even for
    unexpected columns the portal might add in the future.
    """
    new_names: dict[str, str] = {}

    for col in df.columns:
        normalised = _normalise_header(str(col))

        if normalised in COLUMN_RENAME_MAP:
            new_names[col] = COLUMN_RENAME_MAP[normalised]
        else:
            # Generic fallback: lowercase + replace spaces/hyphens with _
            snake = re.sub(r"[\s\-\.]+", "_", normalised.lower())
            snake = re.sub(r"[^\w]", "", snake)   # remove non-word chars
            new_names[col] = snake

    df.rename(columns=new_names, inplace=True)
    logger.debug("[TRANSFORMER] Column rename map applied: %s", new_names)
    return df


def _clean_numeric_string(value) -> Optional[float]:
    """
    Convert a messy numeric string to a Python float, or return NaN.

    Examples of inputs this handles:
      "₹ 12,345.67"   →  12345.67
      "12345"         →  12345.0
      "  -  "         →  NaN         (dash = missing value convention)
      ""              →  NaN
      123.45          →  123.45      (already numeric, pass-through)

    This is a scalar function (operates on a SINGLE cell value), applied
    across an entire column via .apply() in _coerce_numeric_columns().
    """
    if pd.isna(value):
        return np.nan

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()

    # Treat common "missing value" conventions as NaN.
    if text in ("", "-", "–", "N/A", "NA", "n/a", "nil", "Nil"):
        return np.nan

    # Remove currency symbols, commas, and spaces.
    text = re.sub(r"[₹$€£,\s]", "", text)

    try:
        return float(text)
    except ValueError:
        logger.debug("[TRANSFORMER] Could not parse numeric value: %r", value)
        return np.nan


def _coerce_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    For every column name in NUMERIC_COLUMNS that exists in the DataFrame,
    apply _clean_numeric_string() across the whole column and cast to float64.

    VECTORISED APPLICATION WITH .apply()
    -------------------------------------
    df["amount"].apply(_clean_numeric_string)

    This is equivalent to:
        for i, cell in enumerate(df["amount"]):
            df.at[i, "amount"] = _clean_numeric_string(cell)

    But the `.apply()` version:
      a) Runs at C speed inside Pandas internals.
      b) Returns a brand-new Series (immutable-style) rather than mutating
         the original in a loop (avoids the SettingWithCopyWarning).

    After .apply(), we cast the Series to float64 explicitly.  Some cells
    may have become np.nan (float); casting to float64 ensures the whole
    column dtype is consistent.
    """
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(_clean_numeric_string).astype("float64")
            logger.debug("[TRANSFORMER] Column '%s' coerced to float64.", col)
    return df


def _coerce_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse date-string columns into pandas datetime64[ns] dtype.

    pd.to_datetime() tries dozens of common date formats automatically.
    We pass `dayfirst=True` because the Indian date convention is dd/mm/yyyy.
    `errors="coerce"` converts unparseable cells to NaT instead of crashing.
    """
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col],
                dayfirst=True,    # treat "01/02/2024" as 1 Feb, not 2 Jan
                errors="coerce",  # bad strings → NaT (Not a Time)
            )
            nat_count = df[col].isna().sum()
            if nat_count:
                logger.warning(
                    "[TRANSFORMER] Column '%s' has %d unparseable date cells (set to NaT).",
                    col, nat_count,
                )
            else:
                logger.debug("[TRANSFORMER] Column '%s' parsed to datetime64.", col)
    return df


# ===========================================================================
# CONVENIENCE MERGE FUNCTION
# ===========================================================================

def merge_monthly_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Concatenate a list of per-month DataFrames (as produced by the full
    year extraction loop) into a single master DataFrame.

    Used in Phase 2 when iterating over 12 months:

        all_frames = []
        for month in range(1, 13):
            rows, tag = await run_extraction(year, month)
            df = normalize(rows, engine_tag=tag)
            all_frames.append(df)

        master_df = merge_monthly_frames(all_frames)

    MATRIX OPERATIONS NOTE FOR algoRoute
    --------------------------------------
    pd.concat() is essentially a matrix row-stack (vertical concatenation):

        Frame A  (shape 120 × 15)
        Frame B  (shape 95  × 15)
        Frame C  (shape 130 × 15)
        ──────────────────────────
        Result   (shape 345 × 15)

    It aligns columns by name, so if Month 3 has an extra column that
    others lack, the missing cells in other frames are filled with NaN –
    no data is silently discarded.

    ignore_index=True produces a clean 0…344 integer index on the result.
    """
    if not frames:
        return pd.DataFrame()

    # Filter out any empty frames to avoid shape mismatches.
    non_empty = [f for f in frames if not f.empty]

    if not non_empty:
        logger.warning("[TRANSFORMER] All monthly frames are empty.")
        return pd.DataFrame()

    master = pd.concat(non_empty, axis=0, ignore_index=True)
    logger.info(
        "[TRANSFORMER] Merged %d monthly frames → master shape: %s",
        len(non_empty), master.shape,
    )
    return master


# ===========================================================================
# QUICK LOCAL TEST  (run with:  python -m src.processing.transformer)
# ===========================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )

    _sample_raw = [
        {
            "Sr No": "1",
            "DDO Code": " sbin000377 ",
            "DDO Name": "District Treasury Office",
            "Amount": "₹ 1,23,456.78",
            "Payment Date": "15/04/2024",
            "Voucher No": "VCH-001",
        },
        {
            "Sr No": "2",
            "DDO Code": "SBIN000378",
            "DDO Name": "Sub Treasury",
            "Amount": "-",           # missing value
            "Payment Date": "not-a-date",
            "Voucher No": "VCH-002",
        },
        {
            "Sr No": "3",
            "DDO Code": "SBIN000379",
            "DDO Name": "Pay Office",
            "Amount": "78000",
            "Payment Date": "2024-04-30",
            "Voucher No": "VCH-003",
        },
        # Exact duplicate of row 1 – should be dropped.
        {
            "Sr No": "1",
            "DDO Code": " sbin000377 ",
            "DDO Name": "District Treasury Office",
            "Amount": "₹ 1,23,456.78",
            "Payment Date": "15/04/2024",
            "Voucher No": "VCH-001",
        },
    ]

    result_df = normalize(_sample_raw, engine_tag="dom")
    print("\n=== Normalised DataFrame ===")
    print(result_df.to_string())
    print("\n=== dtypes ===")
    print(result_df.dtypes)
