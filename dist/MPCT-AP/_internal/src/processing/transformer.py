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
    "Sr No"              : "serial_no",
    "Sr. No."            : "serial_no",
    "S.No"               : "serial_no",
    "DDO"                : "ddo_code",
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
    "Voucher Date"       : "payment_date",
    "Payment Date"       : "payment_date",
    "Date"               : "payment_date",
    "Cheque No"          : "cheque_no",
    "Voucher No"         : "voucher_no",
    "Voucher Number"     : "voucher_no",
    "Bill No"            : "bill_no",
    "Party Name"         : "party_name",
    "UTR No"             : "utr_number",
    "Status"             : "transaction_status",
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

# ---------------------------------------------------------------------------
# STRICT OUTPUT SCHEMA
# ---------------------------------------------------------------------------
# The 7 canonical columns from the portal, in display order.
# Any column not in this list (plus the metadata columns below) is dropped.
# This is the contract between the transformer and the final Excel export.
# ---------------------------------------------------------------------------
CORE_COLUMNS: list[str] = [
    "ddo_code",
    "payment_date",
    "cheque_no",
    "amount",
    "party_name",
    "utr_number",
    "transaction_status",
]

# Optional columns appended by the Supervisor (year / month context).
METADATA_COLUMNS: list[str] = [
    "year",
    "month",
    EXTRACTION_ENGINE_COLUMN,
]

# ---------------------------------------------------------------------------
# UI GARBAGE PATTERNS
# ---------------------------------------------------------------------------
# Case-insensitive substrings that only appear in portal chrome / layout
# cells — never in real DDO disbursement data.
# When ANY of these is found anywhere in a row's text the row is discarded.
# ---------------------------------------------------------------------------
_UI_KEYWORDS: tuple[str, ...] = (
    "print",
    "export",
    "pages :",
    "displaying",
    "total visits",
    "thank you for visiting",
    "terms of use",
    "this website was last updated",
    "server name:",
    "logged in user:",
    "report generated time:",
    "account no::",
    "ifsc code:",
    "year(yyyy)::",
    "month(mm)::",
    "e-payment status",
    "sitemap",
    "feedback",
    "sign in",
    "hindi",
    "theme",
    "home\\n",
    "about us\\n",
    "circulars",
    "citizen charter",
    "codes/rules",
    "version :",
    "copyright",
    "designed, developed",
    "screen resolution",
    "production environment",
    "a a a",
)


# ---------------------------------------------------------------------------
# PRE-NORMALIZATION RAW ROW RESCUE
# ---------------------------------------------------------------------------
# Root-cause defence: if engine.py ever returns rows where a single cell
# contains the entire page text as a multi-line/tab-separated blob (the
# symptom seen when the layout table is scraped instead of the data table),
# this function surgically extracts only the real transaction rows before
# the DataFrame is even constructed.
#
# Detection heuristic: if any value in a raw dict is a multi-line string
# that itself contains tab-separated DDO/Amount/UTR content, we split it
# into proper row dicts using the embedded TSV structure.
# ---------------------------------------------------------------------------

_BLOB_EXPECTED_HEADERS = {
    "ddo", "voucher date", "cheque no", "amount", "party name", "utr no", "status"
}


def _rescue_blob_rows(raw_rows: list[dict]) -> list[dict]:
    """
    If the engine accidentally scraped the entire page as a text blob inside
    a single cell (because it matched the layout table instead of the data
    table), parse out the real transaction rows from the embedded TSV content.

    If the rows look normal (each dict has 5-9 reasonable-length string values
    that aren't multi-line page dumps), they pass through unchanged.
    """
    rescued: list[dict] = []

    for row in raw_rows:
        values = list(row.values())
        # Detect blob: any value is a multi-line string AND contains the
        # canonical header words (meaning the whole page was crammed in).
        blob_value = None
        for v in values:
            if isinstance(v, str) and "\n" in v and "Amount" in v and "DDO" in v:
                blob_value = v
                break

        if blob_value is None:
            # Normal row — pass through unchanged.
            rescued.append(row)
            continue

        # Parse the embedded TSV inside the blob.
        lines = [ln.strip() for ln in blob_value.splitlines()]
        headers: list[str] = []
        for line in lines:
            if not line:
                continue
            parts = [p.strip() for p in line.split("\t")]
            # Identify the header row by checking if its parts match portal headers.
            if not headers:
                lower_parts = {p.lower() for p in parts}
                if lower_parts & _BLOB_EXPECTED_HEADERS:
                    headers = parts
                    continue
            else:
                # Data row: must have at least as many columns as headers.
                if len(parts) >= len(headers):
                    row_dict = dict(zip(headers, parts))
                    rescued.append(row_dict)
                    logger.debug(
                        "[TRANSFORMER] Rescued blob row: %s", row_dict
                    )

    if len(rescued) != len(raw_rows):
        logger.warning(
            "[TRANSFORMER] _rescue_blob_rows: input had %d rows, output has %d "
            "(blob rows were unpacked into individual transaction rows).",
            len(raw_rows), len(rescued),
        )
    return rescued


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

    # ------------------------------------------------------------------
    # STEP 0 – Pre-normalization blob rescue.
    # If the extraction engine accidentally scraped a layout table and
    # returned the entire page as a multi-line blob in one cell, extract
    # the real transaction rows from the embedded TSV before we build the
    # DataFrame. Normal rows pass through this function unchanged.
    # ------------------------------------------------------------------
    raw_rows = _rescue_blob_rows(raw_rows)
    if not raw_rows:
        logger.warning("[TRANSFORMER] No valid rows survived blob rescue; returning empty DataFrame.")
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
    # STEP 8 – RUTHLESS ROW FILTERING (UI garbage elimination).
    #
    # The portal uses nested <table> elements for page layout, so the DOM
    # engine may scrape navigation bars, pagination controls, footer text,
    # and report-header metadata alongside real data rows.
    #
    # We apply THREE independent filters — a row must PASS ALL THREE to
    # survive into the final DataFrame.
    #
    # Filter A – amount must be a valid, non-zero float.
    #   Any row without a parseable numeric amount is layout / header noise.
    #
    # Filter B – payment_date must have resolved to a real datetime.
    #   NaT means the cell was blank, a label string, or unparseable.
    #
    # Filter C – no UI keyword anywhere in the row.
    #   Checks a concatenated string of ALL cell values so keywords buried
    #   in any column (not just ddo_code) are caught.
    # ------------------------------------------------------------------
    before_filter = len(df)

    # Filter A: valid numeric amount
    if "amount" in df.columns:
        df = df[df["amount"].notna() & (df["amount"] != 0.0)]
        df.reset_index(drop=True, inplace=True)
        logger.debug(
            "[TRANSFORMER] After amount filter: %d rows remain (dropped %d).",
            len(df), before_filter - len(df),
        )

    # Filter B: valid (non-NaT) payment date
    if "payment_date" in df.columns:
        before_b = len(df)
        df = df[df["payment_date"].notna()]
        df.reset_index(drop=True, inplace=True)
        logger.debug(
            "[TRANSFORMER] After date filter: %d rows remain (dropped %d).",
            len(df), before_b - len(df),
        )

    # Filter C: no UI/chrome keywords present anywhere in the row
    if not df.empty:
        before_c = len(df)
        # Build a single lower-case string from all cells in the row.
        row_text = df.astype(str).apply(
            lambda row: " ".join(row.values), axis=1
        ).str.lower()
        ui_mask = row_text.apply(
            lambda text: any(kw in text for kw in _UI_KEYWORDS)
        )
        df = df[~ui_mask]
        df.reset_index(drop=True, inplace=True)
        logger.debug(
            "[TRANSFORMER] After UI-keyword filter: %d rows remain (dropped %d).",
            len(df), before_c - len(df),
        )

    total_dropped = before_filter - len(df)
    if total_dropped:
        logger.info(
            "[TRANSFORMER] Row filtering removed %d garbage rows; %d data rows remain.",
            total_dropped, len(df),
        )

    # ------------------------------------------------------------------
    # STEP 9 – Remove exact duplicate rows.
    # ------------------------------------------------------------------
    if drop_duplicates:
        before = len(df)
        df.drop_duplicates(keep="first", inplace=True)
        df.reset_index(drop=True, inplace=True)
        dropped = before - len(df)
        if dropped:
            logger.info("[TRANSFORMER] Removed %d duplicate rows.", dropped)

    # ------------------------------------------------------------------
    # STEP 11 – Enforce strict output schema.
    #
    # Keep ONLY the 7 core columns + any optional metadata columns that
    # actually exist in the DataFrame (year/month if the Supervisor added
    # them; _extraction_engine which we just attached).
    #
    # Any column the portal adds unexpectedly, or any residual junk column
    # the DOM scraper picked up, is silently discarded here.
    # ------------------------------------------------------------------
    allowed_columns = CORE_COLUMNS + METADATA_COLUMNS
    final_columns = [c for c in allowed_columns if c in df.columns]
    df = df[final_columns]

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
            rows, tag = await run_extraction(ifsc, account_no, year, month)
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
