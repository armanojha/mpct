"""
src/automation/engine.py
========================
Phase 1 – Core Extraction Engine (Local Pipeline)
Team: algoRoute | Project: MPCT-AP

PURPOSE
-------
This module is the heart of the entire automation system.
It contains TWO extraction strategies that mirror each other's goal
(get disbursement rows) but use completely different techniques:

  Primary Engine   → Parse the HTML table that Playwright renders in the
                     browser's DOM (the visible webpage).
  Secondary Engine → Intercept the raw binary (.xls/.xlsx) file the portal
                     downloads when the user clicks "Export", and parse that
                     stream directly without ever touching the DOM table.

The architecture doc calls this "Dual-Engine Extraction Logic".

WHY TWO ENGINES?
----------------
Web portals change.  If the treasury team redesigns their table HTML
(DOM drift), the primary engine's selectors will break.  The secondary
engine is immune to visual redesigns because it taps the *network response*
before the browser even renders it.  A confidence score (Phase 2) decides
which engine's data to trust.

ASYNC PATTERN – THE BIG PICTURE
---------------------------------
Python's `asyncio` library lets us do many things at once WITHOUT using
multiple CPU threads.  Think of it like a single chef who juggles multiple
pots by checking each pot in turn, never waiting idle.

  async def some_function():   # marks the function as a coroutine
      await something_slow()   # "pause HERE and let other tasks run"

Playwright is built on top of asyncio.  Every browser action (click,
navigate, wait) is a coroutine that you `await`.  This keeps the program
responsive while the remote server loads.

PLAYWRIGHT CONTEXT vs PAGE
---------------------------
  browser  → the whole Chromium process (expensive to create)
  context  → an isolated "incognito window" inside the browser (cheap)
  page     → a single tab inside a context

We create one context per extraction job so cookies/sessions never bleed
between concurrent user requests.
"""

import asyncio
import io
import logging
from typing import Optional

import pandas as pd
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Request,
    Response,
    async_playwright,
)

# ---------------------------------------------------------------------------
# Module-level logger.  The calling code (supervisor / tests) configures the
# root logger format; we just attach to the hierarchy here.
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SELECTORS  (CSS / XPath strings that point to elements on the page)
# ---------------------------------------------------------------------------
# These strings are the PRIMARY maintenance surface.  When the portal's HTML
# changes, only these constants need to be updated – no hunting through code.
#
# CSS selector syntax primer for algoRoute:
#   "table"          → any <table> element
#   "table.report"   → <table class="report …">
#   "table#rptTable" → <table id="rptTable">
#   "tr"             → any <tr> (table row)
#   "td, th"         → any <td> or <th> (cell or header cell)
#
# XPath syntax primer:
#   "//table"                       → any table anywhere in the document
#   "//table[contains(@class,'x')]" → table whose class attribute contains 'x'
# ---------------------------------------------------------------------------

# --- Primary engine (DOM) ---

# The main data grid.  We use a broad selector first so the fallback chain
# (see _find_table_element) can narrow it down at runtime.
TABLE_CSS_BROAD      = "table"
TABLE_CSS_REPORT     = "table.report-table"
TABLE_CSS_ID         = "table#tblReport"
TABLE_XPATH_FALLBACK = "//table[contains(@class,'tbl') or contains(@id,'Report')]"

# Individual row and cell selectors, scoped INSIDE the table element.
ROW_SELECTOR    = "tr"
HEADER_SELECTOR = "th"
CELL_SELECTOR   = "td"

# The portal's navigation / filter form elements.
# Adjust these if the portal uses different names or IDs.
YEAR_DROPDOWN_SELECTOR  = "select#ddlYear, select[name='ddlYear']"
MONTH_DROPDOWN_SELECTOR = "select#ddlMonth, select[name='ddlMonth']"
IFSC_INPUT_SELECTOR     = "input#ifscCode, input[name*='ifsc']"
ACCOUNT_INPUT_SELECTOR  = "input#accountNo, input[name*='account']"
SUBMIT_BUTTON_SELECTOR = (
    "input[value='Generate Report'], "
    "button:has-text('Generate Report'), "
    "input[type='submit']"
)
EXPORT_BUTTON_SELECTOR = (
    "img[src*='excel'], "
    "a:has-text('Excel'), "
    "input[title*='Excel']"
)

# --- Loading / readiness signals ---
# We wait for this selector to appear before we trust the DOM is fully painted.
TABLE_LOADED_SELECTOR = f"{TABLE_CSS_REPORT}, {TABLE_CSS_ID}, {TABLE_CSS_BROAD}"

# MIME types we treat as Excel binary streams.
EXCEL_MIME_TYPES = {
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/octet-stream",   # portal sometimes serves xlsx with this type
}

# Portal entry point.
PORTAL_BASE_URL = "https://mptreasury.gov.in/MPCTP/portal.htm?viewName=myEPaymentStatusReport&registered=N"

# How long (milliseconds) to wait for a selector before raising a timeout.
NAV_TIMEOUT_MS    = 60_000   # 60 s  – page navigation
ELEMENT_TIMEOUT_MS = 30_000  # 30 s  – individual element appearance


# ===========================================================================
# PRIMARY ENGINE  –  DOM Extraction
# ===========================================================================

async def extract_via_dom(
    page: Page,
    ifsc: str,
    account_no: str,
    year: int,
    month: int,
) -> list[dict]:
    """
    PRIMARY ENGINE: Navigate the portal, submit the filter form, and parse
    the resulting HTML table row-by-row from the live DOM.

    Parameters
    ----------
    page  : an already-open Playwright Page (inside an isolated context)
    ifsc       : bank IFSC code used by the portal filter
    account_no : bank account number used by the portal filter
    year       : 4-digit fiscal year (e.g. 2024)
    month      : 1-based month number (1 = April for Indian fiscal calendar)

    Returns
    -------
    List of dicts, one per data row.  Keys are the column headers scraped
    from <th> cells; values are the text content of each <td> cell.

    Raises
    ------
    ExtractionError if the table cannot be found after all selector fallbacks.
    """
    logger.info("[DOM] Starting DOM extraction – year=%s month=%s", year, month)

    # ------------------------------------------------------------------
    # STEP 1 – Navigate to the portal landing page.
    # `await` pauses this coroutine until the browser reports the page has
    # finished loading ("load" event fired by the browser).
    # ------------------------------------------------------------------
    await page.goto(PORTAL_BASE_URL, wait_until="load", timeout=NAV_TIMEOUT_MS)
    logger.debug("[DOM] Portal landing page loaded.")

    # ------------------------------------------------------------------
    # STEP 2 – Fill the filter form (year + month dropdowns).
    # `page.select_option` finds the <select> element by our CSS selector
    # and chooses the <option> whose `value` attribute matches the string.
    # ------------------------------------------------------------------
    await page.fill(IFSC_INPUT_SELECTOR, ifsc)
    logger.debug("[DOM] IFSC field filled.")

    await page.fill(ACCOUNT_INPUT_SELECTOR, account_no)
    logger.debug("[DOM] Account number field filled.")

    await page.select_option(YEAR_DROPDOWN_SELECTOR, str(year))
    logger.debug("[DOM] Year dropdown set to %s", year)

    await page.select_option(MONTH_DROPDOWN_SELECTOR, str(month))
    logger.debug("[DOM] Month dropdown set to %s", month)

    # ------------------------------------------------------------------
    # STEP 3 – Submit the form and wait for the results table to appear.
    # `page.click` fires a synthetic mouse click on the button element.
    # `page.wait_for_selector` then BLOCKS (asynchronously) until the CSS
    # selector matches a visible element – this is how we know the AJAX
    # response has finished painting the DOM.
    # ------------------------------------------------------------------
    await page.click(SUBMIT_BUTTON_SELECTOR)
    logger.debug("[DOM] Submit button clicked, waiting for table…")

    await page.wait_for_selector(
        TABLE_LOADED_SELECTOR,
        state="visible",
        timeout=ELEMENT_TIMEOUT_MS,
    )
    logger.info("[DOM] Results table appeared in DOM.")

    # ------------------------------------------------------------------
    # STEP 4 – Locate the best <table> element via a selector fallback chain.
    # Web portals sometimes change class names but keep the overall structure.
    # We try the most specific selector first and fall back to broader ones.
    # ------------------------------------------------------------------
    table_element = await _find_table_element(page)

    # ------------------------------------------------------------------
    # STEP 5 – Extract headers from <th> cells inside the first <tr>.
    # `query_selector_all` returns a Python list of ElementHandle objects.
    # We call `.inner_text()` on each handle to get the visible string.
    #
    # List comprehension pattern (CS101 reminder):
    #   [expression  for variable  in iterable]
    # is equivalent to:
    #   result = []
    #   for variable in iterable:
    #       result.append(expression)
    # ------------------------------------------------------------------
    header_cells = await table_element.query_selector_all(
        f"{ROW_SELECTOR}:first-child {HEADER_SELECTOR}"
    )

    # If no <th> found, the portal may use <td> in the first row as headers.
    if not header_cells:
        first_row = await table_element.query_selector(ROW_SELECTOR)
        header_cells = await first_row.query_selector_all(CELL_SELECTOR) if first_row else []

    # Await each handle's text asynchronously and strip surrounding whitespace.
    headers: list[str] = [
        (await cell.inner_text()).strip()
        for cell in header_cells
    ]
    logger.debug("[DOM] Headers detected: %s", headers)

    if not headers:
        raise ExtractionError("DOM table found but no header cells detected.")

    # ------------------------------------------------------------------
    # STEP 6 – Extract every data row (skip the header row).
    # `nth(0)` is the header row; we skip it by slicing `all_rows[1:]`.
    # ------------------------------------------------------------------
    all_rows = await table_element.query_selector_all(ROW_SELECTOR)
    data_rows = all_rows[1:]   # Python slice: everything from index 1 onward

    extracted: list[dict] = []

    for row_handle in data_rows:
        cells = await row_handle.query_selector_all(CELL_SELECTOR)
        cell_texts = [(await cell.inner_text()).strip() for cell in cells]

        # zip() pairs each header with its corresponding cell text.
        # If a row has fewer cells than headers (colspan/rowspan), the extra
        # headers map to an empty string via zip_longest-style padding below.
        if len(cell_texts) < len(headers):
            cell_texts += [""] * (len(headers) - len(cell_texts))

        row_dict = dict(zip(headers, cell_texts))
        extracted.append(row_dict)

    logger.info("[DOM] Extracted %d data rows from DOM.", len(extracted))
    return extracted


async def _find_table_element(page: Page):
    """
    Internal helper: try selectors from most-specific to most-general,
    returning the first ElementHandle that resolves.

    This is a private function (leading underscore convention in Python).
    It is NOT part of the public API of this module.
    """
    selector_priority = [
        TABLE_CSS_ID,           # most specific → exact id match
        TABLE_CSS_REPORT,       # class-based match
        TABLE_XPATH_FALLBACK,   # XPath heuristic
        TABLE_CSS_BROAD,        # last resort: first <table> on the page
    ]

    for selector in selector_priority:
        element = await page.query_selector(selector)
        if element:
            logger.debug("[DOM] Table located via selector: %r", selector)
            return element

    raise ExtractionError(
        "Could not locate data table with any known selector. "
        "The portal DOM may have changed. Update TABLE_CSS_* constants."
    )


# ===========================================================================
# SECONDARY ENGINE  –  Excel Binary Stream Interception
# ===========================================================================

async def extract_via_excel_stream(
    page: Page,
    ifsc: str,
    account_no: str,
    year: int,
    month: int,
) -> list[dict]:
    """
    SECONDARY ENGINE: Intercept the raw Excel binary that the portal serves
    when the user clicks the "Export" button, then parse it with openpyxl /
    xlrd — completely bypassing the rendered HTML table.

    HOW NETWORK INTERCEPTION WORKS IN PLAYWRIGHT
    --------------------------------------------
    When a browser makes an HTTP request, Playwright can insert itself as a
    "listener" on the response event.  This is similar to browser DevTools'
    Network tab, but programmable.

    We register a callback with `page.on("response", handler)`.  Every time
    ANY network response arrives (images, JS, CSS, XHR calls …), our handler
    is called.  Inside the handler we check the MIME type and URL pattern to
    decide if this is the Excel file we want.

    Because the handler may fire BEFORE or AFTER the click resolves, we use
    an `asyncio.Future` as a one-shot "promise":
      • The handler resolves the Future when the Excel response arrives.
      • The main flow `await`s the Future with a timeout.

    asyncio.Future  (simplified mental model for algoRoute)
    --------------------------------------------------------
    Think of a Future as an empty box.
      - future = asyncio.Future()        → creates the empty box
      - future.set_result(value)         → puts `value` in the box
      - result = await asyncio.wait_for(future, timeout=30) → waits until
        something is in the box, then gives you `value`

    Parameters
    ----------
    page  : an already-open Playwright Page
    ifsc       : bank IFSC code used by the portal filter
    account_no : bank account number used by the portal filter
    year       : 4-digit fiscal year
    month      : 1-based month

    Returns
    -------
    List of dicts (same schema as DOM engine output).
    """
    logger.info(
        "[STREAM] Starting Excel stream interception – year=%s month=%s",
        year, month,
    )

    # ------------------------------------------------------------------
    # Navigate and fill the form exactly as the DOM engine does.
    # (DRY – Don't Repeat Yourself – is a valid concern here; in Phase 2
    # we will refactor this into a shared _navigate_and_filter() helper.)
    # ------------------------------------------------------------------
    await page.goto(PORTAL_BASE_URL, wait_until="load", timeout=NAV_TIMEOUT_MS)
    await page.fill(IFSC_INPUT_SELECTOR, ifsc)
    await page.fill(ACCOUNT_INPUT_SELECTOR, account_no)
    await page.select_option(YEAR_DROPDOWN_SELECTOR, str(year))
    await page.select_option(MONTH_DROPDOWN_SELECTOR, str(month))
    await page.click(SUBMIT_BUTTON_SELECTOR)
    await page.wait_for_selector(TABLE_LOADED_SELECTOR, state="visible", timeout=ELEMENT_TIMEOUT_MS)

    # ------------------------------------------------------------------
    # Set up the Future that the response handler will resolve.
    # `asyncio.get_event_loop()` returns the currently running event loop –
    # the engine that coordinates all our `await` operations.
    # ------------------------------------------------------------------
    loop = asyncio.get_running_loop()
    excel_future: asyncio.Future = loop.create_future()

    async def _on_response(response: Response) -> None:
        """
        This callback fires for EVERY network response the page receives.
        We check two conditions to identify the Excel export:
          1. The Content-Type header is one of the Excel MIME types.
          2. The URL contains keywords typical of export endpoints.

        If both match and the Future is still empty (not yet resolved),
        we fetch the raw bytes and put them in the Future.
        """
        # Avoid processing the same response twice if the event fires
        # multiple times (Playwright edge-case on redirected responses).
        if excel_future.done():
            return

        content_type: str = response.headers.get("content-type", "").lower()
        url: str = response.url.lower()

        is_excel_mime = any(mime in content_type for mime in EXCEL_MIME_TYPES)
        is_export_url = any(
            keyword in url
            for keyword in ("export", "excel", "download", "xls")
        )

        if is_excel_mime or is_export_url:
            logger.info("[STREAM] Excel response intercepted: %s", response.url)
            try:
                # `response.body()` fetches the complete binary payload.
                raw_bytes: bytes = await response.body()
                excel_future.set_result(raw_bytes)
            except Exception as exc:  # noqa: BLE001
                excel_future.set_exception(exc)

    # ------------------------------------------------------------------
    # Register the listener BEFORE clicking Export so we don't miss it.
    # Playwright calls `_on_response` synchronously in the event loop
    # each time a response arrives.
    # ------------------------------------------------------------------
    page.on("response", _on_response)

    try:
        logger.debug("[STREAM] Clicking Export button…")
        await page.click(EXPORT_BUTTON_SELECTOR)

        # Wait up to 45 seconds for the Excel Future to be resolved.
        # `asyncio.wait_for` raises `asyncio.TimeoutError` if time runs out.
        raw_excel_bytes: bytes = await asyncio.wait_for(
            excel_future, timeout=45.0
        )

    except asyncio.TimeoutError:
        raise ExtractionError(
            "Excel binary stream was not intercepted within 45 seconds. "
            "The Export button selector may be wrong, or the portal didn't "
            "serve an Excel file."
        )
    finally:
        # Always remove the listener, even if something went wrong.
        # Leaving dangling listeners causes memory leaks in long-lived contexts.
        page.remove_listener("response", _on_response)

    # ------------------------------------------------------------------
    # Parse the raw bytes into a Pandas DataFrame.
    # `io.BytesIO` wraps bytes in a file-like object so pandas can read it
    # as if it were an open file on disk – no temp file needed.
    # ------------------------------------------------------------------
    logger.debug("[STREAM] Parsing %d bytes of Excel data…", len(raw_excel_bytes))

    try:
        df: pd.DataFrame = pd.read_excel(
            io.BytesIO(raw_excel_bytes),
            sheet_index=0,      # first sheet
            header=0,           # first row contains column names
            engine="openpyxl",  # handles both .xls and .xlsx
        )
    except Exception as exc:
        raise ExtractionError(f"Failed to parse Excel binary: {exc}") from exc

    # Convert the DataFrame to a list of row-dicts so both engines return
    # the same data structure (contract for the transformer module).
    rows: list[dict] = df.to_dict(orient="records")
    logger.info("[STREAM] Parsed %d rows from Excel stream.", len(rows))
    return rows


# ===========================================================================
# DUAL-ENGINE ORCHESTRATOR
# ===========================================================================

async def run_extraction(
    ifsc: str,
    account_no: str,
    year: int,
    month: int,
    headless: bool = True,
    prefer_stream: bool = False,
) -> tuple[list[dict], str]:
    """
    Public entry point called by the Supervisor (Phase 3) or directly in
    Phase 1 local testing.

    Spins up a full Playwright browser + isolated context, runs the chosen
    primary engine, and returns the raw row data along with a tag indicating
    which engine produced it.

    Parameters
    ----------
    ifsc          : bank IFSC code used by the portal filter
    account_no    : bank account number used by the portal filter
    year          : 4-digit year
    month         : 1-based month
    headless      : run Chromium without a visible window (True for servers)
    prefer_stream : if True, try the Excel stream engine first (useful for
                    debugging the secondary path independently)

    Returns
    -------
    (rows, engine_tag)
      rows       : list of row dicts
      engine_tag : "dom" or "stream" – tells the transformer which engine ran
    """
    async with async_playwright() as pw:
        # `async with` is a context manager that guarantees cleanup.
        # Even if an exception is raised mid-way, Playwright will close
        # the browser when the block exits.

        browser: Browser = await pw.chromium.launch(headless=headless)

        # An isolated context means no cookies or local storage from
        # previous runs will interfere with this job.
        context: BrowserContext = await browser.new_context(
            # Accept XLSX downloads without a save-dialog.
            accept_downloads=True,
            # Spoof a realistic User-Agent to avoid bot detection.
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page: Page = await context.new_page()

        try:
            if prefer_stream:
                # Try stream first; fall back to DOM on failure.
                try:
                    rows = await extract_via_excel_stream(page, ifsc, account_no, year, month)
                    return rows, "stream"
                except ExtractionError as exc:
                    logger.warning(
                        "[ORCHESTRATOR] Stream engine failed (%s); falling back to DOM.", exc
                    )
                    rows = await extract_via_dom(page, ifsc, account_no, year, month)
                    return rows, "dom"
            else:
                # Default: DOM first, stream as fallback.
                try:
                    rows = await extract_via_dom(page, ifsc, account_no, year, month)
                    return rows, "dom"
                except ExtractionError as exc:
                    logger.warning(
                        "[ORCHESTRATOR] DOM engine failed (%s); falling back to stream.", exc
                    )
                    rows = await extract_via_excel_stream(page, ifsc, account_no, year, month)
                    return rows, "stream"

        finally:
            # Teardown in reverse order: page → context → browser.
            # The `async with` above handles browser.close() automatically,
            # but we explicitly close the context to flush any pending I/O.
            await context.close()


# ===========================================================================
# CUSTOM EXCEPTION
# ===========================================================================

class ExtractionError(RuntimeError):
    """
    Raised when neither extraction engine can produce usable data.
    Inheriting from RuntimeError (rather than bare Exception) lets callers
    catch only extraction-related failures with a specific except clause.
    """


# ===========================================================================
# QUICK LOCAL TEST  (run with:  python -m src.automation.engine)
# ===========================================================================

if __name__ == "__main__":
    import pprint

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )

    async def _smoke_test():
        rows, engine = await run_extraction(
            ifsc="",
            account_no="",
            year=2024,
            month=1,
            headless=False,
        )
        print(f"\n✓ Engine used: {engine}")
        print(f"✓ Rows returned: {len(rows)}")
        if rows:
            print("✓ First row sample:")
            pprint.pprint(rows[0])

    asyncio.run(_smoke_test())
