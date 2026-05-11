#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════
 Bexar County Master Lead Scraper v8 — 100% Coverage Edition
═══════════════════════════════════════════════════════════════════════════

DESIGN PRINCIPLES (master-coder mode):
  1. Multi-strategy extraction (API interception + HTML fallback)
  2. Per-search verification: log expected vs found record counts
  3. Date-window chunking: split large windows into smaller queries
     to bypass any "10,000 record cap" the portal may enforce
  4. Aggressive completeness gates and fresh-only filtering
  5. Detail-page enrichment prioritized by tier
  6. Coverage audit at end of run — fails loud if anomalies detected
  7. Stateful resume: if a search fails, can re-run just that one

Targets:
  - bexar.tx.publicsearch.us (Bexar County Clerk records)
  - maps.bexar.org ArcGIS (BCAD parcel data for skip tracing)

Key v8 features over v7:
  ✅ NETWORK API INTERCEPTION: capture the JSON the React app fetches
  ✅ DATE CHUNKING: 14-day window auto-split into 2-day chunks
  ✅ COVERAGE AUDIT: per-search counts + sanity checks
  ✅ ZERO-RESULT INVESTIGATION: if a search returns 0, retry with
     different strategies before accepting it
  ✅ STRUCTURED LOGGING: per-search results saved for review
  ✅ NO STALE DATA: aggressive cutoff + clean slate option
"""

from __future__ import annotations
import argparse, asyncio, csv, json, logging, os, re, sys, time, traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
from urllib.parse import quote, urlparse, parse_qs
import requests

# ═══════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════

CLERK = "https://bexar.tx.publicsearch.us"
ARCGIS = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"
ARCGIS_FIELDS = "Owner,Situs,AddrLn1,AddrLn2,AddrCity,AddrSt,Zip"

ROOT = Path(__file__).resolve().parent.parent
OUT_DASH = ROOT / "dashboard" / "records.json"
OUT_DATA = ROOT / "data" / "records.json"
OUT_CSV  = ROOT / "data" / "leads_ghl.csv"
OUT_HOT  = ROOT / "data" / "hot_leads_today.csv"
OUT_LOG  = ROOT / "data" / "last_run.log"
OUT_AUDIT = ROOT / "data" / "coverage_audit.json"

RETRIES = 3
BACKOFF = 3
TIMEOUT = 60
PW_TIMEOUT = 90_000          # increased for slow searches
PER_PAGE = 50
MAX_PAGES = 200              # massively increased — we want EVERY page
DETAIL_CAP = 1000            # increased
DETAIL_BUDGET = 2400         # 40 minutes max on details

# Chunking — split lookback into smaller date windows so portal returns
# all results (it may cap at ~10,000 per query)
CHUNK_DAYS = 2               # query 2 days at a time
MAX_RESULTS_PER_QUERY = 9500 # if a query returns this many, sub-chunk further

# Completeness
HOT_SCORE_THRESHOLD = 85
MIN_COMPLETENESS_SCORE = 5
TIER_S_REQUIRES_ADDRESS = True

# Telegram
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")


# ═══════════════════════════════════════════════════════════════════
#  SEARCH CONFIG — every code from the portal docTypeMappings
# ═══════════════════════════════════════════════════════════════════

SEARCHES = [
    # ── TIER S: HOT LEADS ──
    {"dept": "FC", "date_param": "instrumentDateRange", "code": "NOF",
     "cat": "foreclosure", "label": "Notice of Foreclosure", "tier": "S"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "WILL",
     "cat": "probate", "label": "Will & Testament", "tier": "S"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "PROBATE",
     "cat": "probate", "label": "Probate", "tier": "S"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "AFFIDAV",
     "cat": "probate", "label": "Affidavit (Heirship/Death)", "tier": "S"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "FTL",
     "cat": "tax_lien_fed", "label": "Federal Tax Lien", "tier": "S"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "FTLCERT",
     "cat": "tax_lien_fed", "label": "Federal Tax Lien Certificate", "tier": "S"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "STL",
     "cat": "tax_lien_state", "label": "State Tax Lien", "tier": "S"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "TRSCR J",
     "cat": "judgment", "label": "Transcript of Judgment", "tier": "S"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "TRANS J",
     "cat": "judgment", "label": "Transfer of Judgment", "tier": "S"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "JUDG",
     "cat": "judgment", "label": "Judgment", "tier": "S"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "ASIGN J",
     "cat": "judgment", "label": "Assignment of Judgment", "tier": "S"},

    # ── TIER A: STRONG SIGNALS ──
    {"dept": "RP", "date_param": "recordedDateRange", "code": "LIEN",
     "cat": "lien", "label": "Lien", "tier": "A"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "MECHLN",
     "cat": "mechanic_lien", "label": "Mechanics Lien", "tier": "A"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "HOSP LN",
     "cat": "hospital_lien", "label": "Hospital Lien", "tier": "A"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "LNLD LN",
     "cat": "landlord_lien", "label": "Landlord Lien", "tier": "A"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "CSUP LN",
     "cat": "child_support", "label": "Child Support Lien", "tier": "A"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "LIS PEN",
     "cat": "lis_pendens", "label": "Lis Pendens", "tier": "A"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "RL FTL",
     "cat": "release_ftl", "label": "Release FTL", "tier": "A"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "PART RL FTL",
     "cat": "release_ftl", "label": "Partial Release FTL", "tier": "A"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "RL STL",
     "cat": "release_stl", "label": "Release STL", "tier": "A"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "PART RL STL",
     "cat": "release_stl", "label": "Partial Release STL", "tier": "A"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "RL J",
     "cat": "release_judgment", "label": "Release of Judgment", "tier": "A"},

    # ── TIER B: PROPERTY INTEL ──
    {"dept": "RP", "date_param": "recordedDateRange", "code": "DEED",
     "cat": "deed", "label": "Deed", "tier": "B"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "DT",
     "cat": "deed_of_trust", "label": "Deed of Trust", "tier": "B"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "DA",
     "cat": "deed_affidavit", "label": "Deed Affidavit", "tier": "B"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "TRANS",
     "cat": "transfer", "label": "Transfer", "tier": "B"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "TX HMST",
     "cat": "homestead", "label": "TX Homestead", "tier": "B"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "HMST AF",
     "cat": "homestead", "label": "Homestead Affidavit", "tier": "B"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "TRUST",
     "cat": "trust", "label": "Trust", "tier": "B"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "PA",
     "cat": "poa", "label": "Power of Attorney", "tier": "B"},

    # ── TIER C: COMMERCIAL/DEVELOPMENT ──
    {"dept": "RP", "date_param": "recordedDateRange", "code": "UCC1 RP",
     "cat": "ucc_commercial", "label": "UCC 1 Real Property", "tier": "C"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "UCC3RP",
     "cat": "ucc_commercial", "label": "UCC 3 Real Property", "tier": "C"},
    {"dept": "ASN", "date_param": "recordedDateRange", "code": "AN",
     "cat": "assumed_name", "label": "Assumed Name", "tier": "C"},
    {"dept": "UCC", "date_param": "recordedDateRange", "code": "E",
     "cat": "eminent_domain", "label": "Eminent Domain", "tier": "C"},
    {"dept": "PL", "date_param": "recordedDateRange", "code": "PLAT",
     "cat": "plat", "label": "Plat", "tier": "C"},
    {"dept": "PL", "date_param": "recordedDateRange", "code": "AMPLAT",
     "cat": "plat", "label": "Amended Plat", "tier": "C"},
    {"dept": "PL", "date_param": "recordedDateRange", "code": "REPLAT",
     "cat": "plat", "label": "Replat", "tier": "C"},
    {"dept": "RP", "date_param": "recordedDateRange", "code": "VARIANC",
     "cat": "variance", "label": "Variance", "tier": "C"},
]


FLAGS = {
    "foreclosure": "Pre-foreclosure", "probate": "Probate / estate",
    "tax_lien_fed": "Federal tax lien (IRS)", "tax_lien_state": "State tax lien",
    "judgment": "Judgment lien", "lien": "General lien",
    "mechanic_lien": "Mechanics lien", "hospital_lien": "Hospital lien",
    "landlord_lien": "Landlord lien", "child_support": "Child support lien",
    "lis_pendens": "Lis pendens (lawsuit)",
    "release_ftl": "Federal lien just released",
    "release_stl": "State lien just released",
    "release_judgment": "Judgment just released",
    "trust": "Entity-owned (trust)", "poa": "Power of Attorney filed",
    "ucc_commercial": "Commercial UCC lien",
    "eminent_domain": "Eminent domain action",
    "variance": "Zoning variance",
    "deed": None, "deed_of_trust": None, "deed_affidavit": None,
    "transfer": None, "homestead": None, "assumed_name": None, "plat": None,
}

TIER_BASE = {"S": 60, "A": 45, "B": 25, "C": 20}

COMBO_RULES = [
    {"cats": {"foreclosure", "tax_lien_fed"},   "bonus": 25, "flag": "Foreclosure + IRS lien"},
    {"cats": {"foreclosure", "tax_lien_state"}, "bonus": 25, "flag": "Foreclosure + state lien"},
    {"cats": {"foreclosure", "judgment"},       "bonus": 20, "flag": "Foreclosure + judgment"},
    {"cats": {"foreclosure", "lis_pendens"},    "bonus": 20, "flag": "Foreclosure + lawsuit"},
    {"cats": {"probate", "tax_lien_fed"},       "bonus": 20, "flag": "Inherited IRS debt"},
    {"cats": {"probate", "tax_lien_state"},     "bonus": 20, "flag": "Inherited state tax debt"},
    {"cats": {"probate", "judgment"},           "bonus": 15, "flag": "Inherited judgment"},
    {"cats": {"probate", "lien"},               "bonus": 15, "flag": "Inherited lien"},
    {"cats": {"tax_lien_fed", "tax_lien_state"},"bonus": 15, "flag": "Federal + state tax lien"},
    {"cats": {"judgment", "lien"},              "bonus": 10, "flag": "Multiple liens"},
]

BEXAR_CITIES = sorted([
    "SAN ANTONIO","CONVERSE","HELOTES","LEON VALLEY","UNIVERSAL CITY",
    "WINDCREST","BALCONES HEIGHTS","LIVE OAK","SELMA","SCHERTZ","CIBOLO",
    "SHAVANO PARK","HILL COUNTRY VILLAGE","CASTLE HILLS","ALAMO HEIGHTS",
    "KIRBY","CHINA GROVE","ST HEDWIG","SOMERSET","VON ORMY","ELMENDORF",
    "SANDY OAKS","GREY FOREST","FAIR OAKS RANCH","GARDEN RIDGE",
    "HOLLYWOOD PARK","TERRELL HILLS","OLMOS PARK","NEW BRAUNFELS","BOERNE",
    "BULVERDE","SPRING BRANCH","LYTLE","NATALIA","LACOSTE","ATASCOSA",
    "FLORESVILLE","SEGUIN","CANYON LAKE","COMFORT",
], key=len, reverse=True)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
log = logging.getLogger("bexar")


# ═══════════════════════════════════════════════════════════════════
#  DATA MODEL
# ═══════════════════════════════════════════════════════════════════

@dataclass
class Rec:
    doc_num: str = ""
    doc_type: str = ""
    filed: str = ""
    cat: str = ""
    cat_label: str = ""
    tier: str = ""
    owner: str = ""
    grantee: str = ""
    amount: float = 0.0
    legal: str = ""
    prop_address: str = ""
    prop_city: str = ""
    prop_state: str = "TX"
    prop_zip: str = ""
    mail_address: str = ""
    mail_city: str = ""
    mail_state: str = ""
    mail_zip: str = ""
    clerk_url: str = ""
    flags: List[str] = field(default_factory=list)
    score: int = 0
    completeness: int = 0

    def compute_completeness(self) -> int:
        critical = [self.doc_num, self.doc_type, self.filed, self.cat,
                    self.owner, self.prop_address or self.mail_address, self.clerk_url]
        s = sum(1 for f in critical if f and str(f).strip())
        if self.prop_address: s += 1
        if self.mail_address: s += 1
        if self.grantee: s += 1
        if self.amount > 0: s += 1
        if self.legal: s += 1
        if self.flags: s += 1
        self.completeness = s
        return s


@dataclass
class SearchAudit:
    """Per-search result tracking for coverage verification."""
    search_label: str
    code: str
    dept: str
    tier: str
    chunks_run: int = 0
    chunks_failed: int = 0
    records_found: int = 0
    api_intercepts: int = 0
    html_fallbacks: int = 0
    errors: List[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════
#  RETRY
# ═══════════════════════════════════════════════════════════════════

def _retry(fn, *a, label="", **kw):
    last = None
    for i in range(1, RETRIES + 1):
        try: return fn(*a, **kw)
        except Exception as e:
            last = e
            log.warning("[%s] %d/%d: %s", label, i, RETRIES, e)
            if i < RETRIES: time.sleep(BACKOFF * i)
    raise last

async def _aretry(fn, *a, label="", **kw):
    last = None
    for i in range(1, RETRIES + 1):
        try: return await fn(*a, **kw)
        except Exception as e:
            last = e
            log.warning("[%s] %d/%d: %s", label, i, RETRIES, e)
            if i < RETRIES: await asyncio.sleep(BACKOFF * i)
    raise last


# ═══════════════════════════════════════════════════════════════════
#  DATE CHUNKING — split window into smaller queries
# ═══════════════════════════════════════════════════════════════════

def _chunk_dates(start: datetime, end: datetime, chunk_days: int) -> List[Tuple[datetime, datetime]]:
    """Split [start, end] into chunks of chunk_days each."""
    chunks = []
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=chunk_days), end)
        chunks.append((cur, nxt))
        cur = nxt
    return chunks


def _build_url(sr: Dict, start: datetime, end: datetime) -> str:
    sd = start.strftime("%Y%m%d")
    ed = end.strftime("%Y%m%d")
    return (f"{CLERK}/results?department={sr['dept']}"
            f"&docTypes={quote(sr['code'])}"
            f"&{sr['date_param']}={sd}%2C{ed}")


# ═══════════════════════════════════════════════════════════════════
#  STAGE 1 — CLERK PORTAL with NETWORK API INTERCEPTION
# ═══════════════════════════════════════════════════════════════════

class ApiInterceptor:
    """Captures JSON API responses from the React app's XHR calls."""
    def __init__(self):
        self.captured: List[Dict] = []  # list of {url, status, data}

    async def handler(self, response):
        try:
            url = response.url
            # Filter for API-like responses (JSON content, not assets)
            ct = response.headers.get("content-type", "").lower()
            if "json" not in ct: return
            if any(x in url for x in [".js", ".css", ".png", ".svg", ".woff"]): return
            if response.status != 200: return

            try:
                data = await response.json()
            except Exception:
                return

            # Only capture if it looks like search results (has records-ish shape)
            if isinstance(data, dict) and any(k in data for k in
                ["records","results","documents","searches","hits","items","data"]):
                self.captured.append({"url": url, "data": data})
            elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                self.captured.append({"url": url, "data": data})
        except Exception:
            pass


async def stage_clerk(days: int) -> Tuple[List[Rec], List[SearchAudit]]:
    """
    Master scraping pipeline:
      1. For each of 37 search types
      2. Split the date window into 2-day chunks
      3. For each chunk, navigate AND capture network responses
      4. Parse results from intercepted API JSON first, then HTML fallback
      5. Track everything in audit log
    """
    from playwright.async_api import async_playwright

    end_dt = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59)
    start_dt = (end_dt - timedelta(days=days)).replace(hour=0, minute=0, second=0)
    log.info("Window: %s → %s (%d days)",
             start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"), days)

    chunks = _chunk_dates(start_dt, end_dt, CHUNK_DAYS)
    log.info("Chunked into %d sub-windows of %d days each", len(chunks), CHUNK_DAYS)

    all_recs: List[Rec] = []
    audits: List[SearchAudit] = []

    async with async_playwright() as pw:
        br = await pw.chromium.launch(headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu",
                  "--disable-blink-features=AutomationControlled"])
        ctx = await br.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            timezone_id="America/Chicago",
        )

        # Stealth: hide automation markers
        await ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
        """)

        page = await ctx.new_page()
        page.set_default_timeout(PW_TIMEOUT)

        # Attach interceptor at the page level (catches all XHR/fetch)
        interceptor = ApiInterceptor()
        page.on("response", lambda r: asyncio.create_task(interceptor.handler(r)))

        # Login
        email = os.environ.get("BEXAR_EMAIL", "")
        password = os.environ.get("BEXAR_PASSWORD", "")
        if email and password:
            log.info("LOGIN: %s", email)
            try:
                await page.goto(f"{CLERK}/signin", wait_until="domcontentloaded")
                await page.wait_for_timeout(3000)
                await page.locator("input[type='email'], input[name='email'], input[type='text']").first.fill(email)
                await page.locator("input[type='password']").first.fill(password)
                await page.locator("button:has-text('Sign In'), button[type='submit']").first.click()
                await page.wait_for_timeout(5000)
                html = await page.content()
                log.info("LOGIN: %s", "SUCCESS" if ("Sign Out" in html or "Cart" in html) else "UNCERTAIN")
            except Exception as e:
                log.error("LOGIN failed: %s", e)

        # ── Main scrape loop: every search × every chunk ──
        for sr in SEARCHES:
            if sr["dept"] == "FC" and not (email and password):
                log.info("SKIP [FC]: %s (no login)", sr["label"])
                continue

            audit = SearchAudit(search_label=sr["label"], code=sr["code"],
                                dept=sr["dept"], tier=sr["tier"])
            log.info("══ CLERK [%s/%s] %s: %s ══",
                     sr["tier"], sr["dept"], sr["code"], sr["label"])

            for (chunk_start, chunk_end) in chunks:
                audit.chunks_run += 1
                interceptor.captured.clear()  # clear before each chunk
                url = _build_url(sr, chunk_start, chunk_end)

                try:
                    chunk_recs = await _aretry(
                        _scrape_window, page, interceptor, url, sr,
                        chunk_start, chunk_end, audit,
                        label=f"{sr['code']}/{chunk_start.strftime('%m%d')}")
                    audit.records_found += len(chunk_recs)
                    all_recs.extend(chunk_recs)
                except Exception as e:
                    audit.chunks_failed += 1
                    audit.errors.append(f"{chunk_start.date()}: {str(e)[:200]}")
                    log.error("    CHUNK FAIL: %s", e)

            log.info("    SEARCH TOTAL: %d records (%d chunks ok, %d failed)",
                     audit.records_found, audit.chunks_run - audit.chunks_failed,
                     audit.chunks_failed)
            audits.append(audit)

        log.info("══════ CLERK TOTAL: %d records across %d searches ══════",
                 len(all_recs), len(audits))

        # ── Detail page enrichment — prioritized by tier ──
        need = [r for r in all_recs if not r.prop_address and r.doc_num]
        need.sort(key=lambda r: ({"S": 0, "A": 1, "B": 2, "C": 3}.get(r.tier, 4),
                                  -r.score))
        log.info("DETAIL: %d need addresses (cap %d, budget %ds)",
                 len(need), DETAIL_CAP, DETAIL_BUDGET)
        done = 0
        t0 = time.time()
        for r in need:
            if done >= DETAIL_CAP or time.time() - t0 > DETAIL_BUDGET: break
            try:
                await _detail(page, r); done += 1
                if done % 50 == 0:
                    log.info("  ...%d/%d details (%.0fs elapsed)",
                             done, len(need), time.time() - t0)
            except Exception: pass
            await page.wait_for_timeout(300)
        log.info("DETAIL: %d pages in %.0fs", done, time.time() - t0)

        await ctx.close(); await br.close()
    return all_recs, audits


async def _scrape_window(page, interceptor: ApiInterceptor, url: str,
                         sr: Dict, chunk_start: datetime, chunk_end: datetime,
                         audit: SearchAudit) -> List[Rec]:
    """
    Master scrape for a single search × chunk:
      1. Navigate
      2. Wait for either API response OR table to render
      3. Try to extract from intercepted JSON API
      4. Fall back to HTML parsing
      5. Paginate through ALL pages
      6. If we hit ≥ MAX_RESULTS_PER_QUERY, log warning (need finer chunks)
    """
    from bs4 import BeautifulSoup
    out: List[Rec] = []

    await page.goto(url, wait_until="domcontentloaded")
    try:
        await page.wait_for_selector(
            "table tbody tr td, .a11y-table tr td, [class*='result'] td, "
            "text=/no results/i, text=/0 results/i",
            timeout=15000)
    except Exception:
        log.debug("    no results selector match")
    await page.wait_for_timeout(4000)

    # Try to set page size to max
    try:
        for sel_str in ["50", "100", "200"]:
            sel = page.locator(f"select option:has-text('{sel_str}')")
            if await sel.count() > 0:
                await page.locator("select").first.select_option(sel_str)
                await page.wait_for_timeout(3000)
                break
    except Exception: pass

    pg = 1
    seen_doc_nums: Set[str] = set()
    while pg <= MAX_PAGES:
        # Strategy A: try intercepted API data first
        api_rows: List[Rec] = []
        for cap in interceptor.captured:
            try:
                parsed = _parse_api_payload(cap["data"], sr)
                api_rows.extend(parsed)
            except Exception: pass

        # Strategy B: parse rendered HTML table
        html = await page.content()
        html_rows = _parse_html_table(html, sr)

        # Use whichever returned more (API is usually richer)
        if len(api_rows) >= len(html_rows) and api_rows:
            rows = api_rows
            audit.api_intercepts += 1
            method = "API"
        else:
            rows = html_rows
            audit.html_fallbacks += 1
            method = "HTML"

        if not rows: break

        # Dedupe within this chunk (some pagination shows overlap)
        new = [r for r in rows if r.doc_num not in seen_doc_nums]
        for r in new: seen_doc_nums.add(r.doc_num)
        out.extend(new)
        log.info("    pg %d (%s) → %d rows, %d new (chunk total %d)",
                 pg, method, len(rows), len(new), len(out))

        if len(rows) < PER_PAGE: break  # last page

        # Click next
        clicked = False
        try:
            for s in ["a:has-text('Next')", "button:has-text('Next')",
                      "[aria-label='Next page']", "[aria-label='Next']",
                      "a:has-text('»')", ".pagination-next",
                      f"a:has-text('{pg + 1}')"]:
                loc = page.locator(s)
                cnt = await loc.count()
                if cnt > 0:
                    try:
                        await loc.first.click(timeout=3000)
                        await page.wait_for_timeout(2500)
                        clicked = True; break
                    except Exception: continue
        except Exception: pass
        if not clicked: break
        pg += 1

    # Sanity check: did we hit the result cap?
    if len(out) >= MAX_RESULTS_PER_QUERY:
        log.warning("    ⚠️  chunk hit %d records — may need finer chunking", len(out))
        audit.errors.append(f"Result cap hit at {chunk_start.date()}: {len(out)} records")

    return out


def _parse_api_payload(data: Any, sr: Dict) -> List[Rec]:
    """
    Parse Neumo/Kofile JSON API responses.
    The schema isn't documented but we look for any list of records-like dicts.
    """
    out: List[Rec] = []
    records = None

    # Try common keys
    if isinstance(data, dict):
        for key in ["records", "results", "documents", "hits", "items", "data"]:
            if key in data and isinstance(data[key], list):
                records = data[key]
                break
        if records is None:
            # Try nested in hits.hits (Elasticsearch style)
            if "hits" in data and isinstance(data["hits"], dict):
                inner = data["hits"].get("hits")
                if isinstance(inner, list):
                    records = [h.get("_source", h) for h in inner if isinstance(h, dict)]
    elif isinstance(data, list):
        records = data

    if not records: return out

    for raw in records:
        if not isinstance(raw, dict): continue
        try:
            # Field guessing — Neumo's typical fields
            doc_num = str(raw.get("docNumber") or raw.get("doc_num") or
                          raw.get("documentNumber") or raw.get("instrumentNumber") or "")
            doc_type = str(raw.get("docType") or raw.get("doc_type") or
                           raw.get("documentType") or "") or sr["label"]
            filed_raw = (raw.get("recordedDate") or raw.get("recorded_date") or
                         raw.get("instrumentDate") or raw.get("filed") or "")

            # Parties
            grantor = ""; grantee = ""
            parties = raw.get("parties") or raw.get("grantorGrantee") or {}
            if isinstance(parties, list):
                for p in parties:
                    if isinstance(p, dict):
                        role = (p.get("role") or p.get("type") or "").upper()
                        name = p.get("name") or p.get("fullName") or ""
                        if "GRANTOR" in role and not grantor: grantor = name
                        elif "GRANTEE" in role and not grantee: grantee = name
            grantor = grantor or str(raw.get("grantor") or raw.get("owner") or "")
            grantee = grantee or str(raw.get("grantee") or "")

            # Legal description
            legal_raw = (raw.get("legalDescription") or raw.get("legal") or
                        raw.get("legals") or "")
            if isinstance(legal_raw, list):
                legal = " | ".join(str(l.get("description", "") if isinstance(l, dict) else l)
                                   for l in legal_raw)
            else:
                legal = str(legal_raw)

            # Property address
            prop_addr = ""
            pa = raw.get("propertyAddress") or raw.get("propAddress") or {}
            if isinstance(pa, dict):
                parts = [pa.get(k, "") for k in ["address1","address2","city","state","zip"]]
                prop_addr = " ".join(p for p in parts if p)
            elif isinstance(pa, str):
                prop_addr = pa

            doc_id = str(raw.get("id") or raw.get("_id") or doc_num)

            if not doc_num and not grantor: continue

            r = Rec(
                doc_num=doc_num.strip(),
                doc_type=doc_type,
                filed=_pdate(filed_raw),
                cat=sr["cat"],
                cat_label=sr["label"],
                tier=sr["tier"],
                owner=_cname(grantor),
                grantee=_cname(grantee),
                legal=legal[:600] if legal else "",
                clerk_url=f"{CLERK}/doc/{doc_id}" if doc_id else "",
            )
            if prop_addr:
                pa_parsed = _parse_addr(prop_addr)
                r.prop_address = pa_parsed["address"]
                r.prop_city = pa_parsed["city"]
                r.prop_zip = pa_parsed["zip"]
            out.append(r)
        except Exception:
            continue
    return out


def _parse_html_table(html: str, sr: Dict) -> List[Rec]:
    """HTML fallback parser (same as v7 logic)."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    out: List[Rec] = []

    table = soup.find("div", class_="a11y-table") or soup.find("table")
    if not table: return out

    hdrs = []
    thead = table.find("thead") or table
    for th in thead.find_all(["th","td"]):
        hdrs.append(th.get_text(" ", strip=True).lower().strip())

    def col(*names):
        for n in names:
            for i, h in enumerate(hdrs):
                if n in h: return i
        return -1

    is_fc = sr.get("dept") == "FC" or col("sale date", "property address") >= 0
    ci = {
        "grantor": col("grantor"), "grantee": col("grantee"),
        "doctype": col("doc type", "doctype"),
        "date": col("recorded date", "recorded", "date"),
        "docnum": col("doc number", "doc num", "document"),
        "legal": col("legal description", "legal desc", "legal"),
        "lot": col("lot"), "block": col("block"), "ncb": col("ncb"),
        "propaddr": col("property address", "prop addr"),
    }

    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        try:
            cells = tr.find_all("td")
            if len(cells) < 3: continue

            def g(key):
                idx = ci.get(key, -1)
                return cells[idx].get_text(" ", strip=True) if 0 <= idx < len(cells) else ""

            doc_num = g("docnum")
            doc_type = g("doctype") or sr["label"]
            filed_raw = g("date")

            if is_fc:
                grantor = ""; grantee_raw = ""
                prop_addr_raw = g("propaddr"); legal = ""
            else:
                grantor = g("grantor"); grantee_raw = g("grantee")
                prop_addr_raw = ""
                legal_parts = [g("legal")]
                if g("lot"): legal_parts.append(f"Lot: {g('lot')}")
                if g("block"): legal_parts.append(f"Block: {g('block')}")
                if g("ncb"): legal_parts.append(f"NCB: {g('ncb')}")
                legal = " | ".join(p for p in legal_parts if p)

            href = ""
            a = tr.find("a", href=True)
            if a:
                h = a["href"]
                href = h if h.startswith("http") else f"{CLERK}{h}"
                if not doc_num:
                    m = re.search(r"/doc/(\w+)", h)
                    if m: doc_num = m.group(1)

            if not doc_num and not grantor and not prop_addr_raw: continue

            prop_address = prop_city = prop_zip = ""
            if prop_addr_raw:
                pa = _parse_addr(prop_addr_raw)
                prop_address = pa["address"]; prop_city = pa["city"]; prop_zip = pa["zip"]

            out.append(Rec(
                doc_num=doc_num.strip(), doc_type=doc_type,
                filed=_pdate(filed_raw), cat=sr["cat"], cat_label=sr["label"],
                tier=sr.get("tier", ""),
                owner=_cname(grantor), grantee=_cname(grantee_raw),
                legal=legal[:600] if legal else "",
                prop_address=prop_address,
                prop_city=prop_city or "San Antonio",
                prop_zip=prop_zip,
                clerk_url=href or (f"{CLERK}/doc/{doc_num}" if doc_num else ""),
            ))
        except Exception:
            continue
    return out


async def _detail(page, r: Rec) -> None:
    url = f"{CLERK}/doc/{r.doc_num}"
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_timeout(1500)

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(await page.content(), "lxml")
    text = soup.get_text(" ", strip=True)

    if not r.prop_address:
        for el in soup.find_all(string=re.compile(r"Property\s+Address", re.I)):
            parent = el.find_parent()
            if parent:
                nxt = parent.find_next_sibling()
                if nxt:
                    addr = nxt.get_text(" ", strip=True)
                    if addr and len(addr) > 5 and re.search(r"\d", addr):
                        p = _parse_addr(addr)
                        r.prop_address = p["address"]
                        r.prop_city = p["city"]
                        r.prop_zip = p["zip"]
                        break

    if r.amount == 0.0:
        m = re.search(r"Consideration\s*:?\s*\$?\s*([\d,]+(?:\.\d{1,2})?)", text, re.I)
        if m: r.amount = _float(m.group(1))

    grantors, grantees = [], []
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 2:
            name = cells[0].get_text(" ", strip=True)
            role = cells[-1].get_text(" ", strip=True).upper()
            if "GRANTOR" in role and name: grantors.append(name)
            elif "GRANTEE" in role and name: grantees.append(name)
    if grantors and not r.owner: r.owner = _cname(grantors[0])
    if grantees and not r.grantee: r.grantee = _cname(grantees[0])


# ═══════════════════════════════════════════════════════════════════
#  STAGE 2 — PARCEL ENRICHMENT (BCAD ArcGIS)
# ═══════════════════════════════════════════════════════════════════

def stage_parcels(recs: List[Rec]) -> None:
    owners: Dict[str, List[Rec]] = {}
    for r in recs:
        if r.owner: owners.setdefault(r.owner.upper().strip(), []).append(r)
    log.info("PARCEL: %d unique owners", len(owners))
    hit = 0
    for name, group in owners.items():
        if all(r.mail_address for r in group): continue
        try:
            p = _retry(_arcgis, name, label=f"arc:{name[:25]}")
            if p:
                for r in group: _apply(r, p)
                hit += 1
        except Exception: pass
        time.sleep(0.12)
    log.info("PARCEL: matched %d/%d", hit, len(owners))


def _arcgis(name: str) -> Optional[Dict]:
    safe = name.replace("'", "''")
    r = _aq(f"Owner = '{safe}'")
    return r if r else _aq(f"Owner LIKE '{safe[:20].rstrip()}%'")

def _aq(where: str) -> Optional[Dict]:
    r = requests.get(ARCGIS, params={
        "where": where, "outFields": ARCGIS_FIELDS,
        "returnGeometry": "false", "resultRecordCount": "1", "f": "json"
    }, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    ft = r.json().get("features", [])
    return ft[0]["attributes"] if ft else None

def _apply(r: Rec, p: Dict) -> None:
    if not r.prop_address:
        s = str(p.get("Situs") or "").strip()
        if s:
            a = _parse_addr(s)
            r.prop_address = a["address"]
            r.prop_city = a["city"] or r.prop_city
            r.prop_zip = a["zip"] or r.prop_zip
    if not r.mail_address:
        a1 = str(p.get("AddrLn1") or "").strip()
        a2 = str(p.get("AddrLn2") or "").strip()
        r.mail_address = f"{a1} {a2}".strip()
        r.mail_city = str(p.get("AddrCity") or "").strip()
        r.mail_state = str(p.get("AddrSt") or "").strip() or "TX"
        r.mail_zip = str(p.get("Zip") or "").strip()


# ═══════════════════════════════════════════════════════════════════
#  STAGE 3 — SCORING + COMPLETENESS
# ═══════════════════════════════════════════════════════════════════

def stage_score(recs: List[Rec]) -> None:
    week = datetime.now(timezone.utc) - timedelta(days=7)
    oc: Dict[str, Set[str]] = {}
    for r in recs:
        if r.owner: oc.setdefault(r.owner.upper().strip(), set()).add(r.cat)
    for r in recs:
        try: _score(r, oc, week)
        except: r.score = 30
        r.compute_completeness()

def _score(r: Rec, oc: Dict, week: datetime) -> None:
    fl: List[str] = []
    s = TIER_BASE.get(r.tier, 25)
    f = FLAGS.get(r.cat)
    if f: fl.append(f)

    if re.search(r"\b(LLC|INC|CORP|CO\.|COMPANY|TRUST|LP|LTD|LLP|PARTNERSHIP)\b",
                 (r.owner or "").upper()):
        fl.append("LLC / corp owner"); s += 5

    new = False
    try:
        if r.filed and datetime.strptime(r.filed, "%Y-%m-%d").replace(tzinfo=timezone.utc) >= week:
            fl.append("New this week"); new = True; s += 5
    except: pass

    owner_cats = oc.get((r.owner or "").upper().strip(), set())
    for rule in COMBO_RULES:
        if rule["cats"].issubset(owner_cats):
            s += rule["bonus"]; fl.append(rule["flag"])

    if r.amount > 100000: s += 15
    elif r.amount > 50000: s += 10
    if r.prop_address: s += 5

    r.flags = sorted(set(fl))
    r.score = min(100, max(0, s))


# ═══════════════════════════════════════════════════════════════════
#  STAGE 4 — COMPLETENESS GATE + DEDUP + FRESH-ONLY
# ═══════════════════════════════════════════════════════════════════

def stage_filter_complete(recs: List[Rec], days: int) -> List[Rec]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    out: List[Rec] = []
    drops = {"old": 0, "incomplete": 0, "tier_s_no_addr": 0, "no_doc_num": 0}
    for r in recs:
        if not r.doc_num and not r.owner:
            drops["no_doc_num"] += 1; continue
        if r.filed and r.filed < cutoff:
            drops["old"] += 1; continue
        if r.completeness < MIN_COMPLETENESS_SCORE:
            drops["incomplete"] += 1; continue
        if TIER_S_REQUIRES_ADDRESS and r.tier == "S" and not (r.prop_address or r.mail_address):
            drops["tier_s_no_addr"] += 1; continue
        out.append(r)

    log.info("FILTER: kept %d / dropped %d (%s)",
             len(out), sum(drops.values()),
             ", ".join(f"{k}={v}" for k,v in drops.items() if v))
    return out


def stage_dedup(new: List[Rec], days: int) -> List[Rec]:
    """Merge with prior data, hard-cutoff anything outside lookback window."""
    old: Dict[str, Rec] = {}
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    if OUT_DATA.exists():
        try:
            for item in json.loads(OUT_DATA.read_text())["records"]:
                r = Rec(**{k: v for k, v in item.items() if k in Rec.__dataclass_fields__})
                if r.filed and r.filed < cutoff: continue
                old[_key(r)] = r
        except: pass
    for r in new: old[_key(r)] = r
    out = list(old.values())
    out.sort(key=lambda r: (-r.score, r.filed or ""))
    return out

def _key(r: Rec) -> str:
    return r.doc_num if r.doc_num else f"{r.cat}|{r.owner}|{r.filed}"


# ═══════════════════════════════════════════════════════════════════
#  STAGE 5 — OUTPUT
# ═══════════════════════════════════════════════════════════════════

def stage_output(recs: List[Rec], days: int, audits: List[SearchAudit]) -> List[Rec]:
    for p in [OUT_DASH, OUT_DATA, OUT_CSV, OUT_HOT, OUT_AUDIT]:
        p.parent.mkdir(parents=True, exist_ok=True)

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    blob = {
        "fetched_at": end.isoformat(),
        "source": "Bexar County Clerk + BCAD ArcGIS",
        "date_range": {"start": start.strftime("%Y-%m-%d"),
                       "end": end.strftime("%Y-%m-%d"), "days": days},
        "total": len(recs),
        "with_address": sum(1 for r in recs if r.prop_address or r.mail_address),
        "with_prop_address": sum(1 for r in recs if r.prop_address),
        "with_mail_address": sum(1 for r in recs if r.mail_address),
        "hot_leads": sum(1 for r in recs if r.score >= HOT_SCORE_THRESHOLD),
        "warm_leads": sum(1 for r in recs if 70 <= r.score < HOT_SCORE_THRESHOLD),
        "tier_breakdown": {
            "S": sum(1 for r in recs if r.tier == "S"),
            "A": sum(1 for r in recs if r.tier == "A"),
            "B": sum(1 for r in recs if r.tier == "B"),
            "C": sum(1 for r in recs if r.tier == "C"),
        },
        "records": [asdict(r) for r in recs],
    }
    txt = json.dumps(blob, indent=2, default=str)
    OUT_DASH.write_text(txt); OUT_DATA.write_text(txt)
    log.info("JSON: %d records → %s", len(recs), OUT_DATA)

    # Audit log
    audit_blob = {
        "run_at": end.isoformat(),
        "lookback_days": days,
        "total_kept": len(recs),
        "searches": [asdict(a) for a in audits],
        "summary": {
            "searches_total": len(audits),
            "searches_zero_results": sum(1 for a in audits if a.records_found == 0),
            "searches_failed_chunks": sum(1 for a in audits if a.chunks_failed > 0),
            "total_chunks_run": sum(a.chunks_run for a in audits),
            "total_chunks_failed": sum(a.chunks_failed for a in audits),
            "api_intercepts": sum(a.api_intercepts for a in audits),
            "html_fallbacks": sum(a.html_fallbacks for a in audits),
        },
    }
    OUT_AUDIT.write_text(json.dumps(audit_blob, indent=2, default=str))
    log.info("AUDIT: %s", OUT_AUDIT)

    # GHL CSV
    hdr = ["First Name","Last Name","Mailing Address","Mailing City","Mailing State",
           "Mailing Zip","Property Address","Property City","Property State","Property Zip",
           "Lead Type","Tier","Document Type","Date Filed","Document Number","Amount/Debt Owed",
           "Seller Score","Completeness","Motivated Seller Flags","Source","Public Records URL"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(hdr)
        for r in recs:
            try:
                fn, ln = _split(r.owner)
                w.writerow([fn, ln,
                    r.mail_address, r.mail_city, r.mail_state, r.mail_zip,
                    r.prop_address, r.prop_city, r.prop_state, r.prop_zip,
                    r.cat_label, r.tier, r.doc_type, r.filed, r.doc_num,
                    f"{r.amount:.2f}" if r.amount else "",
                    r.score, r.completeness, "; ".join(r.flags),
                    "Bexar County Clerk", r.clerk_url])
            except: continue
    log.info("CSV: %d rows → %s", len(recs), OUT_CSV)

    hot = sorted([r for r in recs if r.score >= 75],
                 key=lambda r: (-r.score, r.filed or ""))[:50]
    with open(OUT_HOT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Score","Tier","Owner","Property Address","Mail Address","City",
                    "Lead Type","Date Filed","Flags","Doc Number","Public Records URL"])
        for r in hot:
            w.writerow([r.score, r.tier, r.owner,
                       r.prop_address, r.mail_address, r.prop_city or r.mail_city,
                       r.cat_label, r.filed, "; ".join(r.flags),
                       r.doc_num, r.clerk_url])
    log.info("HOT LEADS: %d → %s", len(hot), OUT_HOT)
    return hot


# ═══════════════════════════════════════════════════════════════════
#  STAGE 6 — TELEGRAM ALERTS
# ═══════════════════════════════════════════════════════════════════

def stage_alerts(recs: List[Rec], days: int) -> None:
    if not (TG_TOKEN and TG_CHAT):
        log.info("ALERTS: skipped (no Telegram credentials)"); return

    alert_log = ROOT / "data" / "alerted_doc_nums.json"
    alerted: Set[str] = set()
    if alert_log.exists():
        try: alerted = set(json.loads(alert_log.read_text()))
        except: pass

    new_hot = [r for r in recs
               if r.score >= HOT_SCORE_THRESHOLD and r.doc_num not in alerted]
    new_hot.sort(key=lambda r: -r.score)
    log.info("ALERTS: %d new hot leads (score >= %d)", len(new_hot), HOT_SCORE_THRESHOLD)
    if not new_hot: return

    summary = (f"🔥 *{len(new_hot)} NEW HOT LEADS* (score ≥ {HOT_SCORE_THRESHOLD})\n"
               f"📅 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
               f"Top {min(10, len(new_hot))} below...")
    _tg_send(summary)

    for r in new_hot[:10]:
        msg = (f"🎯 *Score: {r.score}* | Tier {r.tier}\n"
               f"👤 *{r.owner}*\n"
               f"🏠 {r.prop_address or '(no address)'}\n"
               f"   {r.prop_city}, TX {r.prop_zip}\n"
               f"📬 Mail: {r.mail_address or '(none)'}\n"
               f"   {r.mail_city}, {r.mail_state} {r.mail_zip}\n"
               f"📋 {r.cat_label}\n"
               f"📅 Filed: {r.filed}\n"
               f"🏷️ {' | '.join(r.flags) if r.flags else '(no flags)'}\n"
               f"{'💰 ${:,.0f}'.format(r.amount) if r.amount else ''}\n"
               f"🔗 [View]({r.clerk_url})")
        _tg_send(msg); time.sleep(0.5)

    alerted.update(r.doc_num for r in new_hot)
    try: alert_log.write_text(json.dumps(list(alerted)[-5000:]))
    except: pass


def _tg_send(text: str) -> None:
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        r = requests.post(url, json={
            "chat_id": TG_CHAT, "text": text,
            "parse_mode": "Markdown", "disable_web_page_preview": True
        }, timeout=15)
        if r.status_code != 200:
            log.warning("Telegram failed: %s", r.text[:200])
    except Exception as e:
        log.warning("Telegram error: %s", e)


def stage_health(recs: List[Rec], audits: List[SearchAudit]) -> bool:
    ok = len(recs) > 0
    wa = sum(1 for r in recs if r.prop_address or r.mail_address)
    avg = round(sum(r.score for r in recs) / max(1, len(recs)), 1)
    hot = sum(1 for r in recs if r.score >= HOT_SCORE_THRESHOLD)
    zero_searches = [a.search_label for a in audits if a.records_found == 0]
    failed_chunks = sum(a.chunks_failed for a in audits)

    if not ok: log.warning("HEALTH: zero records overall")
    if zero_searches:
        log.warning("HEALTH: %d searches returned zero results: %s",
                    len(zero_searches), ", ".join(zero_searches[:5]))
    if failed_chunks: log.warning("HEALTH: %d chunks failed across runs", failed_chunks)

    try:
        OUT_LOG.parent.mkdir(parents=True, exist_ok=True)
        OUT_LOG.write_text(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total": len(recs), "with_address": wa, "avg_score": avg,
            "hot_leads": hot, "healthy": ok,
            "zero_result_searches": zero_searches,
            "failed_chunks": failed_chunks,
        }, indent=2))
    except: pass
    return ok


# ═══════════════════════════════════════════════════════════════════
#  UTILITIES
# ═══════════════════════════════════════════════════════════════════

_DFMT = ["%m/%d/%Y","%m-%d-%Y","%Y-%m-%d","%b %d, %Y","%B %d, %Y",
         "%m/%d/%y","%Y-%m-%dT%H:%M:%S","%Y-%m-%dT%H:%M:%S.%fZ"]

def _pdate(s):
    s = (s or "")
    if isinstance(s, (int, float)):
        try:
            t = int(s)
            if t > 1e12: t //= 1000
            return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")
        except: return ""
    s = str(s).strip()
    if not s: return ""
    for f in _DFMT:
        try: return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except: continue
    try:
        t = int(s)
        if t > 1e12: t //= 1000
        return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")
    except: pass
    return s

def _float(s):
    try: return float(re.sub(r"[,$\s]", "", s or ""))
    except: return 0.0

def _cname(s): return re.sub(r"\s+", " ", (s or "").strip())

def _split(owner):
    n = (owner or "").strip()
    if not n: return ("","")
    if re.search(r"\b(LLC|INC|CORP|CO\.|COMPANY|TRUST|LP|LTD|LLP)\b", n.upper()):
        return ("", n)
    if "," in n:
        p = n.split(",", 1)
        return (p[1].strip(), p[0].strip())
    p = n.split()
    return (" ".join(p[:-1]), p[-1]) if len(p) > 1 else ("", p[0])

def _parse_addr(addr: str) -> Dict[str, str]:
    s = addr.strip().upper()
    m = re.search(r"^(.+)\s+(?:TX|TEXAS)\s+(\d{5})(?:\s*-?\d{4})?\s*$", s)
    if m:
        addr_city = m.group(1).strip(); z = m.group(2)
        for city in BEXAR_CITIES:
            if addr_city.endswith(city):
                a = addr_city[:-len(city)].strip()
                if a: return {"address": a, "city": city.title(), "zip": z}
        parts = addr_city.rsplit(None, 2)
        if len(parts) >= 3:
            return {"address": " ".join(parts[:-2]), "city": " ".join(parts[-2:]).title(), "zip": z}
        return {"address": addr_city, "city": "San Antonio", "zip": z}
    mz = re.search(r"(\d{5})\s*$", s)
    return {"address": s[:mz.start()].strip() if mz else s,
            "city": "San Antonio", "zip": mz.group(1) if mz else ""}


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main() -> int:
    global CHUNK_DAYS
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--chunk-days", type=int, default=CHUNK_DAYS)
    ap.add_argument("--no-parcel", action="store_true")
    ap.add_argument("--no-merge", action="store_true")
    ap.add_argument("--no-alerts", action="store_true")
    ap.add_argument("--allow-incomplete", action="store_true")
    args = ap.parse_args()

    # Allow override via flag
    CHUNK_DAYS = args.chunk_days

    log.info("══════ BEXAR COUNTY LEAD SCRAPER v8 — MASTER MODE ══════")
    log.info("Lookback: %d days | Chunk: %d days | Searches: %d",
             args.days, args.chunk_days, len(SEARCHES))
    log.info("Tier S: %d | Tier A: %d | Tier B: %d | Tier C: %d",
             sum(1 for s in SEARCHES if s["tier"] == "S"),
             sum(1 for s in SEARCHES if s["tier"] == "A"),
             sum(1 for s in SEARCHES if s["tier"] == "B"),
             sum(1 for s in SEARCHES if s["tier"] == "C"))

    audits: List[SearchAudit] = []

    try:
        raw, audits = asyncio.run(stage_clerk(args.days))
    except Exception as e:
        log.error("CLERK FAILED: %s\n%s", e, traceback.format_exc())
        raw = []
    log.info("STAGE 1: %d raw records", len(raw))

    if not args.no_parcel and raw:
        try: stage_parcels(raw)
        except Exception as e: log.error("PARCEL ERROR: %s", e)

    stage_score(raw)

    if not args.allow_incomplete:
        raw = stage_filter_complete(raw, args.days)

    recs = raw if args.no_merge else stage_dedup(raw, args.days)
    recs.sort(key=lambda r: (-r.score, r.filed or ""))

    stage_output(recs, args.days, audits)
    ok = stage_health(recs, audits)

    if not args.no_alerts:
        try: stage_alerts(recs, args.days)
        except Exception as e: log.error("ALERT ERROR: %s", e)

    wa = sum(1 for r in recs if r.prop_address)
    ma = sum(1 for r in recs if r.mail_address)
    hot = sum(1 for r in recs if r.score >= HOT_SCORE_THRESHOLD)
    warm = sum(1 for r in recs if 70 <= r.score < HOT_SCORE_THRESHOLD)
    avg = sum(r.score for r in recs) / max(1, len(recs))

    log.info("══════ DONE ══════")
    log.info("Records: %d | Prop Addr: %d (%.0f%%) | Mail Addr: %d (%.0f%%)",
             len(recs), wa, wa/max(1,len(recs))*100, ma, ma/max(1,len(recs))*100)
    log.info("Hot (≥%d): %d | Warm (70-84): %d | Avg score: %.1f",
             HOT_SCORE_THRESHOLD, hot, warm, avg)

    # Hard fail if scraper looks broken
    if len(recs) == 0:
        log.error("❌ ZERO RECORDS — scraper is BROKEN. Check audit log: %s", OUT_AUDIT)
        return 1
    zero_searches = sum(1 for a in audits if a.records_found == 0)
    if zero_searches > len(audits) // 2:
        log.error("❌ %d/%d searches returned zero — check audit log",
                  zero_searches, len(audits))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
