#!/usr/bin/env python3
"""
Bexar County (San Antonio TX) — Motivated Seller Lead Scraper v2
================================================================

Architecture
------------
1. CLERK PORTAL  — Playwright intercepts the Neumo SPA's XHR/fetch responses
   so we get structured JSON instead of parsing brittle HTML.  Falls back to
   DOM scraping only when the intercept yields nothing.

2. PARCEL DATA   — ArcGIS REST API on maps.bexar.org (real fields: Owner,
   Situs, AddrLn1, AddrCity, AddrSt, Zip).  Queried per-owner in batches.
   No bulk download required — works in CI without captchas.

3. SCORING       — deterministic 0-100 seller score with documented weights.

4. OUTPUT        — JSON (dashboard + data), GHL CSV, run log.

5. DEDUP         — merges with prior data/records.json so multi-day runs
   accumulate without losing history.

Resilience: 3-attempt retry everywhere, per-record try/except, always writes
valid output even on total failure. Logs enough to debug but never crashes.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode

import requests

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

CLERK_BASE = "https://bexar.tx.publicsearch.us"

# ArcGIS REST endpoint — verified live with real field names
ARCGIS_PARCEL_QUERY = (
    "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"
)
ARCGIS_MAX_RECORDS = 1000   # server-side cap per request
ARCGIS_FIELDS = "Owner,Situs,AddrLn1,AddrLn2,AddrCity,AddrSt,Zip,PropID"

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_JSON = ROOT / "dashboard" / "records.json"
DATA_JSON = ROOT / "data" / "records.json"
GHL_CSV = ROOT / "data" / "leads_ghl.csv"
RUN_LOG = ROOT / "data" / "last_run.log"

RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 3          # seconds × attempt number
HTTP_TIMEOUT = 60
PW_TIMEOUT = 45_000        # Playwright ms

# Document types we care about, mapped to categories + display labels
DOC_TYPES: List[Dict[str, str]] = [
    {"code": "LP",        "cat": "lis_pendens",      "label": "Lis Pendens"},
    {"code": "NOFC",      "cat": "foreclosure",      "label": "Notice of Foreclosure"},
    {"code": "TAXDEED",   "cat": "tax_deed",         "label": "Tax Deed"},
    {"code": "JUD",       "cat": "judgment",          "label": "Judgment"},
    {"code": "CCJ",       "cat": "judgment",          "label": "Certified Judgment"},
    {"code": "DRJUD",     "cat": "judgment",          "label": "Domestic Judgment"},
    {"code": "LNCORPTX",  "cat": "tax_lien",         "label": "Corp Tax Lien"},
    {"code": "LNIRS",     "cat": "tax_lien",         "label": "IRS Lien"},
    {"code": "LNFED",     "cat": "tax_lien",         "label": "Federal Lien"},
    {"code": "LN",        "cat": "lien",             "label": "Lien"},
    {"code": "LNMECH",    "cat": "mechanic_lien",    "label": "Mechanic Lien"},
    {"code": "LNHOA",     "cat": "hoa_lien",         "label": "HOA Lien"},
    {"code": "MEDLN",     "cat": "medicaid_lien",    "label": "Medicaid Lien"},
    {"code": "PRO",       "cat": "probate",          "label": "Probate"},
    {"code": "NOC",       "cat": "noc",              "label": "Notice of Commencement"},
    {"code": "RELLP",     "cat": "rel_lis_pendens",  "label": "Release Lis Pendens"},
]

# Score flag associated with each category
CAT_FLAG = {
    "lis_pendens":     "Lis pendens",
    "foreclosure":     "Pre-foreclosure",
    "tax_deed":        "Pre-foreclosure",
    "judgment":        "Judgment lien",
    "tax_lien":        "Tax lien",
    "lien":            "Judgment lien",
    "mechanic_lien":   "Mechanic lien",
    "hoa_lien":        "Judgment lien",
    "medicaid_lien":   "Judgment lien",
    "probate":         "Probate / estate",
    "noc":             None,
    "rel_lis_pendens": None,
}

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
log = logging.getLogger("bexar")


# ---------------------------------------------------------------------------
# DATA MODEL
# ---------------------------------------------------------------------------

@dataclass
class Record:
    doc_num: str = ""
    doc_type: str = ""
    filed: str = ""
    cat: str = ""
    cat_label: str = ""
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


# ---------------------------------------------------------------------------
# RETRY HELPERS
# ---------------------------------------------------------------------------

def retry_sync(fn, *a, attempts=RETRY_ATTEMPTS, label="", **kw):
    last = None
    for i in range(1, attempts + 1):
        try:
            return fn(*a, **kw)
        except Exception as e:
            last = e
            log.warning("[%s] attempt %d/%d: %s", label or fn.__name__, i, attempts, e)
            if i < attempts:
                time.sleep(RETRY_BACKOFF * i)
    raise last  # type: ignore


async def retry_async(coro_fn, *a, attempts=RETRY_ATTEMPTS, label="", **kw):
    last = None
    for i in range(1, attempts + 1):
        try:
            return await coro_fn(*a, **kw)
        except Exception as e:
            last = e
            log.warning("[%s] attempt %d/%d: %s", label or coro_fn.__name__, i, attempts, e)
            if i < attempts:
                await asyncio.sleep(RETRY_BACKOFF * i)
    raise last  # type: ignore


# ===================================================================
#  STAGE 1 — CLERK PORTAL (Playwright, API-intercept + DOM fallback)
# ===================================================================

def _clerk_results_url(code: str, start: str, end: str) -> str:
    """Build the Neumo SPA results URL."""
    params = {
        "department":        "RP",
        "docTypes":          code,
        "recordedDateRange": f"{start},{end}",
        "searchType":        "advancedSearch",
        "limit":             "250",
    }
    return f"{CLERK_BASE}/results?{urlencode(params)}"


async def scrape_clerk(days: int) -> List[Record]:
    """Main clerk entry point — drives Playwright."""
    from playwright.async_api import async_playwright

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)
    s = start_dt.strftime("%Y%m%d")
    e = end_dt.strftime("%Y%m%d")

    records: List[Record] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-extensions",
            ],
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
        )
        page = await ctx.new_page()
        page.set_default_timeout(PW_TIMEOUT)

        for dt in DOC_TYPES:
            url = _clerk_results_url(dt["code"], s, e)
            log.info("Clerk: %s (%s)", dt["code"], dt["label"])
            try:
                rows = await retry_async(
                    _scrape_single_type, page, url, dt,
                    label=f"clerk:{dt['code']}",
                )
                log.info("  → %d records", len(rows))
                records.extend(rows)
            except Exception as exc:
                log.error("Clerk: %s failed after retries: %s", dt["code"], exc)

        await ctx.close()
        await browser.close()

    return records


async def _scrape_single_type(page, url: str, dt: Dict) -> List[Record]:
    """
    Strategy:
      1. Set up a route listener that intercepts JSON responses from the
         Neumo back-end (they typically hit /api/ or return application/json).
      2. Navigate to the results URL.
      3. If the intercept captured JSON results, parse those.
      4. Otherwise, fall back to DOM table scraping.
    """
    captured_json: List[dict] = []

    async def _intercept(response):
        """Capture any JSON response that looks like a results payload."""
        try:
            ct = response.headers.get("content-type", "")
            if "json" not in ct:
                return
            if response.status != 200:
                return
            body = await response.json()
            # Neumo patterns: body is a list, or body has a "results" key
            if isinstance(body, list) and len(body) > 0:
                captured_json.append({"items": body})
            elif isinstance(body, dict):
                # Look for any list of objects in the top-level keys
                for key, val in body.items():
                    if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                        captured_json.append({"items": val, "key": key})
                        break
        except Exception:
            pass

    page.on("response", _intercept)
    try:
        await page.goto(url, wait_until="domcontentloaded")
        # Give the SPA time to fire its data requests
        await page.wait_for_timeout(4000)
    finally:
        page.remove_listener("response", _intercept)

    # -- Strategy A: Parse intercepted JSON --
    if captured_json:
        return _parse_intercepted_json(captured_json, dt)

    # -- Strategy B: DOM fallback --
    log.debug("  No JSON intercepted for %s, trying DOM", dt["code"])
    return await _parse_dom(page, dt)


def _parse_intercepted_json(captures: List[dict], dt: Dict) -> List[Record]:
    """Turn intercepted API responses into Record objects."""
    out: List[Record] = []
    for cap in captures:
        for item in cap.get("items", []):
            try:
                rec = _json_item_to_record(item, dt)
                if rec:
                    out.append(rec)
            except Exception:
                continue
    return out


def _json_item_to_record(item: dict, dt: Dict) -> Optional[Record]:
    """
    Map Neumo JSON fields to our Record.  Common field names across Neumo
    portals: docNumber/documentNumber, docType, recordedDate/filedDate,
    grantor, grantee, legalDescription, consideration/amount.
    We search case-insensitively.
    """
    def g(*keys: str) -> str:
        for k in keys:
            for ik, iv in item.items():
                if ik.lower().replace("_", "") == k.lower().replace("_", ""):
                    if iv is not None:
                        return str(iv).strip()
        return ""

    doc_num = g("docNumber", "documentNumber", "docNum", "id", "documentId")
    if not doc_num and not g("grantor", "grantorName"):
        return None  # empty row

    filed_raw = g("recordedDate", "filedDate", "dateRecorded", "date")
    filed = _parse_date(filed_raw)

    amount_str = g("consideration", "amount", "monetaryAmount", "debtAmount")
    amount = _to_float(amount_str)

    clerk_url = ""
    if doc_num:
        clerk_url = f"{CLERK_BASE}/doc/{doc_num}"

    return Record(
        doc_num=doc_num,
        doc_type=g("docType", "documentType", "type") or dt["label"],
        filed=filed,
        cat=dt["cat"],
        cat_label=dt["label"],
        owner=_clean_name(g("grantor", "grantorName", "grantors")),
        grantee=_clean_name(g("grantee", "granteeName", "grantees")),
        amount=amount,
        legal=g("legalDescription", "legal", "legalDesc")[:500],
        clerk_url=clerk_url,
    )


async def _parse_dom(page, dt: Dict) -> List[Record]:
    """
    Fallback: parse whatever the SPA rendered into the DOM.
    Neumo renders either <table> rows or result-card divs.
    """
    from bs4 import BeautifulSoup

    html = await page.content()
    soup = BeautifulSoup(html, "lxml")
    out: List[Record] = []

    # --- Try <table> first ---
    table = soup.find("table")
    if table:
        headers = [
            (th.get_text(" ", strip=True) or "").lower()
            for th in (table.find("thead") or table).find_all(["th"])
        ]
        tbody = table.find("tbody") or table
        for tr in tbody.find_all("tr"):
            try:
                cells = tr.find_all("td")
                if not cells:
                    continue
                rec = _row_to_record(cells, headers, dt)
                if rec:
                    out.append(rec)
            except Exception:
                continue
        if out:
            return out

    # --- Try result cards (Neumo "result-item" pattern) ---
    cards = soup.select("[class*='result'], [class*='Result'], [class*='search-result']")
    for card in cards:
        try:
            text = card.get_text(" | ", strip=True)
            rec = _card_text_to_record(text, card, dt)
            if rec:
                out.append(rec)
        except Exception:
            continue

    return out


def _row_to_record(cells, headers, dt) -> Optional[Record]:
    """Parse a <tr>'s <td> cells using header names or positional fallback."""
    def col(names, pos=None):
        for n in names:
            for i, h in enumerate(headers):
                if n in h and i < len(cells):
                    return cells[i].get_text(" ", strip=True)
        if pos is not None and pos < len(cells):
            return cells[pos].get_text(" ", strip=True)
        return ""

    doc_num = col(["doc #", "doc#", "document", "number"], 0)
    filed_raw = col(["recorded", "date", "filed"], 2)
    grantor = col(["grantor"], 3)
    grantee = col(["grantee"], 4)
    legal = col(["legal", "description"], 5)

    if not doc_num and not grantor:
        return None

    # Find link
    href = ""
    for cell in cells:
        a = cell.find("a", href=True)
        if a:
            h = a["href"]
            if "/doc/" in h:
                href = h if h.startswith("http") else f"{CLERK_BASE}{h}"
                if not doc_num:
                    m = re.search(r"/doc/(\d+)", h)
                    if m:
                        doc_num = m.group(1)
            break

    return Record(
        doc_num=doc_num,
        doc_type=col(["doc type", "type"], 1) or dt["label"],
        filed=_parse_date(filed_raw),
        cat=dt["cat"],
        cat_label=dt["label"],
        owner=_clean_name(grantor),
        grantee=_clean_name(grantee),
        legal=legal[:500],
        clerk_url=href or (f"{CLERK_BASE}/doc/{doc_num}" if doc_num else ""),
    )


def _card_text_to_record(text: str, card, dt: Dict) -> Optional[Record]:
    """Last resort: regex-parse a text blob from a result card."""
    doc_m = re.search(r"(?:doc(?:ument)?[#\s:]*)?(\d{6,})", text, re.I)
    doc_num = doc_m.group(1) if doc_m else ""

    date_m = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", text)
    filed = _parse_date(date_m.group(1)) if date_m else ""

    if not doc_num and not filed:
        return None

    href = ""
    a = card.find("a", href=True)
    if a and "/doc/" in a["href"]:
        href = a["href"] if a["href"].startswith("http") else f"{CLERK_BASE}{a['href']}"

    return Record(
        doc_num=doc_num,
        doc_type=dt["label"],
        filed=filed,
        cat=dt["cat"],
        cat_label=dt["label"],
        owner=_clean_name(text.split("|")[0] if "|" in text else ""),
        clerk_url=href or (f"{CLERK_BASE}/doc/{doc_num}" if doc_num else ""),
    )


# ===================================================================
#  STAGE 2 — PARCEL ENRICHMENT (ArcGIS REST API — no bulk download)
# ===================================================================

def enrich_with_parcels(records: List[Record]) -> None:
    """
    For each unique owner name, query the Bexar County ArcGIS Parcel
    layer and fill in property + mailing address.
    """
    unique_owners: Dict[str, List[Record]] = {}
    for r in records:
        if not r.owner:
            continue
        key = r.owner.upper().strip()
        unique_owners.setdefault(key, []).append(r)

    log.info("Parcel: enriching %d unique owners", len(unique_owners))
    hit = 0

    for owner_name, recs in unique_owners.items():
        # Skip if all records already have addresses (e.g. from prior run merge)
        if all(r.prop_address for r in recs):
            continue
        try:
            parcel = retry_sync(
                _query_arcgis_owner, owner_name,
                label=f"arcgis:{owner_name[:30]}",
            )
            if parcel:
                for r in recs:
                    _apply_parcel(r, parcel)
                hit += 1
        except Exception as e:
            log.debug("Parcel lookup failed for %s: %s", owner_name[:40], e)
            continue

        # Be polite: 150ms between queries
        time.sleep(0.15)

    log.info("Parcel: enriched %d/%d unique owners", hit, len(unique_owners))


def _query_arcgis_owner(owner_name: str) -> Optional[Dict]:
    """
    Query the ArcGIS MapServer for a parcel matching this owner.

    Tries exact match first, then a LIKE prefix match if needed.
    The API returns JSON with features[].attributes.
    """
    # Escape single quotes for ArcGIS SQL
    safe = owner_name.replace("'", "''")

    # Try exact match
    result = _arcgis_query(f"Owner = '{safe}'", max_results=1)
    if result:
        return result

    # Try LIKE prefix (first 20 chars)
    prefix = safe[:20].rstrip()
    result = _arcgis_query(f"Owner LIKE '{prefix}%'", max_results=1)
    return result


def _arcgis_query(where: str, max_results: int = 1) -> Optional[Dict]:
    """Execute an ArcGIS REST query and return the first feature's attributes."""
    params = {
        "where":         where,
        "outFields":     ARCGIS_FIELDS,
        "returnGeometry": "false",
        "resultRecordCount": str(max_results),
        "f":             "json",
    }
    r = requests.get(
        ARCGIS_PARCEL_QUERY,
        params=params,
        timeout=HTTP_TIMEOUT,
        headers={"User-Agent": "Mozilla/5.0 BexarLeadScraper/2.0"},
    )
    r.raise_for_status()
    data = r.json()
    features = data.get("features", [])
    if features:
        return features[0].get("attributes", {})
    return None


def _apply_parcel(rec: Record, parcel: Dict) -> None:
    """Write ArcGIS parcel data into a Record, respecting existing values."""
    if not rec.prop_address:
        situs = str(parcel.get("Situs") or "").strip()
        if situs:
            # Situs often contains "123 MAIN ST SAN ANTONIO TX 78201"
            parts = _split_situs(situs)
            rec.prop_address = parts["address"]
            rec.prop_city = parts["city"] or rec.prop_city
            rec.prop_zip = parts["zip"] or rec.prop_zip

    if not rec.mail_address:
        addr1 = str(parcel.get("AddrLn1") or "").strip()
        addr2 = str(parcel.get("AddrLn2") or "").strip()
        rec.mail_address = f"{addr1} {addr2}".strip()
        rec.mail_city = str(parcel.get("AddrCity") or "").strip()
        rec.mail_state = str(parcel.get("AddrSt") or "").strip() or "TX"
        rec.mail_zip = str(parcel.get("Zip") or "").strip()


def _split_situs(situs: str) -> Dict[str, str]:
    """
    Parse a BCAD situs string like "123 MAIN ST SAN ANTONIO TX 78201"
    into {address, city, zip}.

    Strategy: scan backward from " TX <zip>" to isolate city, then
    match known Bexar-area city names from that chunk to split
    street address from city.
    """
    s = situs.upper().strip()

    # Pattern: <address> <city> TX <zip>
    m = re.search(r"^(.+)\s+TX\s+(\d{5})(?:\s*-?\s*\d{4})?\s*$", s)
    if not m:
        # Fallback: just grab zip from end
        mz = re.search(r"(\d{5})\s*$", s)
        return {
            "address": s[:mz.start()].strip() if mz else s,
            "city": "San Antonio",
            "zip": mz.group(1) if mz else "",
        }

    addr_city = m.group(1).strip()  # "123 MAIN ST SAN ANTONIO"
    zipcode = m.group(2)

    # Known Bexar-area cities, longest first to avoid partial matches
    _CITIES = sorted([
        "HILL COUNTRY VILLAGE", "BALCONES HEIGHTS", "FAIR OAKS RANCH",
        "HOLLYWOOD PARK", "TERRELL HILLS", "UNIVERSAL CITY", "GARDEN RIDGE",
        "SHAVANO PARK", "CASTLE HILLS", "ALAMO HEIGHTS", "LEON VALLEY",
        "CHINA GROVE", "SANDY OAKS", "GREY FOREST", "SAN ANTONIO",
        "VON ORMY", "LIVE OAK", "WINDCREST", "ELMENDORF", "ST HEDWIG",
        "SOMERSET", "CONVERSE", "HELOTES", "SCHERTZ", "CIBOLO", "SELMA",
        "KIRBY", "OLMOS PARK",
    ], key=len, reverse=True)

    for city in _CITIES:
        if addr_city.endswith(city):
            addr = addr_city[: -len(city)].strip()
            if addr:
                return {"address": addr, "city": city.title(), "zip": zipcode}

    # City not in our list — assume last two words are the city
    parts = addr_city.rsplit(None, 2)
    if len(parts) >= 3:
        return {
            "address": " ".join(parts[:-2]),
            "city": " ".join(parts[-2:]).title(),
            "zip": zipcode,
        }

    return {"address": addr_city, "city": "San Antonio", "zip": zipcode}


# ===================================================================
#  STAGE 3 — SCORING
# ===================================================================

def score_all(records: List[Record]) -> None:
    """Score every record, using cross-record LP+FC combo detection."""
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)

    # Pre-compute owner → set of categories for combo detection
    owner_cats: Dict[str, Set[str]] = {}
    for r in records:
        if r.owner:
            owner_cats.setdefault(r.owner.upper().strip(), set()).add(r.cat)

    for r in records:
        try:
            _score_one(r, owner_cats, week_ago)
        except Exception:
            r.score = 30  # safe default


def _score_one(r: Record, owner_cats: Dict[str, Set[str]], week_ago: datetime) -> None:
    """
    Base 30
      +10 per flag
      +20 LP + FC combo (same owner has both lis_pendens and foreclosure)
      +15 amount > $100k
      +10 amount > $50k  (not stacked with the +15)
      +5  filed within the last 7 days
      +5  has property address
    Cap at 100.
    """
    flags: List[str] = []

    # Category flag
    flag = CAT_FLAG.get(r.cat)
    if flag:
        flags.append(flag)

    # LLC / corp owner
    if re.search(
        r"\b(LLC|L\.L\.C|INC|CORP|CO\.|COMPANY|TRUST|LP|LTD|LLP|PARTNERSHIP)\b",
        (r.owner or "").upper(),
    ):
        flags.append("LLC / corp owner")

    # New this week
    is_new = False
    try:
        if r.filed:
            fd = datetime.strptime(r.filed, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if fd >= week_ago:
                flags.append("New this week")
                is_new = True
    except ValueError:
        pass

    # LP + FC combo
    has_combo = False
    if r.cat in ("lis_pendens", "foreclosure") and r.owner:
        cats = owner_cats.get(r.owner.upper().strip(), set())
        if "lis_pendens" in cats and "foreclosure" in cats:
            has_combo = True

    score = 30
    score += 10 * len(flags)
    if has_combo:
        score += 20
    if r.amount > 100_000:
        score += 15
    elif r.amount > 50_000:
        score += 10
    if is_new:
        score += 5
    if r.prop_address:
        score += 5

    r.flags = sorted(set(flags))
    r.score = min(100, max(0, score))


# ===================================================================
#  STAGE 4 — DEDUPLICATION & MERGE
# ===================================================================

def dedup_records(new: List[Record], prior_path: Path = DATA_JSON) -> List[Record]:
    """
    Merge new records with any existing data/records.json.
    Key = doc_num. New data overwrites old for the same key.
    Records without a doc_num are keyed by owner+date+cat.
    """
    existing: Dict[str, Record] = {}

    if prior_path.exists():
        try:
            data = json.loads(prior_path.read_text(encoding="utf-8"))
            for item in data.get("records", []):
                r = Record(**{k: v for k, v in item.items() if k in Record.__dataclass_fields__})
                key = _record_key(r)
                existing[key] = r
        except Exception as e:
            log.warning("Could not load prior records: %s", e)

    for r in new:
        key = _record_key(r)
        existing[key] = r  # new wins

    merged = list(existing.values())
    merged.sort(key=lambda r: (-r.score, r.filed or ""))
    return merged


def _record_key(r: Record) -> str:
    if r.doc_num:
        return r.doc_num
    return f"{r.cat}|{r.owner}|{r.filed}|{r.grantee}"


# ===================================================================
#  STAGE 5 — OUTPUT
# ===================================================================

def write_outputs(records: List[Record], days: int) -> None:
    """Write JSON + CSV."""
    for p in [DASHBOARD_JSON, DATA_JSON, GHL_CSV]:
        p.parent.mkdir(parents=True, exist_ok=True)

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    payload = {
        "fetched_at": end.isoformat(),
        "source": "Bexar County Clerk + BCAD ArcGIS",
        "date_range": {
            "start": start.strftime("%Y-%m-%d"),
            "end":   end.strftime("%Y-%m-%d"),
            "days":  days,
        },
        "total": len(records),
        "with_address": sum(1 for r in records if r.prop_address or r.mail_address),
        "records": [asdict(r) for r in records],
    }

    text = json.dumps(payload, indent=2, default=str)
    DASHBOARD_JSON.write_text(text, encoding="utf-8")
    DATA_JSON.write_text(text, encoding="utf-8")
    log.info("Wrote %d records → %s, %s", len(records), DASHBOARD_JSON, DATA_JSON)

    _write_ghl_csv(records)


def _write_ghl_csv(records: List[Record]) -> None:
    headers = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]
    with open(GHL_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in records:
            try:
                first, last = _split_name(r.owner)
                w.writerow([
                    first, last,
                    r.mail_address, r.mail_city, r.mail_state, r.mail_zip,
                    r.prop_address, r.prop_city, r.prop_state, r.prop_zip,
                    r.cat_label, r.doc_type, r.filed, r.doc_num,
                    f"{r.amount:.2f}" if r.amount else "",
                    r.score, "; ".join(r.flags),
                    "Bexar County Clerk", r.clerk_url,
                ])
            except Exception:
                continue
    log.info("Wrote GHL CSV → %s (%d rows)", GHL_CSV, len(records))


# ===================================================================
#  HEALTH CHECK / ALERTING
# ===================================================================

def health_check(records: List[Record], days: int) -> bool:
    """
    Return True if the run looks healthy.  Log warnings for suspicious
    results so CI can surface them.
    """
    ok = True
    if len(records) == 0:
        log.warning("HEALTH: zero records returned — portal may be down or layout changed")
        ok = False
    elif len(records) < 5 and days >= 7:
        log.warning("HEALTH: only %d records for %d-day window — unusually low", len(records), days)

    scored = [r for r in records if r.score > 0]
    if records and not scored:
        log.warning("HEALTH: all records scored 0 — scoring logic may be broken")
        ok = False

    with_addr = sum(1 for r in records if r.prop_address or r.mail_address)
    if records and with_addr == 0:
        log.warning("HEALTH: no addresses found — ArcGIS enrichment may have failed")

    # Write a run log
    try:
        RUN_LOG.parent.mkdir(parents=True, exist_ok=True)
        RUN_LOG.write_text(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total": len(records),
            "with_address": with_addr,
            "avg_score": round(sum(r.score for r in records) / max(1, len(records)), 1),
            "healthy": ok,
        }, indent=2), encoding="utf-8")
    except Exception:
        pass

    return ok


# ===================================================================
#  UTILITY
# ===================================================================

_DATE_FMTS = ["%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%b %d, %Y", "%B %d, %Y",
              "%m/%d/%y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
              "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]


def _parse_date(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Try millisecond epoch (Neumo sometimes returns this)
    try:
        ts = int(s)
        if ts > 1e12:
            ts = ts / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, TypeError, OverflowError):
        pass
    return s


def _to_float(s: str) -> float:
    try:
        return float(re.sub(r"[,$\s]", "", s or ""))
    except (ValueError, TypeError):
        return 0.0


def _clean_name(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _split_name(owner: str) -> Tuple[str, str]:
    n = (owner or "").strip()
    if not n:
        return ("", "")
    if re.search(r"\b(LLC|INC|CORP|CO\.|COMPANY|TRUST|LP|LTD|LLP)\b", n.upper()):
        return ("", n)
    if "," in n:
        parts = n.split(",", 1)
        return (parts[1].strip(), parts[0].strip())
    parts = n.split()
    if len(parts) == 1:
        return ("", parts[0])
    return (" ".join(parts[:-1]), parts[-1])


# ===================================================================
#  MAIN
# ===================================================================

def parse_args():
    p = argparse.ArgumentParser(description="Bexar County Motivated Seller Lead Scraper v2")
    p.add_argument("--days", type=int, default=7, help="Lookback window (default 7)")
    p.add_argument("--no-parcel", action="store_true", help="Skip ArcGIS parcel enrichment")
    p.add_argument("--no-merge", action="store_true", help="Don't merge with prior data")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    log.info("═══ Bexar County Lead Scraper v2 — %d-day lookback ═══", args.days)

    # -- Stage 1: Clerk portal --
    try:
        raw_records = asyncio.run(scrape_clerk(args.days))
    except Exception as e:
        log.error("Clerk stage FAILED: %s\n%s", e, traceback.format_exc())
        raw_records = []

    log.info("Clerk: %d raw records", len(raw_records))

    # -- Stage 2: Parcel enrichment --
    if not args.no_parcel and raw_records:
        try:
            enrich_with_parcels(raw_records)
        except Exception as e:
            log.error("Parcel stage error: %s", e)

    # -- Stage 3: Score --
    score_all(raw_records)

    # -- Stage 4: Deduplicate / merge --
    if args.no_merge:
        records = raw_records
    else:
        records = dedup_records(raw_records)

    records.sort(key=lambda r: (-r.score, r.filed or ""))

    # -- Stage 5: Output --
    write_outputs(records, args.days)

    # -- Stage 6: Health check --
    healthy = health_check(records, args.days)

    log.info(
        "═══ Done: %d leads, %d with address, avg score %.0f %s ═══",
        len(records),
        sum(1 for r in records if r.prop_address),
        sum(r.score for r in records) / max(1, len(records)),
        "✓" if healthy else "⚠ CHECK LOGS",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
