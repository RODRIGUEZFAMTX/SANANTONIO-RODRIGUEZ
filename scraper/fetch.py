#!/usr/bin/env python3
"""
Bexar County (San Antonio TX) — Motivated Seller Lead Scraper v3
================================================================

Scrapes the Bexar County Clerk portal (bexar.tx.publicsearch.us) using
Playwright, with correct document type names, full pagination, and
detail-page scraping for property addresses.

Enriches with mailing addresses from the BCAD ArcGIS parcel API.
Scores leads 0-100, deduplicates across runs, exports JSON + GHL CSV.
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
from urllib.parse import urlencode, quote

import requests

CLERK_BASE = "https://bexar.tx.publicsearch.us"

ARCGIS_PARCEL_QUERY = (
    "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"
)
ARCGIS_FIELDS = "Owner,Situs,AddrLn1,AddrLn2,AddrCity,AddrSt,Zip,PropID"

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_JSON = ROOT / "dashboard" / "records.json"
DATA_JSON = ROOT / "data" / "records.json"
GHL_CSV = ROOT / "data" / "leads_ghl.csv"
RUN_LOG = ROOT / "data" / "last_run.log"

RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 3
HTTP_TIMEOUT = 60
PW_TIMEOUT = 60_000
RESULTS_PER_PAGE = 50
MAX_DETAIL_PAGES = 100  # max detail pages to visit per doc type per run

# ── DOCUMENT TYPES ──
# Uses the FULL names as they appear in the portal URL, NOT abbreviations.
# Confirmed from live portal: docTypes=LIS%20PENDENS in the URL bar.
DOC_TYPES: List[Dict[str, str]] = [
    {"code": "LIS PENDENS",               "cat": "lis_pendens",      "label": "Lis Pendens"},
    {"code": "NOTICE OF FORECLOSURE",      "cat": "foreclosure",      "label": "Notice of Foreclosure"},
    {"code": "TAX DEED",                   "cat": "tax_deed",         "label": "Tax Deed"},
    {"code": "JUDGMENT",                   "cat": "judgment",          "label": "Judgment"},
    {"code": "CERTIFIED JUDGMENT",         "cat": "judgment",          "label": "Certified Judgment"},
    {"code": "DOMESTIC JUDGMENT",          "cat": "judgment",          "label": "Domestic Judgment"},
    {"code": "CORP TAX LIEN",             "cat": "tax_lien",         "label": "Corp Tax Lien"},
    {"code": "IRS LIEN",                  "cat": "tax_lien",         "label": "IRS Lien"},
    {"code": "FEDERAL LIEN",              "cat": "tax_lien",         "label": "Federal Lien"},
    {"code": "LIEN",                      "cat": "lien",             "label": "Lien"},
    {"code": "MECHANIC LIEN",             "cat": "mechanic_lien",    "label": "Mechanic Lien"},
    {"code": "HOA LIEN",                  "cat": "hoa_lien",         "label": "HOA Lien"},
    {"code": "MEDICAID LIEN",             "cat": "medicaid_lien",    "label": "Medicaid Lien"},
    {"code": "PROBATE",                   "cat": "probate",          "label": "Probate"},
    {"code": "NOTICE OF COMMENCEMENT",    "cat": "noc",              "label": "Notice of Commencement"},
    {"code": "RELEASE LIS PENDENS",       "cat": "rel_lis_pendens",  "label": "Release Lis Pendens"},
]

CAT_FLAG = {
    "lis_pendens": "Lis pendens", "foreclosure": "Pre-foreclosure",
    "tax_deed": "Pre-foreclosure", "judgment": "Judgment lien",
    "tax_lien": "Tax lien", "lien": "Judgment lien",
    "mechanic_lien": "Mechanic lien", "hoa_lien": "Judgment lien",
    "medicaid_lien": "Judgment lien", "probate": "Probate / estate",
    "noc": None, "rel_lis_pendens": None,
}

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
log = logging.getLogger("bexar")


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


# ── RETRY HELPERS ──

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
    raise last


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
    raise last


# ===================================================================
#  STAGE 1 — CLERK PORTAL (Playwright with DOM table parsing)
# ===================================================================

def _clerk_results_url(doc_type_name: str, start: str, end: str) -> str:
    """
    Build URL matching the live portal format:
    /results?department=RP&docTypes=LIS%20PENDENS&recordedDateRange=20260501,20260509
    """
    return (
        f"{CLERK_BASE}/results?"
        f"department=RP&"
        f"docTypes={quote(doc_type_name)}&"
        f"recordedDateRange={start}%2C{end}"
    )


async def scrape_clerk(days: int) -> List[Record]:
    """Main clerk entry: iterate all doc types, paginate, parse DOM tables."""
    from playwright.async_api import async_playwright

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)
    s = start_dt.strftime("%Y%m%d")
    e = end_dt.strftime("%Y%m%d")

    records: List[Record] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
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
            log.info("Clerk: %s", dt["label"])
            try:
                rows = await retry_async(
                    _scrape_doc_type_all_pages, page, url, dt,
                    label=f"clerk:{dt['code']}",
                )
                log.info("  -> %d records for %s", len(rows), dt["label"])
                records.extend(rows)
            except Exception as exc:
                log.error("Clerk: %s failed: %s", dt["label"], exc)

        # Stage 1b: Visit detail pages to get property addresses
        log.info("Fetching detail pages for property addresses...")
        detail_count = 0
        for r in records:
            if r.prop_address or not r.doc_num:
                continue
            if detail_count >= MAX_DETAIL_PAGES * len(DOC_TYPES):
                break
            try:
                await _scrape_detail_page(page, r)
                detail_count += 1
                await page.wait_for_timeout(300)  # polite delay
            except Exception as exc:
                log.debug("Detail page fail %s: %s", r.doc_num, exc)
        log.info("Visited %d detail pages", detail_count)

        await ctx.close()
        await browser.close()

    return records


async def _scrape_doc_type_all_pages(page, url: str, dt: Dict) -> List[Record]:
    """Load results page and handle pagination to get ALL results."""
    from bs4 import BeautifulSoup

    all_rows: List[Record] = []

    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_timeout(3000)

    # Try to set Results Per Page to 50 if there's a dropdown
    try:
        selector = page.locator("select").filter(has_text="50")
        if await selector.count() > 0:
            await selector.first.select_option("50")
            await page.wait_for_timeout(2000)
    except Exception:
        pass

    # Parse the first page
    page_num = 1
    while True:
        html = await page.content()
        rows = _parse_results_table(html, dt)

        if not rows:
            break

        all_rows.extend(rows)
        log.info("    Page %d: %d rows (total so far: %d)", page_num, len(rows), len(all_rows))

        # Check for "next page" and click it
        if len(rows) < RESULTS_PER_PAGE:
            break  # last page

        # Try to find and click a next page button
        try:
            next_btn = page.locator(
                "button:has-text('Next'), a:has-text('Next'), "
                "[aria-label='Next'], [class*='next'], [class*='Next']"
            )
            if await next_btn.count() > 0:
                await next_btn.first.click()
                await page.wait_for_timeout(3000)
                page_num += 1
            else:
                break
        except Exception:
            break

        if page_num > 20:  # safety cap
            break

    return all_rows


def _parse_results_table(html: str, dt: Dict) -> List[Record]:
    """
    Parse the results table from the Bexar County clerk portal.

    Confirmed column order from live portal:
    [checkbox] [icon] GRANTOR | GRANTEE | DOC TYPE | RECORDED DATE | DOC NUMBER |
    BOOK/VOLUME/PAGE | LEGAL DESCRIPTION | LOT | BLOCK | NCB | COUNTY BLOCK
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    out: List[Record] = []

    table = soup.find("table")
    if not table:
        return out

    # Get headers
    headers: List[str] = []
    for th in (table.find("thead") or table).find_all(["th", "td"]):
        headers.append((th.get_text(" ", strip=True) or "").lower().strip())

    # Map header names to column indices
    def find_col(*names):
        for name in names:
            for i, h in enumerate(headers):
                if name in h:
                    return i
        return -1

    col_grantor = find_col("grantor")
    col_grantee = find_col("grantee")
    col_doctype = find_col("doc type", "doctype", "type")
    col_date = find_col("recorded date", "recorded", "date")
    col_docnum = find_col("doc number", "doc num", "document")
    col_legal = find_col("legal description", "legal desc", "legal")
    col_lot = find_col("lot")
    col_block = find_col("block")

    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        try:
            cells = tr.find_all("td")
            if len(cells) < 4:
                continue

            def cell_text(idx):
                if idx >= 0 and idx < len(cells):
                    return cells[idx].get_text(" ", strip=True)
                return ""

            grantor = cell_text(col_grantor) if col_grantor >= 0 else ""
            grantee = cell_text(col_grantee) if col_grantee >= 0 else ""
            doc_type = cell_text(col_doctype) if col_doctype >= 0 else dt["label"]
            filed_raw = cell_text(col_date) if col_date >= 0 else ""
            doc_num = cell_text(col_docnum) if col_docnum >= 0 else ""
            legal = cell_text(col_legal) if col_legal >= 0 else ""
            lot = cell_text(col_lot) if col_lot >= 0 else ""
            block = cell_text(col_block) if col_block >= 0 else ""

            # Build fuller legal description
            if lot or block:
                legal_parts = [legal]
                if lot:
                    legal_parts.append(f"Lot: {lot}")
                if block:
                    legal_parts.append(f"Block: {block}")
                legal = " ".join(filter(None, legal_parts))

            # Find the doc link for the detail page
            href = ""
            a = tr.find("a", href=True)
            if a:
                h = a["href"]
                href = h if h.startswith("http") else f"{CLERK_BASE}{h}"
                if not doc_num:
                    m = re.search(r"/doc/(\d+)", h)
                    if m:
                        doc_num = m.group(1)

            if not doc_num and not grantor:
                continue

            rec = Record(
                doc_num=doc_num.strip(),
                doc_type=doc_type or dt["label"],
                filed=_parse_date(filed_raw),
                cat=dt["cat"],
                cat_label=dt["label"],
                owner=_clean_name(grantor),
                grantee=_clean_name(grantee),
                legal=legal[:500],
                clerk_url=href or (f"{CLERK_BASE}/doc/{doc_num}" if doc_num else ""),
            )
            out.append(rec)
        except Exception:
            continue

    return out


async def _scrape_detail_page(page, rec: Record) -> None:
    """
    Visit /doc/{id} and extract property address, consideration amount,
    and any additional data from the detail panel.

    From the live portal, the detail page shows:
    - Document Number, Recorded Date, Instrument Date
    - Consideration (amount)
    - Parties (Grantor/Grantee with roles)
    - Legal Description (fuller version)
    - Property Address (at the bottom of the SUMMARY panel)
    """
    detail_url = f"{CLERK_BASE}/doc/{rec.doc_num}"
    await page.goto(detail_url, wait_until="domcontentloaded")
    await page.wait_for_timeout(2000)

    html = await page.content()
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    # Property Address — look for the section
    addr_section = soup.find(string=re.compile(r"Property Address", re.I))
    if addr_section:
        parent = addr_section.find_parent()
        if parent:
            # Get the next sibling or the text after "Property Address"
            next_el = parent.find_next_sibling()
            if next_el:
                addr_text = next_el.get_text(" ", strip=True)
            else:
                addr_text = parent.get_text(" ", strip=True)
                addr_text = re.sub(r"^.*Property Address\s*:?\s*", "", addr_text, flags=re.I)

            if addr_text and len(addr_text) > 5:
                parts = _parse_full_address(addr_text)
                rec.prop_address = parts.get("address", "")
                rec.prop_city = parts.get("city", "")
                rec.prop_zip = parts.get("zip", "")

    # Also try regex on the full text
    if not rec.prop_address:
        m = re.search(
            r"Property\s+Address\s*:?\s*(\d+[^,\n]{3,}?)(?:\s+(?:SAN ANTONIO|CONVERSE|HELOTES|"
            r"SCHERTZ|CIBOLO|SELMA|LIVE OAK|UNIVERSAL CITY|NEW BRAUNFELS|BOERNE|"
            r"ALAMO HEIGHTS|LEON VALLEY|CASTLE HILLS|WINDCREST|KIRBY|SOMERSET|"
            r"VON ORMY|FAIR OAKS RANCH|GARDEN RIDGE|HOLLYWOOD PARK|TERRELL HILLS|"
            r"OLMOS PARK|SHAVANO PARK|BALCONES HEIGHTS|HILL COUNTRY VILLAGE|"
            r"CHINA GROVE|GREY FOREST|SANDY OAKS|ELMENDORF|ST HEDWIG)\s*,?\s*"
            r"(?:TX|TEXAS)\s*(\d{5}))",
            text, re.I
        )
        if m:
            rec.prop_address = m.group(1).strip()
            rec.prop_zip = m.group(2) if m.group(2) else ""

    # Consideration / amount
    if rec.amount == 0.0:
        m = re.search(r"Consideration\s*:?\s*\$?\s*([\d,]+(?:\.\d{1,2})?)", text, re.I)
        if m:
            rec.amount = _to_float(m.group(1))

    # Fuller legal description
    m = re.search(r"Subdivision\s*[-–—]\s*Name\s*:?\s*([^\n]+)", text, re.I)
    if m and len(m.group(1).strip()) > len(rec.legal):
        rec.legal = m.group(1).strip()[:500]


def _parse_full_address(addr: str) -> Dict[str, str]:
    """Parse an address like '27114 HARMONY HILLS SAN ANTONIO TEXAS 78260'"""
    addr = addr.strip().upper()
    m = re.match(
        r"^(\d+\s+.+?)\s+"
        r"(SAN ANTONIO|CONVERSE|HELOTES|LEON VALLEY|UNIVERSAL CITY"
        r"|WINDCREST|BALCONES HEIGHTS|LIVE OAK|SELMA|SCHERTZ|CIBOLO"
        r"|SHAVANO PARK|HILL COUNTRY VILLAGE|CASTLE HILLS|ALAMO HEIGHTS"
        r"|KIRBY|CHINA GROVE|ST HEDWIG|SOMERSET|VON ORMY|ELMENDORF"
        r"|SANDY OAKS|GREY FOREST|FAIR OAKS RANCH|GARDEN RIDGE"
        r"|HOLLYWOOD PARK|TERRELL HILLS|OLMOS PARK|NEW BRAUNFELS|BOERNE)"
        r"\s+(?:TX|TEXAS)\s+(\d{5})",
        addr,
    )
    if m:
        return {"address": m.group(1).strip(), "city": m.group(2).strip().title(), "zip": m.group(3)}

    # Fallback: grab zip from end
    mz = re.search(r"(\d{5})\s*$", addr)
    # Try to split at TX/TEXAS
    mt = re.search(r"^(.+?)\s+(?:TX|TEXAS)\s+(\d{5})", addr)
    if mt:
        return {"address": mt.group(1).strip(), "city": "San Antonio", "zip": mt.group(2)}
    return {
        "address": addr[:mz.start()].strip() if mz else addr,
        "city": "San Antonio",
        "zip": mz.group(1) if mz else "",
    }


# ===================================================================
#  STAGE 2 — PARCEL ENRICHMENT (ArcGIS REST API)
# ===================================================================

def enrich_with_parcels(records: List[Record]) -> None:
    unique_owners: Dict[str, List[Record]] = {}
    for r in records:
        if not r.owner:
            continue
        key = r.owner.upper().strip()
        unique_owners.setdefault(key, []).append(r)

    log.info("Parcel: enriching %d unique owners", len(unique_owners))
    hit = 0

    for owner_name, recs in unique_owners.items():
        if all(r.mail_address for r in recs):
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
        except Exception:
            continue
        time.sleep(0.15)

    log.info("Parcel: enriched %d/%d unique owners", hit, len(unique_owners))


def _query_arcgis_owner(owner_name: str) -> Optional[Dict]:
    safe = owner_name.replace("'", "''")
    result = _arcgis_query(f"Owner = '{safe}'", 1)
    if result:
        return result
    prefix = safe[:20].rstrip()
    return _arcgis_query(f"Owner LIKE '{prefix}%'", 1)


def _arcgis_query(where: str, max_results: int = 1) -> Optional[Dict]:
    params = {
        "where": where, "outFields": ARCGIS_FIELDS,
        "returnGeometry": "false", "resultRecordCount": str(max_results), "f": "json",
    }
    r = requests.get(ARCGIS_PARCEL_QUERY, params=params, timeout=HTTP_TIMEOUT,
                     headers={"User-Agent": "Mozilla/5.0 BexarLeadScraper/3.0"})
    r.raise_for_status()
    features = r.json().get("features", [])
    if features:
        return features[0].get("attributes", {})
    return None


def _apply_parcel(rec: Record, parcel: Dict) -> None:
    if not rec.prop_address:
        situs = str(parcel.get("Situs") or "").strip()
        if situs:
            parts = _parse_full_address(situs)
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


# ===================================================================
#  STAGE 3 — SCORING
# ===================================================================

def score_all(records: List[Record]) -> None:
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    owner_cats: Dict[str, Set[str]] = {}
    for r in records:
        if r.owner:
            owner_cats.setdefault(r.owner.upper().strip(), set()).add(r.cat)
    for r in records:
        try:
            _score_one(r, owner_cats, week_ago)
        except Exception:
            r.score = 30


def _score_one(r: Record, owner_cats: Dict[str, Set[str]], week_ago: datetime) -> None:
    flags: List[str] = []
    flag = CAT_FLAG.get(r.cat)
    if flag:
        flags.append(flag)
    if re.search(r"\b(LLC|L\.L\.C|INC|CORP|CO\.|COMPANY|TRUST|LP|LTD|LLP|PARTNERSHIP)\b",
                 (r.owner or "").upper()):
        flags.append("LLC / corp owner")
    is_new = False
    try:
        if r.filed:
            fd = datetime.strptime(r.filed, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if fd >= week_ago:
                flags.append("New this week")
                is_new = True
    except ValueError:
        pass
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
#  STAGE 4 — DEDUP & MERGE
# ===================================================================

def dedup_records(new: List[Record], prior_path: Path = DATA_JSON) -> List[Record]:
    existing: Dict[str, Record] = {}
    if prior_path.exists():
        try:
            data = json.loads(prior_path.read_text(encoding="utf-8"))
            for item in data.get("records", []):
                r = Record(**{k: v for k, v in item.items() if k in Record.__dataclass_fields__})
                existing[_record_key(r)] = r
        except Exception as e:
            log.warning("Could not load prior records: %s", e)
    for r in new:
        existing[_record_key(r)] = r
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
    for p in [DASHBOARD_JSON, DATA_JSON, GHL_CSV]:
        p.parent.mkdir(parents=True, exist_ok=True)

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    payload = {
        "fetched_at": end.isoformat(),
        "source": "Bexar County Clerk + BCAD ArcGIS",
        "date_range": {"start": start.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d"), "days": days},
        "total": len(records),
        "with_address": sum(1 for r in records if r.prop_address or r.mail_address),
        "records": [asdict(r) for r in records],
    }
    text = json.dumps(payload, indent=2, default=str)
    DASHBOARD_JSON.write_text(text, encoding="utf-8")
    DATA_JSON.write_text(text, encoding="utf-8")
    log.info("Wrote %d records -> %s, %s", len(records), DASHBOARD_JSON, DATA_JSON)
    _write_ghl_csv(records)


def _write_ghl_csv(records: List[Record]) -> None:
    headers = [
        "First Name", "Last Name", "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL",
    ]
    with open(GHL_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in records:
            try:
                first, last = _split_name(r.owner)
                w.writerow([
                    first, last, r.mail_address, r.mail_city, r.mail_state, r.mail_zip,
                    r.prop_address, r.prop_city, r.prop_state, r.prop_zip,
                    r.cat_label, r.doc_type, r.filed, r.doc_num,
                    f"{r.amount:.2f}" if r.amount else "", r.score, "; ".join(r.flags),
                    "Bexar County Clerk", r.clerk_url,
                ])
            except Exception:
                continue
    log.info("Wrote GHL CSV -> %s (%d rows)", GHL_CSV, len(records))


# ===================================================================
#  HEALTH CHECK
# ===================================================================

def health_check(records: List[Record], days: int) -> bool:
    ok = True
    if len(records) == 0:
        log.warning("HEALTH: zero records")
        ok = False
    with_addr = sum(1 for r in records if r.prop_address or r.mail_address)
    try:
        RUN_LOG.parent.mkdir(parents=True, exist_ok=True)
        RUN_LOG.write_text(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total": len(records), "with_address": with_addr,
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
    return re.sub(r"\s+", " ", (s or "").strip())

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
    p = argparse.ArgumentParser(description="Bexar County Motivated Seller Lead Scraper v3")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--no-parcel", action="store_true")
    p.add_argument("--no-merge", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    log.info("=== Bexar County Lead Scraper v3 — %d-day lookback ===", args.days)

    try:
        raw_records = asyncio.run(scrape_clerk(args.days))
    except Exception as e:
        log.error("Clerk stage FAILED: %s\n%s", e, traceback.format_exc())
        raw_records = []

    log.info("Clerk: %d raw records", len(raw_records))

    if not args.no_parcel and raw_records:
        try:
            enrich_with_parcels(raw_records)
        except Exception as e:
            log.error("Parcel stage error: %s", e)

    score_all(raw_records)

    if args.no_merge:
        records = raw_records
    else:
        records = dedup_records(raw_records)

    records.sort(key=lambda r: (-r.score, r.filed or ""))
    write_outputs(records, args.days)
    healthy = health_check(records, args.days)

    log.info(
        "=== Done: %d leads, %d with address, avg score %.0f %s ===",
        len(records),
        sum(1 for r in records if r.prop_address),
        sum(r.score for r in records) / max(1, len(records)),
        "OK" if healthy else "CHECK LOGS",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
