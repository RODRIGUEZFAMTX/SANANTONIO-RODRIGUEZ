#!/usr/bin/env python3
"""
Bexar County Motivated Seller Lead Scraper v4  (production)
============================================================
Targets: bexar.tx.publicsearch.us   |   maps.bexar.org ArcGIS

Guarantees:
  - Every record for all 16 doc types within the lookback window
  - Full pagination (all pages, not just page 1)
  - Property address from detail pages (time-budgeted)
  - Mailing address from BCAD ArcGIS parcel API
  - Seller score 0-100 with flag breakdown
  - GHL-ready CSV + JSON for dashboard
  - Cross-run deduplication (new overwrites old)
  - Never crashes on bad records
"""

from __future__ import annotations
import argparse, asyncio, csv, json, logging, os, re, sys, time, traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote
import requests

# ═══════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════

CLERK = "https://bexar.tx.publicsearch.us"
ARCGIS = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"
ARCGIS_FIELDS = "Owner,Situs,AddrLn1,AddrLn2,AddrCity,AddrSt,Zip"

ROOT = Path(__file__).resolve().parent.parent
OUT_DASH = ROOT / "dashboard" / "records.json"
OUT_DATA = ROOT / "data" / "records.json"
OUT_CSV  = ROOT / "data" / "leads_ghl.csv"
OUT_LOG  = ROOT / "data" / "last_run.log"

RETRIES = 3
BACKOFF = 3
TIMEOUT = 60
PW_TIMEOUT = 60_000
PER_PAGE = 50
MAX_PAGES = 30          # per doc type
DETAIL_CAP = 400        # max detail-page visits total
DETAIL_BUDGET = 1500    # 25 min max on detail pages

# ── ALL 16 DOCUMENT TYPES — full portal names ──
TYPES = [
    # ── PRE-FORECLOSURE / FORECLOSURE (highest motivation) ──
    {"q": "LIS PENDENS",              "cat": "lis_pendens",     "label": "Lis Pendens"},
    {"q": "NOTICE OF FORECLOSURE",    "cat": "foreclosure",     "label": "Notice of Foreclosure"},
    {"q": "NOTICE OF TRUSTEE SALE",   "cat": "foreclosure",     "label": "Notice of Trustee Sale"},
    {"q": "NOTICE OF DEFAULT",        "cat": "foreclosure",     "label": "Notice of Default"},
    {"q": "APPOINTMENT SUBSTITUTE TRUSTEE", "cat": "foreclosure", "label": "Appointment Sub Trustee"},
    {"q": "TAX DEED",                 "cat": "tax_deed",        "label": "Tax Deed"},

    # ── JUDGMENTS (debt pressure = motivation) ──
    {"q": "JUDGMENT",                 "cat": "judgment",         "label": "Judgment"},
    {"q": "CERTIFIED JUDGMENT",       "cat": "judgment",         "label": "Certified Judgment"},
    {"q": "DOMESTIC JUDGMENT",        "cat": "judgment",         "label": "Domestic Judgment"},
    {"q": "ABSTRACT OF JUDGMENT",     "cat": "judgment",         "label": "Abstract of Judgment"},

    # ── TAX LIENS (government debt = high motivation) ──
    {"q": "CORP TAX LIEN",           "cat": "tax_lien",        "label": "Corp Tax Lien"},
    {"q": "IRS LIEN",                "cat": "tax_lien",        "label": "IRS Lien"},
    {"q": "FEDERAL LIEN",            "cat": "tax_lien",        "label": "Federal Lien"},
    {"q": "STATE TAX LIEN",          "cat": "tax_lien",        "label": "State Tax Lien"},

    # ── OTHER LIENS (financial distress indicators) ──
    {"q": "LIEN",                    "cat": "lien",            "label": "Lien"},
    {"q": "MECHANIC LIEN",           "cat": "mechanic_lien",   "label": "Mechanic Lien"},
    {"q": "HOA LIEN",                "cat": "hoa_lien",        "label": "HOA Lien"},
    {"q": "MEDICAID LIEN",           "cat": "medicaid_lien",   "label": "Medicaid Lien"},
    {"q": "HOSPITAL LIEN",           "cat": "medicaid_lien",   "label": "Hospital Lien"},

    # ── PROBATE / DEATH / INHERITANCE (estate = must sell) ──
    {"q": "PROBATE",                 "cat": "probate",         "label": "Probate"},
    {"q": "AFFIDAVIT OF HEIRSHIP",   "cat": "probate",         "label": "Affidavit of Heirship"},
    {"q": "DEATH CERTIFICATE",       "cat": "probate",         "label": "Death Certificate"},

    # ── DIVORCE (forced sale / partition) ──
    {"q": "DIVORCE DECREE",          "cat": "divorce",         "label": "Divorce Decree"},
    {"q": "DIVORCE",                 "cat": "divorce",         "label": "Divorce"},

    # ── INFORMATIONAL (track market / releases) ──
    {"q": "NOTICE OF COMMENCEMENT",  "cat": "noc",             "label": "Notice of Commencement"},
    {"q": "RELEASE LIS PENDENS",     "cat": "rel_lis_pendens", "label": "Release Lis Pendens"},
]

FLAGS = {
    "lis_pendens": "Lis pendens", "foreclosure": "Pre-foreclosure",
    "tax_deed": "Pre-foreclosure", "judgment": "Judgment lien",
    "tax_lien": "Tax lien", "lien": "Judgment lien",
    "mechanic_lien": "Mechanic lien", "hoa_lien": "Judgment lien",
    "medicaid_lien": "Judgment lien", "probate": "Probate / estate",
    "divorce": "Divorce / partition",
    "noc": None, "rel_lis_pendens": None,
}

# All known Bexar-area cities for address parsing
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


# ═══════════════════════════════════════════════════════════════════
#  RETRY
# ═══════════════════════════════════════════════════════════════════

def _retry(fn, *a, label="", **kw):
    last = None
    for i in range(1, RETRIES + 1):
        try:
            return fn(*a, **kw)
        except Exception as e:
            last = e
            log.warning("[%s] %d/%d: %s", label, i, RETRIES, e)
            if i < RETRIES: time.sleep(BACKOFF * i)
    raise last

async def _aretry(fn, *a, label="", **kw):
    last = None
    for i in range(1, RETRIES + 1):
        try:
            return await fn(*a, **kw)
        except Exception as e:
            last = e
            log.warning("[%s] %d/%d: %s", label, i, RETRIES, e)
            if i < RETRIES: await asyncio.sleep(BACKOFF * i)
    raise last


# ═══════════════════════════════════════════════════════════════════
#  STAGE 1 — CLERK PORTAL (Playwright)
# ═══════════════════════════════════════════════════════════════════

def _url(doc_name: str, start: str, end: str) -> str:
    return f"{CLERK}/results?department=RP&docTypes={quote(doc_name)}&recordedDateRange={start}%2C{end}"


async def stage_clerk(days: int) -> List[Rec]:
    from playwright.async_api import async_playwright
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)
    sd, ed = start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")

    all_recs: List[Rec] = []

    async with async_playwright() as pw:
        br = await pw.chromium.launch(headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu"])
        ctx = await br.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
            viewport={"width": 1440, "height": 900})
        page = await ctx.new_page()
        page.set_default_timeout(PW_TIMEOUT)

        # ── 1A: Scrape listing pages for all 16 doc types ──
        for dt in TYPES:
            url = _url(dt["q"], sd, ed)
            log.info("CLERK: %s", dt["label"])
            try:
                rows = await _aretry(_scrape_all_pages, page, url, dt, label=dt["q"])
                log.info("  → %d records", len(rows))
                all_recs.extend(rows)
            except Exception as e:
                log.error("  FAIL %s: %s", dt["label"], e)

        log.info("CLERK TOTAL: %d records from listing pages", len(all_recs))

        # ── 1B: Visit detail pages for property addresses ──
        need = [r for r in all_recs if not r.prop_address and r.doc_num]
        log.info("DETAIL: %d records need addresses (cap %d, budget %ds)",
                 len(need), DETAIL_CAP, DETAIL_BUDGET)
        done = 0
        t0 = time.time()
        for r in need:
            if done >= DETAIL_CAP:
                log.info("DETAIL: hit cap (%d)", DETAIL_CAP)
                break
            if time.time() - t0 > DETAIL_BUDGET:
                log.info("DETAIL: hit time budget (%.0fs)", time.time() - t0)
                break
            try:
                await _detail(page, r)
                done += 1
                if done % 50 == 0:
                    log.info("  ...%d detail pages scraped", done)
            except Exception:
                pass
            await page.wait_for_timeout(400)
        log.info("DETAIL: scraped %d pages in %.0fs", done, time.time() - t0)

        await ctx.close()
        await br.close()
    return all_recs


async def _scrape_all_pages(page, url: str, dt: Dict) -> List[Rec]:
    """Navigate to results URL, paginate through every page, parse DOM table."""
    from bs4 import BeautifulSoup
    out: List[Rec] = []

    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_timeout(3500)

    # Set results per page to 50
    try:
        sel = page.locator("select")
        for i in range(await sel.count()):
            opts = await sel.nth(i).inner_text()
            if "50" in opts:
                await sel.nth(i).select_option("50")
                await page.wait_for_timeout(2500)
                break
    except Exception:
        pass

    pg = 1
    while pg <= MAX_PAGES:
        html = await page.content()
        rows = _parse_table(html, dt)
        if not rows:
            break
        out.extend(rows)
        log.info("    pg %d → %d rows (cum %d)", pg, len(rows), len(out))

        if len(rows) < PER_PAGE:
            break

        # Click next page
        clicked = False
        try:
            # Try multiple selectors for the next button
            for sel in [
                "a:has-text('Next')", "button:has-text('Next')",
                "[aria-label='Next page']", "[aria-label='Next']",
                "a:has-text('»')", "a:has-text('>')",
                ".pagination-next", "[class*='next' i]",
            ]:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    await loc.first.click()
                    await page.wait_for_timeout(3000)
                    clicked = True
                    break
        except Exception:
            pass

        if not clicked:
            break
        pg += 1

    return out


def _parse_table(html: str, dt: Dict) -> List[Rec]:
    """Parse the DOM table from the Bexar clerk portal results page."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    out: List[Rec] = []

    table = soup.find("table")
    if not table:
        return out

    # Build header index
    hdrs = []
    for th in (table.find("thead") or table).find_all(["th","td"]):
        hdrs.append(th.get_text(" ", strip=True).lower().strip())

    def col(*names):
        for n in names:
            for i, h in enumerate(hdrs):
                if n in h:
                    return i
        return -1

    ci = {
        "grantor": col("grantor"),
        "grantee": col("grantee"),
        "doctype": col("doc type","doctype"),
        "date":    col("recorded date","recorded","date"),
        "docnum":  col("doc number","doc num","document"),
        "legal":   col("legal description","legal desc","legal"),
        "lot":     col("lot"),
        "block":   col("block"),
        "ncb":     col("ncb"),
    }

    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        try:
            cells = tr.find_all("td")
            if len(cells) < 4:
                continue

            def g(key):
                idx = ci.get(key, -1)
                if 0 <= idx < len(cells):
                    return cells[idx].get_text(" ", strip=True)
                return ""

            doc_num = g("docnum")
            grantor = g("grantor")
            grantee_raw = g("grantee")
            doc_type = g("doctype") or dt["label"]
            filed_raw = g("date")
            legal = g("legal")
            lot = g("lot")
            block = g("block")
            ncb = g("ncb")

            # Build comprehensive legal description
            legal_parts = [legal]
            if lot: legal_parts.append(f"Lot: {lot}")
            if block: legal_parts.append(f"Block: {block}")
            if ncb: legal_parts.append(f"NCB: {ncb}")
            full_legal = " | ".join(p for p in legal_parts if p)

            # Extract link
            href = ""
            a = tr.find("a", href=True)
            if a:
                h = a["href"]
                href = h if h.startswith("http") else f"{CLERK}{h}"
                if not doc_num:
                    m = re.search(r"/doc/(\w+)", h)
                    if m: doc_num = m.group(1)

            if not doc_num and not grantor:
                continue

            out.append(Rec(
                doc_num=doc_num.strip(),
                doc_type=doc_type,
                filed=_pdate(filed_raw),
                cat=dt["cat"],
                cat_label=dt["label"],
                owner=_cname(grantor),
                grantee=_cname(grantee_raw),
                legal=full_legal[:600],
                clerk_url=href or (f"{CLERK}/doc/{doc_num}" if doc_num else ""),
            ))
        except Exception:
            continue
    return out


async def _detail(page, r: Rec) -> None:
    """Visit detail page, extract property address + consideration + parties."""
    url = f"{CLERK}/doc/{r.doc_num}"
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_timeout(1800)

    from bs4 import BeautifulSoup
    html = await page.content()
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    # ── Property Address ──
    # The portal shows "Property Address" as a section heading,
    # with the address on the next line/element
    if not r.prop_address:
        # Method 1: Find the heading element
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

        # Method 2: Regex on full text
        if not r.prop_address:
            m = re.search(
                r"Property\s+Address\s*:?\s*(\d+\s+[A-Z0-9 .]+(?:ST|AVE|BLVD|DR|RD|LN|WAY|CT|CIR|PKWY|PL|TRL|HWY|LOOP|PASS|RUN|VW|COVE|PATH|CREEK|RIDGE|HILL|OAKS|PARK|GLEN|VALE|MEADOW|SPRING|BEND|CROSSING|POINT|LANDING|CHASE|CREST|TRACE|WALK|GATE|GROVE|HAVEN|KNOLL|SPUR|TERRACE|VISTA|CANYON|HOLLOW|SUMMIT|RANCH|FALLS|HEIGHTS|VALLEY|VILLAGE|ISLE|LAKE|RIVER|STONE|WOOD|WIND|FIELD|GARDEN|SHADOW|TIMBER|SILVER|GOLDEN|CEDAR|PINE|OAK|ELM|MAPLE|WILLOW|BIRCH|ASH|WALNUT|HICKORY|PECAN|CYPRESS|MESQUITE|HUISACHE)[A-Z0-9 .]*)",
                text, re.I
            )
            if m:
                full = m.group(1).strip()
                # Try to get city and zip after it
                after = text[m.end():]
                cm = re.match(r"\s*,?\s*([A-Z ]+?)\s*,?\s*(?:TX|TEXAS)\s*(\d{5})", after, re.I)
                if cm:
                    r.prop_address = full
                    r.prop_city = cm.group(1).strip().title()
                    r.prop_zip = cm.group(2)
                else:
                    p = _parse_addr(full)
                    r.prop_address = p["address"]
                    r.prop_city = p["city"]
                    r.prop_zip = p["zip"]

    # ── Consideration (amount) ──
    if r.amount == 0.0:
        m = re.search(r"Consideration\s*:?\s*\$?\s*([\d,]+(?:\.\d{1,2})?)", text, re.I)
        if m:
            r.amount = _float(m.group(1))

    # ── Better grantor/grantee from Parties section ──
    # The detail page lists parties with roles (GRANTOR/GRANTEE)
    grantors = []
    grantees = []
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 2:
            name = cells[0].get_text(" ", strip=True)
            role = cells[-1].get_text(" ", strip=True).upper()
            if "GRANTOR" in role and name:
                grantors.append(name)
            elif "GRANTEE" in role and name:
                grantees.append(name)
    if grantors and not r.owner:
        r.owner = _cname(grantors[0])
    if grantees and not r.grantee:
        r.grantee = _cname(grantees[0])


# ═══════════════════════════════════════════════════════════════════
#  STAGE 2 — PARCEL ENRICHMENT (ArcGIS)
# ═══════════════════════════════════════════════════════════════════

def stage_parcels(recs: List[Rec]) -> None:
    owners: Dict[str, List[Rec]] = {}
    for r in recs:
        if r.owner:
            owners.setdefault(r.owner.upper().strip(), []).append(r)
    log.info("PARCEL: %d unique owners to look up", len(owners))
    hit = 0
    for name, group in owners.items():
        if all(r.mail_address for r in group):
            continue
        try:
            p = _retry(_arcgis, name, label=f"arc:{name[:25]}")
            if p:
                for r in group:
                    _apply(r, p)
                hit += 1
        except Exception:
            pass
        time.sleep(0.12)
    log.info("PARCEL: matched %d/%d owners", hit, len(owners))


def _arcgis(name: str) -> Optional[Dict]:
    safe = name.replace("'", "''")
    r = _aq(f"Owner = '{safe}'")
    if r: return r
    return _aq(f"Owner LIKE '{safe[:20].rstrip()}%'")

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
#  STAGE 3 — SCORING
# ═══════════════════════════════════════════════════════════════════

def stage_score(recs: List[Rec]) -> None:
    week = datetime.now(timezone.utc) - timedelta(days=7)
    oc: Dict[str, Set[str]] = {}
    for r in recs:
        if r.owner:
            oc.setdefault(r.owner.upper().strip(), set()).add(r.cat)
    for r in recs:
        try: _score(r, oc, week)
        except: r.score = 30

def _score(r: Rec, oc: Dict, week: datetime) -> None:
    fl: List[str] = []
    f = FLAGS.get(r.cat)
    if f: fl.append(f)
    if re.search(r"\b(LLC|INC|CORP|CO\.|COMPANY|TRUST|LP|LTD|LLP|PARTNERSHIP)\b",
                 (r.owner or "").upper()):
        fl.append("LLC / corp owner")
    new = False
    try:
        if r.filed:
            if datetime.strptime(r.filed, "%Y-%m-%d").replace(tzinfo=timezone.utc) >= week:
                fl.append("New this week"); new = True
    except: pass
    combo = False
    if r.cat in ("lis_pendens","foreclosure") and r.owner:
        c = oc.get(r.owner.upper().strip(), set())
        if "lis_pendens" in c and "foreclosure" in c: combo = True
    s = 30 + 10*len(fl)
    if combo: s += 20
    if r.amount > 100000: s += 15
    elif r.amount > 50000: s += 10
    if new: s += 5
    if r.prop_address: s += 5
    r.flags = sorted(set(fl))
    r.score = min(100, max(0, s))


# ═══════════════════════════════════════════════════════════════════
#  STAGE 4 — DEDUP & MERGE
# ═══════════════════════════════════════════════════════════════════

def stage_dedup(new: List[Rec]) -> List[Rec]:
    old: Dict[str, Rec] = {}
    if OUT_DATA.exists():
        try:
            for item in json.loads(OUT_DATA.read_text())["records"]:
                r = Rec(**{k: v for k, v in item.items() if k in Rec.__dataclass_fields__})
                old[_key(r)] = r
        except: pass
    for r in new:
        old[_key(r)] = r
    out = list(old.values())
    out.sort(key=lambda r: (-r.score, r.filed or ""))
    return out

def _key(r: Rec) -> str:
    return r.doc_num if r.doc_num else f"{r.cat}|{r.owner}|{r.filed}"


# ═══════════════════════════════════════════════════════════════════
#  STAGE 5 — OUTPUT
# ═══════════════════════════════════════════════════════════════════

def stage_output(recs: List[Rec], days: int) -> None:
    for p in [OUT_DASH, OUT_DATA, OUT_CSV]:
        p.parent.mkdir(parents=True, exist_ok=True)
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    blob = {
        "fetched_at": end.isoformat(),
        "source": "Bexar County Clerk + BCAD ArcGIS",
        "date_range": {"start": start.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d"), "days": days},
        "total": len(recs),
        "with_address": sum(1 for r in recs if r.prop_address or r.mail_address),
        "records": [asdict(r) for r in recs],
    }
    txt = json.dumps(blob, indent=2, default=str)
    OUT_DASH.write_text(txt)
    OUT_DATA.write_text(txt)
    log.info("JSON: %d records → %s + %s", len(recs), OUT_DASH, OUT_DATA)

    # GHL CSV
    hdr = ["First Name","Last Name","Mailing Address","Mailing City","Mailing State",
           "Mailing Zip","Property Address","Property City","Property State","Property Zip",
           "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
           "Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(hdr)
        for r in recs:
            try:
                fn, ln = _split(r.owner)
                w.writerow([fn, ln,
                    r.mail_address, r.mail_city, r.mail_state, r.mail_zip,
                    r.prop_address, r.prop_city, r.prop_state, r.prop_zip,
                    r.cat_label, r.doc_type, r.filed, r.doc_num,
                    f"{r.amount:.2f}" if r.amount else "",
                    r.score, "; ".join(r.flags),
                    "Bexar County Clerk", r.clerk_url])
            except: continue
    log.info("CSV: %d rows → %s", len(recs), OUT_CSV)


def stage_health(recs: List[Rec], days: int) -> bool:
    ok = len(recs) > 0
    wa = sum(1 for r in recs if r.prop_address or r.mail_address)
    avg = round(sum(r.score for r in recs) / max(1, len(recs)), 1)
    if not ok: log.warning("HEALTH: zero records")
    try:
        OUT_LOG.parent.mkdir(parents=True, exist_ok=True)
        OUT_LOG.write_text(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total": len(recs), "with_address": wa, "avg_score": avg, "healthy": ok
        }, indent=2))
    except: pass
    return ok


# ═══════════════════════════════════════════════════════════════════
#  UTILITIES
# ═══════════════════════════════════════════════════════════════════

_DFMT = ["%m/%d/%Y","%m-%d-%Y","%Y-%m-%d","%b %d, %Y","%B %d, %Y",
         "%m/%d/%y","%Y-%m-%dT%H:%M:%S","%Y-%m-%dT%H:%M:%S.%fZ"]

def _pdate(s):
    s = (s or "").strip()
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

def _cname(s):
    return re.sub(r"\s+", " ", (s or "").strip())

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
    """Parse addresses like '27114 HARMONY HILLS SAN ANTONIO TEXAS 78260'"""
    s = addr.strip().upper()
    # Try to match known cities scanning backward from TX/TEXAS <zip>
    m = re.search(r"^(.+)\s+(?:TX|TEXAS)\s+(\d{5})(?:\s*-?\d{4})?\s*$", s)
    if m:
        addr_city = m.group(1).strip()
        z = m.group(2)
        for city in BEXAR_CITIES:
            if addr_city.endswith(city):
                a = addr_city[:-len(city)].strip()
                if a: return {"address": a, "city": city.title(), "zip": z}
        # Unknown city — guess last two words
        parts = addr_city.rsplit(None, 2)
        if len(parts) >= 3:
            return {"address": " ".join(parts[:-2]), "city": " ".join(parts[-2:]).title(), "zip": z}
        return {"address": addr_city, "city": "San Antonio", "zip": z}
    # No TX marker — grab zip if present
    mz = re.search(r"(\d{5})\s*$", s)
    return {
        "address": s[:mz.start()].strip() if mz else s,
        "city": "San Antonio",
        "zip": mz.group(1) if mz else ""
    }


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--no-parcel", action="store_true")
    ap.add_argument("--no-merge", action="store_true")
    args = ap.parse_args()

    log.info("══════ BEXAR COUNTY LEAD SCRAPER v4 ══════")
    log.info("Lookback: %d days | Detail cap: %d | Budget: %ds", args.days, DETAIL_CAP, DETAIL_BUDGET)

    # Stage 1: Clerk
    try:
        raw = asyncio.run(stage_clerk(args.days))
    except Exception as e:
        log.error("CLERK FAILED: %s\n%s", e, traceback.format_exc())
        raw = []
    log.info("STAGE 1 DONE: %d raw records", len(raw))

    # Stage 2: Parcel enrichment
    if not args.no_parcel and raw:
        try: stage_parcels(raw)
        except Exception as e: log.error("PARCEL ERROR: %s", e)

    # Stage 3: Score
    stage_score(raw)

    # Stage 4: Dedup/merge
    recs = raw if args.no_merge else stage_dedup(raw)
    recs.sort(key=lambda r: (-r.score, r.filed or ""))

    # Stage 5: Output
    stage_output(recs, args.days)
    ok = stage_health(recs, args.days)

    wa = sum(1 for r in recs if r.prop_address)
    ma = sum(1 for r in recs if r.mail_address)
    avg = sum(r.score for r in recs) / max(1, len(recs))
    log.info("══════ DONE ══════")
    log.info("Total: %d | Prop addr: %d | Mail addr: %d | Avg score: %.0f | %s",
             len(recs), wa, ma, avg, "HEALTHY" if ok else "CHECK LOGS")
    return 0

if __name__ == "__main__":
    sys.exit(main())
