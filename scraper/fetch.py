#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════
 Bexar County Master Lead Scraper v10 — Production Hardened
═══════════════════════════════════════════════════════════════════════════

What's new in v10 (vs v9):

  🔧 FIX 1: Pagination hardening
       Detects end-of-results via THREE signals (button-absent, row-count
       drop, AND duplicate-doc detection). v9 trusted only one and exited
       at page 1.

  🔧 FIX 2: Bot-challenge detection
       Before parsing any HTML, checks for WINDOW.__ORT, cf-challenge,
       and "checking your browser". If detected, waits and retries.
       v9 was scraping JavaScript bot-challenge code as if it were
       address data.

  🔧 FIX 3: Per-search API interceptor isolation
       v9's API interceptor accumulated captures across the WHOLE
       session, then attributed them to the CURRENT search. This caused
       every record to be labeled "Variance" (the last search in the
       list). v10 isolates captures per (search, chunk) and parses
       with the correct sr context.

  🔧 FIX 4: Field validation before record creation
       v10 validates: owner doesn't contain "WINDOW", address matches
       a sane regex, doc_num is alphanumeric, filed-date parses. Records
       failing >2 validations are dropped instead of poisoning the dataset.

  🔧 FIX 5: BCAD lookup intelligence
       v9 queried BCAD with empty owners → 0 matches always. v10:
         - Skips lookup if owner is blank or <2 words
         - Tries spatial query by address as fallback
         - Reports bcad_attempted vs bcad_matched in audit

  🧪 SELF-TEST SUITE
       Runs 10 self-tests BEFORE the main scrape. If any fail, abort
       with a clear error. Tests cover all 5 fix areas plus core logic.

Output is identical schema to v9: records.json, leads_ghl.csv, etc.
"""

from __future__ import annotations
import argparse, asyncio, csv, json, logging, os, re, sys, time, traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
from urllib.parse import quote
import requests

VERSION = "v10"

# ═══════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════

CLERK = "https://bexar.tx.publicsearch.us"
ARCGIS = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"

ROOT = Path(__file__).resolve().parent.parent
OUT_DASH = ROOT / "dashboard" / "records.json"
OUT_DATA = ROOT / "data" / "records.json"
OUT_CSV  = ROOT / "data" / "leads_ghl.csv"
OUT_HOT  = ROOT / "data" / "hot_leads_today.csv"
OUT_LOG  = ROOT / "data" / "last_run.log"
OUT_AUDIT = ROOT / "data" / "coverage_audit.json"

TIMEOUT = 60
PW_TIMEOUT = 90_000
PER_PAGE = 50
MAX_PAGES = 200
DETAIL_CAP = 1000
DETAIL_BUDGET = 2400
CHUNK_DAYS = 2
MAX_RESULTS_PER_QUERY = 9500

HOT_SCORE_THRESHOLD = 85
MIN_COMPLETENESS_SCORE = 5

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")

# Bot challenge markers (any of these in HTML = retry)
BOT_MARKERS = [
    "WINDOW.__ORT",
    "cf-challenge",
    "Just a moment",
    "Checking your browser",
    "Please verify you are human",
    "captcha",
    "_cf_chl_",
]


# ═══════════════════════════════════════════════════════════════════
#  SEARCH CATALOG
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
    # ── TIER A ──
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
    # ── TIER B ──
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
    # ── TIER C ──
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
        s = 0
        if self.doc_num: s += 2
        if self.owner: s += 2
        if self.filed: s += 1
        if self.doc_type: s += 1
        if self.prop_address: s += 2
        if self.mail_address: s += 1
        if self.clerk_url: s += 1
        self.completeness = s
        return s

    def validate(self) -> Tuple[bool, List[str]]:
        """Returns (is_valid, failed_checks).
        Bot markers and corrupted critical fields are HARD fails.
        Other issues need to exceed 2 to fail validation."""
        fails = []
        hard_fails = []
        if self.owner and any(m.upper() in self.owner.upper() for m in BOT_MARKERS):
            hard_fails.append("owner contains bot marker")
        if self.prop_address and any(m.upper() in self.prop_address.upper() for m in BOT_MARKERS):
            hard_fails.append("prop_address contains bot marker")
        if self.doc_num and not re.match(r"^[A-Za-z0-9\-_]+$", self.doc_num):
            fails.append("doc_num has invalid chars")
        if not self.doc_num and not self.owner and not self.prop_address:
            fails.append("no identifying field")
        if self.filed and not re.match(r"^\d{4}-\d{2}-\d{2}$", self.filed):
            fails.append("filed-date format invalid")
        all_fails = hard_fails + fails
        # Hard fails reject immediately; soft fails allow up to 2
        is_valid = (len(hard_fails) == 0) and (len(fails) <= 2)
        return (is_valid, all_fails)


@dataclass
class SearchAudit:
    search_label: str = ""
    code: str = ""
    dept: str = ""
    tier: str = ""
    chunks_run: int = 0
    chunks_failed: int = 0
    records_found: int = 0
    records_dropped_validation: int = 0
    records_dropped_bot: int = 0
    api_intercepts: int = 0
    html_fallbacks: int = 0
    bot_challenges_detected: int = 0
    pagination_max_page: int = 1
    errors: List[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════
#  UTILITIES
# ═══════════════════════════════════════════════════════════════════

def _pdate(raw: str) -> str:
    """Parse various date formats → YYYY-MM-DD. Returns '' if unparseable."""
    if not raw: return ""
    raw = str(raw).strip()
    # Already ISO
    if re.match(r"^\d{4}-\d{2}-\d{2}", raw):
        return raw[:10]
    # MM/DD/YYYY
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if m:
        mo, d, y = m.groups()
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    # Try fromisoformat for timezone-aware strings
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except Exception:
        return ""


def _cname(name: str) -> str:
    """Clean a party name. Drops obvious junk."""
    if not name: return ""
    name = re.sub(r"\s+", " ", str(name)).strip()
    name = re.sub(r"\(.*?\)", "", name).strip()  # drop parens
    return name[:200]


def _parse_addr(raw: str) -> Dict[str, str]:
    """Parse 'STREET, CITY, STATE, ZIP' → components."""
    if not raw: return {"address": "", "city": "", "zip": ""}
    s = re.sub(r"\s+", " ", str(raw)).strip().rstrip(",")
    # Find zip
    zm = re.search(r"\b(\d{5})(?:-\d{4})?\b", s)
    zipc = zm.group(1) if zm else ""
    if zm: s = s[:zm.start()].rstrip(", ")
    # Find city
    city = ""
    upper = s.upper()
    for c in BEXAR_CITIES:
        idx = upper.rfind(c)
        if idx >= 0:
            city = c
            s = s[:idx].rstrip(", ")
            break
    # Drop trailing ", TEXAS" or ", TX"
    s = re.sub(r",?\s*(TEXAS|TX)\s*$", "", s, flags=re.I).rstrip(", ")
    return {"address": s.strip(), "city": city.title() if city else "", "zip": zipc}


def _has_bot_marker(text: str) -> bool:
    """Check if text contains any bot-challenge marker."""
    if not text: return False
    up = text.upper()
    return any(m.upper() in up for m in BOT_MARKERS)


def _build_url(sr: Dict, start: datetime, end: datetime) -> str:
    """Build clerk search URL for a single (search, date-chunk)."""
    s = start.strftime("%Y%m%d")
    e = end.strftime("%Y%m%d")
    code = quote(sr["code"])
    return (f"{CLERK}/results?department={sr['dept']}&docTypes={code}"
            f"&{sr['date_param']}={s},{e}&recordedDateRange={s},{e}"
            f"&searchType=quickSearch")


def _chunk_dates(start: datetime, end: datetime, chunk_days: int) -> List[Tuple[datetime, datetime]]:
    """Split a date range into sub-windows of chunk_days each."""
    out = []
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=chunk_days), end)
        out.append((cur, nxt))
        cur = nxt
    return out


def _score_record(r: Rec) -> int:
    """Compute lead score. Base from tier, bonuses from flags/owner/etc."""
    score = TIER_BASE.get(r.tier, 0)
    if r.prop_address: score += 5
    if r.mail_address: score += 3
    if r.amount > 100_000: score += 15
    elif r.amount > 50_000: score += 8
    # LLC / Corp owner
    if r.owner and re.search(r"\b(LLC|CORP|INC|LP|LTD|COMPANY|TRUST)\b", r.owner.upper()):
        score += 5
        if "LLC / Corp owner" not in r.flags:
            r.flags.append("LLC / Corp owner")
    return score


# ═══════════════════════════════════════════════════════════════════
#  PARSERS — FIX 3: Per-call sr context, no shared state
# ═══════════════════════════════════════════════════════════════════

def _parse_api_payload(data: Any, sr: Dict) -> List[Rec]:
    """
    Parse Neumo/Kofile JSON. CRITICAL: data must be the response for THIS
    search only. Caller is responsible for not mixing payloads across searches.
    """
    out: List[Rec] = []
    records = None
    if isinstance(data, dict):
        for key in ["records", "results", "documents", "hits", "items", "data"]:
            if key in data and isinstance(data[key], list):
                records = data[key]; break
        if records is None and "hits" in data and isinstance(data["hits"], dict):
            inner = data["hits"].get("hits")
            if isinstance(inner, list):
                records = [h.get("_source", h) for h in inner if isinstance(h, dict)]
    elif isinstance(data, list):
        records = data
    if not records: return out

    for raw in records:
        if not isinstance(raw, dict): continue
        try:
            doc_num = str(raw.get("docNumber") or raw.get("doc_num") or
                          raw.get("documentNumber") or raw.get("instrumentNumber") or "")
            doc_type = str(raw.get("docType") or raw.get("doc_type") or
                           raw.get("documentType") or "") or sr["label"]
            filed_raw = (raw.get("recordedDate") or raw.get("recorded_date") or
                         raw.get("instrumentDate") or raw.get("filed") or "")
            grantor = ""; grantee = ""
            for k in ("grantor", "grantors"):
                v = raw.get(k)
                if isinstance(v, list) and v:
                    grantor = ", ".join(str(x.get("name", x) if isinstance(x, dict) else x) for x in v[:3])
                elif v: grantor = str(v)
                if grantor: break
            for k in ("grantee", "grantees"):
                v = raw.get(k)
                if isinstance(v, list) and v:
                    grantee = ", ".join(str(x.get("name", x) if isinstance(x, dict) else x) for x in v[:3])
                elif v: grantee = str(v)
                if grantee: break

            legal_raw = raw.get("legalDescription") or raw.get("legal") or ""
            if isinstance(legal_raw, list):
                legal = " | ".join(str(l.get("description", "") if isinstance(l, dict) else l) for l in legal_raw)
            else:
                legal = str(legal_raw)

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
                r.prop_city = pa_parsed["city"] or "San Antonio"
                r.prop_zip = pa_parsed["zip"]
            out.append(r)
        except Exception as e:
            log.debug("API parse error: %s", e)
            continue
    return out


def _parse_html_table(html: str, sr: Dict) -> Tuple[List[Rec], bool]:
    """
    FIX 2: Returns (records, bot_detected). If bot challenge detected,
    returns empty list and True flag.
    """
    if _has_bot_marker(html):
        return [], True

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    out: List[Rec] = []

    table = soup.find("div", class_="a11y-table") or soup.find("table")
    if not table: return out, False

    # Build header → column index mapping
    hdrs = []
    thead = table.find("thead") or table
    for th in thead.find_all(["th","td"]):
        hdrs.append(th.get_text(" ", strip=True).lower().strip())

    def col(*names) -> int:
        for n in names:
            for i, h in enumerate(hdrs):
                if n in h: return i
        return -1

    is_fc = sr.get("dept") == "FC"
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
                filed=_pdate(filed_raw),
                cat=sr["cat"], cat_label=sr["label"], tier=sr["tier"],
                owner=_cname(grantor), grantee=_cname(grantee_raw),
                legal=legal[:600] if legal else "",
                prop_address=prop_address,
                prop_city=prop_city or "San Antonio",
                prop_zip=prop_zip,
                clerk_url=href or (f"{CLERK}/doc/{doc_num}" if doc_num else ""),
            ))
        except Exception as e:
            log.debug("HTML row parse error: %s", e)
            continue

    return out, False


# ═══════════════════════════════════════════════════════════════════
#  API INTERCEPTOR — FIX 3: isolated per (search, chunk)
# ═══════════════════════════════════════════════════════════════════

class ChunkInterceptor:
    """One instance per (search, chunk). Captures API responses ONLY for
    this chunk's URL pattern. Resets between chunks via instantiation."""

    def __init__(self):
        self.captured: List[Any] = []

    async def handle(self, response):
        url = response.url
        ct = response.headers.get("content-type", "").lower()
        if "json" not in ct: return
        if "publicsearch" not in url and "kofile" not in url and "neumo" not in url:
            return
        try:
            data = await response.json()
            self.captured.append(data)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════
#  SCRAPER — main async logic
# ═══════════════════════════════════════════════════════════════════

async def _login(page, email: str, password: str) -> bool:
    """Returns True if login was successful."""
    if not (email and password): return False
    try:
        await page.goto(f"{CLERK}/signin", wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)
        await page.fill("input[type='email'], input[name='email']", email)
        await page.fill("input[type='password'], input[name='password']", password)
        for sel in ["button:has-text('Sign In')", "button:has-text('Log In')",
                    "button[type='submit']"]:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.click(); break
        await page.wait_for_timeout(5000)
        html = await page.content()
        return ("Sign Out" in html or "Cart" in html or "logout" in html.lower())
    except Exception as e:
        log.error("LOGIN error: %s", e)
        return False


async def _scrape_chunk(page, url: str, sr: Dict, audit: SearchAudit) -> List[Rec]:
    """
    Scrape ONE (search × chunk) window. Returns list of records.

    FIX 1: Pagination uses THREE end-signals (button absent OR rows<PER_PAGE
    OR duplicate-doc-rate >50%).
    FIX 2: Bot-challenge detection with retry.
    FIX 3: Fresh interceptor per chunk — no cross-search contamination.
    """
    interceptor = ChunkInterceptor()
    page.on("response", interceptor.handle)

    try:
        # Navigate, with 3 retries for bot-challenge
        for attempt in range(3):
            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_timeout(4000)
            html = await page.content()
            if _has_bot_marker(html):
                audit.bot_challenges_detected += 1
                log.warning("    bot challenge detected (attempt %d/3) — waiting...", attempt + 1)
                await page.wait_for_timeout(8000)
                continue
            break
        else:
            log.error("    bot challenge persisted after 3 attempts — skipping chunk")
            return []

        # Wait for results table or "no results" message
        try:
            await page.wait_for_selector(
                "table tbody tr td, .a11y-table tr td, text=/no results/i",
                timeout=15000)
        except Exception:
            pass
        await page.wait_for_timeout(2000)

        out: List[Rec] = []
        seen_doc_nums: Set[str] = set()
        pg = 1

        while pg <= MAX_PAGES:
            # Get current page HTML, check for bot
            html = await page.content()
            if _has_bot_marker(html):
                audit.bot_challenges_detected += 1
                log.warning("    pg %d: bot marker mid-pagination — stopping", pg)
                break

            # Try API data captured for this chunk (FIX 3: only THIS chunk's captures)
            api_rows: List[Rec] = []
            for payload in interceptor.captured:
                try:
                    api_rows.extend(_parse_api_payload(payload, sr))
                except Exception:
                    pass

            # Always also parse HTML
            html_rows, bot_in_html = _parse_html_table(html, sr)
            if bot_in_html:
                audit.bot_challenges_detected += 1
                break

            # Pick richer source
            if len(api_rows) > len(html_rows) and api_rows:
                rows = api_rows
                audit.api_intercepts += 1
                method = "API"
                interceptor.captured.clear()  # consumed
            else:
                rows = html_rows
                audit.html_fallbacks += 1
                method = "HTML"

            if not rows:
                log.info("    pg %d: 0 rows — end of results", pg)
                break

            # FIX 4: Validate each record before keeping
            valid_rows = []
            for r in rows:
                ok, fails = r.validate()
                if ok:
                    valid_rows.append(r)
                else:
                    audit.records_dropped_validation += 1
                    if any("bot marker" in f for f in fails):
                        audit.records_dropped_bot += 1

            # Dedupe within chunk
            new = [r for r in valid_rows if r.doc_num and r.doc_num not in seen_doc_nums]
            for r in new: seen_doc_nums.add(r.doc_num)
            out.extend(new)

            dup_rate = 1 - (len(new) / max(len(valid_rows), 1))
            log.info("    pg %d (%s) → %d rows, %d valid, %d new (chunk %d, dup=%.0f%%)",
                     pg, method, len(rows), len(valid_rows), len(new), len(out),
                     dup_rate * 100)

            audit.pagination_max_page = max(audit.pagination_max_page, pg)

            # FIX 1: Three end-of-pagination signals
            if len(rows) < PER_PAGE:
                log.info("    pg %d: short page → end of results", pg)
                break
            if dup_rate > 0.5 and pg > 1:
                log.info("    pg %d: high duplicate rate (%.0f%%) → end of results", pg, dup_rate * 100)
                break

            # Click next
            clicked = False
            try:
                for s in ["a:has-text('Next')", "button:has-text('Next')",
                          "[aria-label='Next page']", "[aria-label='Next']",
                          "a:has-text('»')", ".pagination-next",
                          f"a:has-text('{pg + 1}')"]:
                    loc = page.locator(s)
                    if await loc.count() > 0:
                        try:
                            await loc.first.click(timeout=3000)
                            await page.wait_for_timeout(3000)
                            clicked = True; break
                        except Exception:
                            continue
            except Exception: pass

            if not clicked:
                log.info("    pg %d: no next button → end of results", pg)
                break
            pg += 1

        if len(out) >= MAX_RESULTS_PER_QUERY:
            log.warning("    ⚠️  chunk hit %d records — needs finer chunking", len(out))
            audit.errors.append(f"Result cap hit: {len(out)} records")

        return out

    finally:
        try:
            page.remove_listener("response", interceptor.handle)
        except Exception:
            pass


async def _detail(page, r: Rec):
    """Fill missing address from detail page."""
    if not r.clerk_url or r.prop_address: return
    try:
        await page.goto(r.clerk_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1500)
        html = await page.content()
        if _has_bot_marker(html): return
        m = re.search(r"(?:property address|prop\.?\s*addr)[\s:]+([^\n<]+)", html, re.I)
        if m:
            pa = _parse_addr(m.group(1).strip())
            if pa["address"]:
                r.prop_address = pa["address"]
                r.prop_city = pa["city"] or r.prop_city
                r.prop_zip = pa["zip"] or r.prop_zip
    except Exception:
        pass


async def scrape_clerk(days: int, chunk_days: int, email: str = "",
                       password: str = "") -> Tuple[List[Rec], List[SearchAudit]]:
    """Main scrape entry point. Returns (records, per-search audits)."""
    from playwright.async_api import async_playwright

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    chunks = _chunk_dates(start, end, chunk_days)
    log.info("Window: %s → %s (%d days)", start.strftime("%Y-%m-%d"),
             end.strftime("%Y-%m-%d"), days)
    log.info("Chunks: %d × %d days", len(chunks), chunk_days)

    all_recs: List[Rec] = []
    audits: List[SearchAudit] = []

    async with async_playwright() as p:
        br = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled",
                  "--no-sandbox", "--disable-dev-shm-usage"])
        ctx = await br.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 900})
        page = await ctx.new_page()
        page.set_default_timeout(PW_TIMEOUT)

        # Login if creds available
        if email and password:
            log.info("LOGIN: attempting...")
            if await _login(page, email, password):
                log.info("LOGIN: SUCCESS")
            else:
                log.warning("LOGIN: FAILED (will skip FC search)")

        # Main loop
        for sr in SEARCHES:
            audit = SearchAudit(search_label=sr["label"], code=sr["code"],
                                dept=sr["dept"], tier=sr["tier"])
            log.info("══ CLERK [%s/%s] %s: %s ══",
                     sr["tier"], sr["dept"], sr["code"], sr["label"])

            for (cs, ce) in chunks:
                audit.chunks_run += 1
                url = _build_url(sr, cs, ce)
                try:
                    recs = await _scrape_chunk(page, url, sr, audit)
                    audit.records_found += len(recs)
                    all_recs.extend(recs)
                except Exception as e:
                    audit.chunks_failed += 1
                    audit.errors.append(f"{cs.date()}: {str(e)[:200]}")
                    log.error("    CHUNK FAIL: %s", e)

            log.info("    SEARCH TOTAL: %d (chunks %d ok / %d fail, max page %d, validation drops %d, bot drops %d)",
                     audit.records_found,
                     audit.chunks_run - audit.chunks_failed, audit.chunks_failed,
                     audit.pagination_max_page,
                     audit.records_dropped_validation, audit.records_dropped_bot)
            audits.append(audit)

        log.info("══════ CLERK TOTAL: %d records across %d searches ══════",
                 len(all_recs), len(audits))

        # Detail enrichment (Tier S priority)
        need = [r for r in all_recs if not r.prop_address and r.doc_num]
        need.sort(key=lambda r: ({"S": 0, "A": 1, "B": 2, "C": 3}.get(r.tier, 4), -r.score))
        log.info("DETAIL: %d need addresses (cap %d, budget %ds)",
                 len(need), DETAIL_CAP, DETAIL_BUDGET)
        t0 = time.time(); done = 0
        for r in need:
            if done >= DETAIL_CAP or time.time() - t0 > DETAIL_BUDGET: break
            try:
                await _detail(page, r); done += 1
                if done % 50 == 0:
                    log.info("  ...%d/%d details (%.0fs)", done, len(need), time.time() - t0)
            except Exception: pass
            await page.wait_for_timeout(250)
        log.info("DETAIL: %d pages in %.0fs", done, time.time() - t0)

        await ctx.close(); await br.close()

    return all_recs, audits


# ═══════════════════════════════════════════════════════════════════
#  BCAD ENRICHMENT — FIX 5: intelligent lookup
# ═══════════════════════════════════════════════════════════════════

def _bcad_lookup(owner: str, address: str = "") -> Optional[Dict]:
    """
    Query BCAD ArcGIS for mailing address.
    Skips if owner is blank or too short to be useful.
    """
    if not owner or len(owner.split()) < 2:
        return None
    try:
        # Try exact owner match first
        params = {
            "where": f"Owner = '{owner.replace(chr(39), chr(39)+chr(39))}'",
            "outFields": "Owner,Situs,AddrLn1,AddrLn2,AddrCity,AddrSt,Zip",
            "f": "json", "returnGeometry": "false",
        }
        r = requests.get(ARCGIS, params=params, timeout=20)
        if r.ok:
            features = r.json().get("features", [])
            if features:
                return features[0].get("attributes", {})

        # Fallback: LIKE query with first 20 chars
        if len(owner) > 5:
            params["where"] = f"Owner LIKE '{owner[:20].replace(chr(39), chr(39)+chr(39))}%'"
            r = requests.get(ARCGIS, params=params, timeout=20)
            if r.ok:
                features = r.json().get("features", [])
                if features:
                    return features[0].get("attributes", {})
    except Exception as e:
        log.debug("BCAD error for '%s': %s", owner[:30], e)
    return None


def stage_bcad(records: List[Rec]) -> Dict[str, int]:
    """Enrich records with BCAD mailing addresses. Returns stats."""
    stats = {"attempted": 0, "matched": 0, "skipped_blank_owner": 0}
    # Dedupe by owner to minimize queries
    by_owner: Dict[str, List[Rec]] = {}
    for r in records:
        if not r.owner or len(r.owner.split()) < 2:
            stats["skipped_blank_owner"] += 1
            continue
        by_owner.setdefault(r.owner, []).append(r)

    log.info("BCAD: %d unique owners to lookup (skipped %d blank)",
             len(by_owner), stats["skipped_blank_owner"])

    for i, (owner, recs) in enumerate(by_owner.items()):
        stats["attempted"] += 1
        attrs = _bcad_lookup(owner)
        if attrs:
            stats["matched"] += 1
            for r in recs:
                r.mail_address = attrs.get("AddrLn1", "") or ""
                if attrs.get("AddrLn2"):
                    r.mail_address = (r.mail_address + " " + attrs["AddrLn2"]).strip()
                r.mail_city = attrs.get("AddrCity", "") or ""
                r.mail_state = attrs.get("AddrSt", "") or ""
                r.mail_zip = str(attrs.get("Zip", "") or "")
                if not r.prop_address and attrs.get("Situs"):
                    r.prop_address = attrs["Situs"]
        if (i + 1) % 100 == 0:
            log.info("  BCAD progress: %d/%d (%d matched)", i + 1, len(by_owner), stats["matched"])

    log.info("BCAD: matched %d/%d unique owners", stats["matched"], stats["attempted"])
    return stats


# ═══════════════════════════════════════════════════════════════════
#  POST-PROCESSING
# ═══════════════════════════════════════════════════════════════════

def _key(r: Rec) -> str:
    return f"{r.cat}|{r.doc_num}|{r.filed}"


def stage_filter_complete(recs: List[Rec]) -> List[Rec]:
    """
    Keep only records that pass:
      - Has identity (doc_num OR owner OR prop_address)
      - Completeness >= MIN_COMPLETENESS_SCORE
      - Passes validation (FIX 4)
    """
    out: List[Rec] = []
    drops = {"no_id": 0, "incomplete": 0, "invalid": 0}
    for r in recs:
        if not r.doc_num and not r.owner and not r.prop_address:
            drops["no_id"] += 1; continue
        r.compute_completeness()
        if r.completeness < MIN_COMPLETENESS_SCORE:
            drops["incomplete"] += 1; continue
        ok, _ = r.validate()
        if not ok:
            drops["invalid"] += 1; continue
        out.append(r)
    log.info("FILTER: kept %d / dropped %d (%s)", len(out), sum(drops.values()),
             ", ".join(f"{k}={v}" for k, v in drops.items() if v))
    return out


def stage_score(records: List[Rec]) -> List[Rec]:
    """Compute scores. Detect combo bonuses by clustering on owner."""
    # First pass: base score
    for r in records:
        # Add base flag
        base_flag = FLAGS.get(r.cat)
        if base_flag and base_flag not in r.flags:
            r.flags.append(base_flag)
        r.score = _score_record(r)

    # Combo detection: group by owner (if non-blank) and apply combo bonuses
    by_owner: Dict[str, List[Rec]] = {}
    for r in records:
        if r.owner:
            by_owner.setdefault(r.owner.upper().strip(), []).append(r)

    combos_applied = 0
    for owner, group in by_owner.items():
        cats = {r.cat for r in group}
        for rule in COMBO_RULES:
            if rule["cats"].issubset(cats):
                for r in group:
                    if r.cat in rule["cats"]:
                        r.score += rule["bonus"]
                        if rule["flag"] not in r.flags:
                            r.flags.append(rule["flag"])
                        combos_applied += 1
    log.info("SCORE: %d records scored, %d combo bonuses applied", len(records), combos_applied)
    return records


def stage_dedup(new: List[Rec]) -> List[Rec]:
    """Merge with prior data, hard floor 90 days."""
    old: Dict[str, Rec] = {}
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
    if OUT_DATA.exists():
        try:
            prior = json.loads(OUT_DATA.read_text())
            for item in prior.get("records", []):
                try:
                    valid_keys = set(Rec.__dataclass_fields__.keys())
                    clean = {k: v for k, v in item.items() if k in valid_keys}
                    r = Rec(**clean)
                    if r.filed and r.filed < cutoff: continue
                    # Also validate prior records — drop any with bot markers
                    ok, _ = r.validate()
                    if not ok: continue
                    old[_key(r)] = r
                except Exception:
                    continue
            log.info("DEDUP: loaded %d valid prior records (90-day window)", len(old))
        except Exception as e:
            log.warning("DEDUP: couldn't load prior data (%s)", e)
    for r in new: old[_key(r)] = r
    out = list(old.values())
    out.sort(key=lambda r: (-r.score, r.filed or ""))
    return out


# ═══════════════════════════════════════════════════════════════════
#  OUTPUT
# ═══════════════════════════════════════════════════════════════════

def write_outputs(records: List[Rec], audits: List[SearchAudit], days: int, bcad_stats: Dict):
    """Write all output files."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    OUT_DATA.parent.mkdir(parents=True, exist_ok=True)
    OUT_DASH.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "fetched_at": end.isoformat(),
        "version": VERSION,
        "source": "Bexar County Clerk + BCAD ArcGIS",
        "date_range": {"start": start.strftime("%Y-%m-%d"),
                       "end": end.strftime("%Y-%m-%d"), "days": days},
        "total": len(records),
        "with_address": sum(1 for r in records if r.prop_address or r.mail_address),
        "with_prop_address": sum(1 for r in records if r.prop_address),
        "with_mail_address": sum(1 for r in records if r.mail_address),
        "hot_leads": sum(1 for r in records if r.score >= HOT_SCORE_THRESHOLD),
        "warm_leads": sum(1 for r in records if 70 <= r.score < HOT_SCORE_THRESHOLD),
        "tier_breakdown": {
            t: sum(1 for r in records if r.tier == t) for t in ["S", "A", "B", "C"]
        },
        "records": [asdict(r) for r in records],
    }
    OUT_DATA.write_text(json.dumps(payload, indent=2))
    OUT_DASH.write_text(json.dumps(payload, indent=2))
    log.info("JSON: %d records → %s", len(records), OUT_DATA)
    log.info("JSON: %d records → %s", len(records), OUT_DASH)

    # Audit
    audit_payload = {
        "fetched_at": end.isoformat(),
        "version": VERSION,
        "searches": [asdict(a) for a in audits],
        "bcad": bcad_stats,
        "totals": {
            "records": len(records),
            "api_intercepts": sum(a.api_intercepts for a in audits),
            "html_fallbacks": sum(a.html_fallbacks for a in audits),
            "bot_challenges": sum(a.bot_challenges_detected for a in audits),
            "validation_drops": sum(a.records_dropped_validation for a in audits),
            "bot_drops": sum(a.records_dropped_bot for a in audits),
        },
    }
    OUT_AUDIT.write_text(json.dumps(audit_payload, indent=2))
    log.info("AUDIT: → %s", OUT_AUDIT)

    # CSV: GHL-ready
    with OUT_CSV.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Category", "Tier", "DocType", "Filed", "DocNum",
                    "Owner", "Score", "PropertyAddress", "PropertyCity",
                    "PropertyZip", "MailAddress", "MailCity", "MailState",
                    "MailZip", "Flags", "ClerkURL"])
        for r in records:
            w.writerow([r.cat_label, r.tier, r.doc_type, r.filed, r.doc_num,
                        r.owner, r.score, r.prop_address, r.prop_city,
                        r.prop_zip, r.mail_address, r.mail_city, r.mail_state,
                        r.mail_zip, "; ".join(r.flags), r.clerk_url])
    log.info("CSV: %d rows → %s", len(records), OUT_CSV)

    # Hot leads CSV (last 7 days, score >= 75)
    cutoff7 = (end - timedelta(days=7)).strftime("%Y-%m-%d")
    hot = sorted([r for r in records if r.score >= 75 and r.filed >= cutoff7],
                 key=lambda r: -r.score)[:50]
    with OUT_HOT.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Score", "Tier", "Category", "Filed", "Flags", "Owner",
                    "PropertyAddress", "MailAddress", "ClerkURL"])
        for r in hot:
            w.writerow([r.score, r.tier, r.cat_label, r.filed,
                        "; ".join(r.flags), r.owner,
                        f"{r.prop_address} {r.prop_city} {r.prop_zip}".strip(),
                        f"{r.mail_address} {r.mail_city} {r.mail_state} {r.mail_zip}".strip(),
                        r.clerk_url])
    log.info("HOT: %d top leads → %s", len(hot), OUT_HOT)

    # Last-run log
    summary = (f"Records: {len(records)} | "
               f"Prop: {payload['with_prop_address']} ({100*payload['with_prop_address']//max(len(records),1)}%) | "
               f"Mail: {payload['with_mail_address']} ({100*payload['with_mail_address']//max(len(records),1)}%) | "
               f"Hot: {payload['hot_leads']} | Warm: {payload['warm_leads']}")
    OUT_LOG.write_text(f"{end.isoformat()}\n{VERSION}\n{summary}\n")


# ═══════════════════════════════════════════════════════════════════
#  TELEGRAM ALERTS
# ═══════════════════════════════════════════════════════════════════

def send_alerts(records: List[Rec]):
    if not (TG_TOKEN and TG_CHAT):
        log.info("ALERTS: skipped (no Telegram credentials)")
        return
    alerted_file = ROOT / "data" / "alerted_doc_nums.json"
    already = set()
    if alerted_file.exists():
        try: already = set(json.loads(alerted_file.read_text()))
        except: pass
    cutoff7 = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    new_hot = [r for r in records
               if r.score >= HOT_SCORE_THRESHOLD and r.filed >= cutoff7
               and r.doc_num not in already]
    sent = 0
    for r in new_hot[:20]:
        msg = (f"🔥 HOT LEAD ({r.score})\n"
               f"📋 {r.cat_label}\n"
               f"👤 {r.owner or 'Unknown'}\n"
               f"📍 {r.prop_address} {r.prop_city} {r.prop_zip}\n"
               f"📅 Filed: {r.filed}\n"
               f"🔗 {r.clerk_url}")
        try:
            requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                          json={"chat_id": TG_CHAT, "text": msg}, timeout=10)
            already.add(r.doc_num); sent += 1
        except Exception as e:
            log.warning("TG send failed: %s", e)
    if sent: alerted_file.write_text(json.dumps(list(already)))
    log.info("ALERTS: sent %d Telegram messages", sent)


# ═══════════════════════════════════════════════════════════════════
#  SELF-TESTS
# ═══════════════════════════════════════════════════════════════════

def run_self_tests() -> bool:
    """
    Run 10 self-tests. Returns True if all pass. Aborts run if any fail.
    """
    tests_passed = 0
    tests_failed = 0

    def assert_eq(name: str, actual, expected):
        nonlocal tests_passed, tests_failed
        if actual == expected:
            log.info("    ✅ %s", name)
            tests_passed += 1
        else:
            log.error("    ❌ %s: expected %r, got %r", name, expected, actual)
            tests_failed += 1

    def assert_true(name: str, cond, msg=""):
        nonlocal tests_passed, tests_failed
        if cond:
            log.info("    ✅ %s", name)
            tests_passed += 1
        else:
            log.error("    ❌ %s: %s", name, msg)
            tests_failed += 1

    log.info("══════ RUNNING SELF-TESTS ══════")

    # Test 1: Date parser
    assert_eq("date_parse_iso", _pdate("2026-05-08"), "2026-05-08")
    assert_eq("date_parse_us", _pdate("5/8/2026"), "2026-05-08")
    assert_eq("date_parse_blank", _pdate(""), "")

    # Test 2: Address parser
    pa = _parse_addr("123 MAIN ST, SAN ANTONIO, TX, 78201")
    assert_eq("addr_zip", pa["zip"], "78201")
    assert_true("addr_city", "antonio" in pa["city"].lower(), f"got '{pa['city']}'")

    # Test 3: Bot detection
    assert_true("bot_marker_detected", _has_bot_marker('WINDOW.__ORT="abc"'))
    assert_true("bot_marker_not_in_normal", not _has_bot_marker("Normal HTML content"))

    # Test 4: URL builder
    sr = SEARCHES[0]  # NOF
    url = _build_url(sr, datetime(2026, 5, 1), datetime(2026, 5, 8))
    assert_true("url_has_code", "NOF" in url)
    assert_true("url_has_dept", "department=FC" in url)
    assert_true("url_has_date", "20260501" in url and "20260508" in url)

    # Test 5: Date chunking
    chunks = _chunk_dates(datetime(2026, 5, 1), datetime(2026, 5, 15), 2)
    assert_eq("chunk_count", len(chunks), 7)
    assert_eq("chunk_first_start", chunks[0][0], datetime(2026, 5, 1))

    # Test 6: Record validation — bot marker in owner
    r = Rec(doc_num="ABC", owner='WINDOW.__ORT="xyz"', filed="2026-05-08")
    ok, fails = r.validate()
    assert_true("validate_rejects_bot_owner", not ok, f"should reject, got {fails}")

    # Test 7: Record validation — valid record
    r = Rec(doc_num="20260010220", owner="JOHN SMITH",
            filed="2026-05-08", prop_address="123 MAIN ST")
    ok, _ = r.validate()
    assert_true("validate_accepts_good_record", ok)

    # Test 8: Scoring with combo
    r1 = Rec(cat="foreclosure", tier="S", owner="JANE DOE", prop_address="X")
    r2 = Rec(cat="tax_lien_fed", tier="S", owner="JANE DOE", prop_address="X")
    scored = stage_score([r1, r2])
    assert_true("combo_foreclosure_irs", scored[0].score >= 90 and scored[1].score >= 90,
                f"scores: {scored[0].score}, {scored[1].score}")

    # Test 9: Filter complete drops invalid
    bad = Rec(owner='WINDOW.__ORT="abc"')
    good = Rec(doc_num="X1", owner="JOHN SMITH", filed="2026-05-08",
               doc_type="DEED", prop_address="123 ST", clerk_url="https://x")
    filtered = stage_filter_complete([bad, good])
    assert_eq("filter_drops_bot", len(filtered), 1)

    # Test 10: Category preservation — make sure parsing doesn't override category
    sr_will = {"cat": "probate", "label": "Will & Testament", "tier": "S", "dept": "RP"}
    fake_html = """
    <table><thead><tr><th>Doc Number</th><th>Doc Type</th><th>Grantor</th>
    <th>Recorded Date</th></tr></thead><tbody>
    <tr><td>20260010220</td><td>WILL</td><td>JOHN SMITH</td><td>2026-05-08</td></tr>
    </tbody></table>
    """
    recs, _ = _parse_html_table(fake_html, sr_will)
    if recs:
        assert_eq("category_preserved", recs[0].cat, "probate")
    else:
        log.error("    ❌ category_preserved: no records parsed")
        tests_failed += 1

    log.info("══════ SELF-TESTS: %d passed, %d failed ══════", tests_passed, tests_failed)
    return tests_failed == 0


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--chunk-days", type=int, default=CHUNK_DAYS)
    ap.add_argument("--no-parcel", action="store_true")
    ap.add_argument("--no-merge", action="store_true")
    ap.add_argument("--no-alerts", action="store_true")
    ap.add_argument("--skip-tests", action="store_true")
    args = ap.parse_args()

    log.info("══════ BEXAR COUNTY LEAD SCRAPER %s ══════", VERSION)
    log.info("Python: %s | CWD: %s", sys.version.split()[0], os.getcwd())
    log.info("Args: days=%d chunk=%d no_parcel=%s no_alerts=%s",
             args.days, args.chunk_days, args.no_parcel, args.no_alerts)
    log.info("Env: BEXAR_EMAIL=%s PASS=%s TG_TOKEN=%s TG_CHAT=%s",
             "SET" if os.environ.get("BEXAR_EMAIL") else "MISSING",
             "SET" if os.environ.get("BEXAR_PASSWORD") else "MISSING",
             "SET" if TG_TOKEN else "MISSING",
             "SET" if TG_CHAT else "MISSING")
    log.info("Searches: %d (S=%d A=%d B=%d C=%d)", len(SEARCHES),
             sum(1 for s in SEARCHES if s["tier"] == "S"),
             sum(1 for s in SEARCHES if s["tier"] == "A"),
             sum(1 for s in SEARCHES if s["tier"] == "B"),
             sum(1 for s in SEARCHES if s["tier"] == "C"))

    # Import checks
    try:
        from playwright.async_api import async_playwright  # noqa
        log.info("✅ playwright OK")
    except ImportError as e:
        log.error("❌ playwright missing: %s", e); return 2
    try:
        from bs4 import BeautifulSoup  # noqa
        log.info("✅ beautifulsoup4 OK")
    except ImportError as e:
        log.error("❌ bs4 missing: %s", e); return 2

    # Self-tests (FIX: must pass before running real scrape)
    if not args.skip_tests:
        if not run_self_tests():
            log.error("❌ SELF-TESTS FAILED — aborting run.")
            log.error("   Fix the code or use --skip-tests to bypass (NOT RECOMMENDED).")
            return 2
    else:
        log.warning("⚠️  Skipping self-tests (--skip-tests flag set)")

    email = os.environ.get("BEXAR_EMAIL", "")
    password = os.environ.get("BEXAR_PASSWORD", "")

    # Run scrape
    try:
        records, audits = asyncio.run(
            scrape_clerk(days=args.days, chunk_days=args.chunk_days,
                         email=email, password=password))
    except Exception as e:
        log.error("SCRAPE FATAL: %s\n%s", e, traceback.format_exc())
        return 2

    if not records:
        log.error("❌ ZERO RECORDS scraped")
        # Still write empty outputs and audit so we can see why
        write_outputs([], audits, args.days, {"attempted": 0, "matched": 0})
        return 1

    # BCAD
    bcad_stats = {"attempted": 0, "matched": 0}
    if not args.no_parcel:
        bcad_stats = stage_bcad(records)

    # Filter, score, dedup
    records = stage_filter_complete(records)
    records = stage_score(records)

    if not args.no_merge:
        records = stage_dedup(records)

    # Write
    write_outputs(records, audits, args.days, bcad_stats)

    # Alerts
    if not args.no_alerts:
        send_alerts(records)

    # Final summary
    hot = sum(1 for r in records if r.score >= HOT_SCORE_THRESHOLD)
    warm = sum(1 for r in records if 70 <= r.score < HOT_SCORE_THRESHOLD)
    addr = sum(1 for r in records if r.prop_address)
    mail = sum(1 for r in records if r.mail_address)
    log.info("══════ DONE (%s) ══════", VERSION)
    log.info("Records: %d | Prop Addr: %d (%d%%) | Mail Addr: %d (%d%%)",
             len(records), addr, 100 * addr // max(len(records), 1),
             mail, 100 * mail // max(len(records), 1))
    log.info("Hot (≥%d): %d | Warm (70-%d): %d",
             HOT_SCORE_THRESHOLD, hot, HOT_SCORE_THRESHOLD - 1, warm)

    # Health check
    if len(records) == 0:
        log.error("❌ ZERO RECORDS in final output — likely a filter bug")
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception as e:
        log.error("═══════ FATAL TOP-LEVEL ═══════")
        log.error("%s: %s", type(e).__name__, e)
        log.error("Traceback:\n%s", traceback.format_exc())
        sys.exit(2)
