# main.py
import os
import time
import asyncio
import csv
import io
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query

# ======= SIMPLE CONFIG =======
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "11dto0ons9kgBaRH8M2thH1dKaeeVaigprFmgSuurvIo")
SHEET_GID = os.getenv("SHEET_GID", "0")  # your URL shows gid=0
STEAM_APPID = int(os.getenv("STEAM_APPID", "730"))
STEAM_CURRENCY_CODE = int(os.getenv("STEAM_CURRENCY_CODE", "20"))  # 20 ~ CAD for Steam community endpoints
SKINPORT_CURRENCY = os.getenv("SKINPORT_CURRENCY", "CAD")  # SkinPort expects a TEXT currency like CAD, USD, EUR
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", str(6 * 3600)))  # 6 hours
REQUEST_CONCURRENCY = int(os.getenv("REQUEST_CONCURRENCY", "4"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.5"))
USER_AGENT = os.getenv("USER_AGENT", "cs2-tracker-backend/1.2 (+render)")

# CSV endpoints (we'll use gviz form first)
CSV_URLS = [
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&gid={SHEET_GID}",
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={SHEET_GID}",
]

PUBLISHED_CSV_URL = os.getenv("PUBLISHED_CSV_URL")  # optional publish-to-web CSV URL
if PUBLISHED_CSV_URL:
    CSV_URLS.append(PUBLISHED_CSV_URL)

# ======= IN-MEMORY CACHE =======
_CACHE: Dict[str, Any] = {}
_CACHE_EXPIRES: Dict[str, float] = {}

def cache_get(key: str):
    exp = _CACHE_EXPIRES.get(key)
    if exp and exp > time.time():
        return _CACHE.get(key)
    if key in _CACHE:
        _CACHE.pop(key, None)
        _CACHE_EXPIRES.pop(key, None)
    return None

def cache_set(key: str, value: Any, ttl: int = CACHE_TTL_SECONDS):
    _CACHE[key] = value
    _CACHE_EXPIRES[key] = time.time() + ttl

# ======= HELPERS =======
def parse_float_safe(v) -> Optional[float]:
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None

def parse_quantity(v) -> int:
    try:
        q = int(float(str(v)))
        return max(1, q)
    except:
        return 1

def extract_number_from_price_str(s: str) -> Optional[float]:
    if not s:
        return None
    m = re.search(r"[\d\.,]+", str(s))
    if not m:
        return None
    num = m.group(0)
    if num.count(",") and num.count("."):
        num = num.replace(",", "")
    elif num.count(",") and not num.count("."):
        # heuristic for locales "0,75"
        if len(num.split(",")[-1]) == 3:
            num = num.replace(",", "")
        else:
            num = num.replace(",", ".")
    try:
        return float(num)
    except:
        return None

def normalize_name(s: str) -> str:
    """Basic normalization to help match names between services."""
    if not s:
        return ""
    t = s.strip()
    # common fixes: "Stat-Trak" -> "StatTrak™"
    t = re.sub(r"\bstat[-\s]?trak\b", "StatTrak™", t, flags=re.I)
    # "Field Tested" -> "Field-Tested"
    t = re.sub(r"\bfield\s*tested\b", "Field-Tested", t, flags=re.I)
    # squeeze spaces
    t = re.sub(r"\s+", " ", t)
    return t

# ======= SHEET READER =======
async def fetch_sheet_csv_text() -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/csv, text/plain;q=0.9, */*;q=0.8",
        "Referer": f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit#gid={SHEET_GID}",
    }
    async with httpx.AsyncClient(headers=headers) as client:
        last_status = None
        last_error = None
        for url in CSV_URLS:
            try:
                r = await client.get(url, timeout=20, follow_redirects=True)
                last_status = r.status_code
                if r.status_code == 200 and r.text and "," in r.text.splitlines()[0]:
                    return r.text
            except Exception as e:
                last_error = str(e)
        detail = f"Unable to fetch sheet CSV"
        if last_status:
            detail += f" (HTTP {last_status})"
        if last_error:
            detail += f" - {last_error}"
        raise HTTPException(status_code=502, detail=detail)

async def read_items_from_sheet() -> List[Dict[str, Any]]:
    cached = cache_get("sheet_rows")
    if cached is not None:
        return cached

    text = await fetch_sheet_csv_text()
    f = io.StringIO(text)
    reader = csv.DictReader(f)

    rows = []
    for row in reader:
        norm = {}
        for k, v in row.items():
            if k is None:
                continue
            key = k.strip().lower().replace(" ", "_")
            norm[key] = v.strip() if isinstance(v, str) else v

        item_name = norm.get("item_name")
        source = (norm.get("source") or "steam").strip().lower()
        paid_price = parse_float_safe(norm.get("paid_price") or 0)
        quantity = parse_quantity(norm.get("quantity") or 1)

        if not item_name:
            continue

        rows.append({
            "item_name": item_name,
            "source": source,
            "paid_price": paid_price,
            "quantity": quantity,
        })

    if not rows:
        raise HTTPException(status_code=400, detail="No items found in sheet (check headers & tab)")

    cache_set("sheet_rows", rows, ttl=60)  # cache sheet for 1 minute
    return rows

# ======= PRICE FETCHERS =======
async def fetch_steam_price(client: httpx.AsyncClient, market_hash_name: str) -> Optional[float]:
    """Use Steam community priceoverview (requires exact market_hash_name)."""
    name = normalize_name(market_hash_name)
    url = "https://steamcommunity.com/market/priceoverview/"
    params = {
        "appid": str(STEAM_APPID),
        "market_hash_name": name,
        "currency": str(STEAM_CURRENCY_CODE),
        "format": "json"
    }
    try:
        r = await client.get(url, params=params, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data.get("success"):
            return None
        price_str = data.get("median_price") or data.get("lowest_price")
        return extract_number_from_price_str(price_str)
    except Exception:
        return None

async def fetch_skinport_catalog(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """
    SkinPort API allows: app_id, currency, tradable (not 'appid' or 'term').
    We'll fetch the whole catalog for app_id=730 and SKINPORT_CURRENCY, then cache it.
    """
    cached = cache_get("skinport_catalog")
    if cached is not None:
        return cached

    params = {"app_id": STEAM_APPID, "currency": SKINPORT_CURRENCY, "tradable": 1}
    try:
        r = await client.get("https://api.skinport.com/v1/items", params=params, timeout=30)
        if r.status_code != 200:
            return []
        data = r.json()
        if not isinstance(data, list):
            return []
        cache_set("skinport_catalog", data, ttl=3600)  # cache catalog 1 hour
        return data
    except Exception:
        return []

def best_skinport_match(catalog: List[Dict[str, Any]], target_name: str) -> Optional[Dict[str, Any]]:
    """
    Try to match by exact lowercased 'market_hash_name' or 'name'.
    """
    t = normalize_name(target_name).lower()

    # First pass: exact matches
    for item in catalog:
        name1 = normalize_name(item.get("market_hash_name") or "").lower()
        name2 = normalize_name(item.get("name") or "").lower()
        if t == name1 or t == name2:
            return item

    # Second pass: loose contains (fallback)
    for item in catalog:
        name1 = normalize_name(item.get("market_hash_name") or "").lower()
        name2 = normalize_name(item.get("name") or "").lower()
        if t in name1 or t in name2:
            return item
    return None

async def fetch_skinport_price(client: httpx.AsyncClient, name: str) -> Optional[float]:
    catalog = await fetch_skinport_catalog(client)
    if not catalog:
        return None
    match = best_skinport_match(catalog, name)
    if not match:
        return None
    # SkinPort schema commonly uses one of these price keys
    for k in ("price", "market_price", "last_price", "min_price"):
        if match.get(k) is not None:
            try:
                return float(match[k])
            except Exception:
                pass
    return None

async def fetch_price_for_item(client: httpx.AsyncClient, item: Dict[str, Any], sem: asyncio.Semaphore):
    name = item["item_name"]
    source = item.get("source", "steam")

    cache_key = f"price::{source}::{name}"
    cached = cache_get(cache_key)
    if cached is not None:
        cached_copy = dict(cached)
        cached_copy["paid_price"] = item.get("paid_price")
        cached_copy["quantity"] = item.get("quantity", 1)
        return cached_copy

    async with sem:
        await asyncio.sleep(REQUEST_DELAY)
        price = None
        if source == "skinport":
            price = await fetch_skinport_price(client, name)
            if price is None:
                price = await fetch_steam_price(client, name)
        else:
            price = await fetch_steam_price(client, name)
            if price is None:
                price = await fetch_skinport_price(client, name)

    result = {
        "item_name": name,
        "source": source,
        "current_price": price,
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
    }
    cache_set(cache_key, result)
    result["paid_price"] = item.get("paid_price")
    result["quantity"] = item.get("quantity", 1)
    return result

# ======= FASTAPI APP =======
app = FastAPI(title="CS2 Portfolio Tracker Backend")

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}

@app.get("/prices")
async def get_prices():
    items = await read_items_from_sheet()
    if not items:
        raise HTTPException(status_code=400, detail="No items found in sheet")

    sem = asyncio.Semaphore(REQUEST_CONCURRENCY)
    async with httpx.AsyncClient(headers={"User-Agent": USER_AGENT}) as client:
        tasks = [fetch_price_for_item(client, it, sem) for it in items]
        results_raw = await asyncio.gather(*tasks)

    output = []
    for r in results_raw:
        current = r.get("current_price")
        paid = r.get("paid_price") or 0.0
        qty = int(r.get("quantity") or 1)
        profit_per = None
        total_profit = None
        pct = None
        if current is not None:
            try:
                profit_per = round(float(current) - float(paid), 2)
                total_profit = round(profit_per * qty, 2)
                pct = round((profit_per / float(paid)) * 100, 2) if paid and float(paid) != 0 else None
            except Exception:
                pass
        output.append({
            "item_name": r.get("item_name"),
            "source": r.get("source"),
            "paid_price": paid,
            "current_price": round(current, 2) if current is not None else None,
            "quantity": qty,
            "profit_per_item": profit_per,
            "profit_total": total_profit,
            "percent_change": pct,
            "timestamp_utc": r.get("timestamp_utc"),
        })
    return output

# ======= DEBUG ENDPOINTS (to test from the server) =======
@app.get("/test_steam")
async def test_steam(name: str = Query(..., description="Exact market name, e.g. 'Gamma 2 Case'")):
    async with httpx.AsyncClient(headers={"User-Agent": USER_AGENT}) as client:
        price = await fetch_steam_price(client, name)
        return {"name": name, "price": price, "currency_code": STEAM_CURRENCY_CODE}

@app.get("/test_skinport")
async def test_skinport(name: str = Query(..., description="Exact market name")):
    async with httpx.AsyncClient(headers={"User-Agent": USER_AGENT}) as client:
        price = await fetch_skinport_price(client, name)
        return {"name": name, "price": price, "currency": SKINPORT_CURRENCY}
