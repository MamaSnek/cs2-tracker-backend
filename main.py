# main.py
import os
import time
import asyncio
import csv
import io
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Query

# ======= SIMPLE CONFIG =======
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "11dto0ons9kgBaRH8M2thH1dKaeeVaigprFmgSuurvIo")
SHEET_GID = os.getenv("SHEET_GID", "0")
STEAM_APPID = int(os.getenv("STEAM_APPID", "730"))
STEAM_CURRENCY_CODE = int(os.getenv("STEAM_CURRENCY_CODE", "20"))  # 20 ~ CAD
SKINPORT_CURRENCY = os.getenv("SKINPORT_CURRENCY", "CAD")          # CAD / USD / EUR
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", str(6 * 3600)))  # 6 hours
REQUEST_CONCURRENCY = int(os.getenv("REQUEST_CONCURRENCY", "4"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.5"))
USER_AGENT = os.getenv("USER_AGENT", "cs2-tracker-backend/1.5 (+render)")

# CSV endpoints
CSV_URLS = [
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&gid={SHEET_GID}",
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={SHEET_GID}",
]
PUBLISHED_CSV_URL = os.getenv("PUBLISHED_CSV_URL")
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
        if len(num.split(",")[-1]) == 3:
            num = num.replace(",", "")
        else:
            num = num.replace(",", ".")
    try:
        return float(num)
    except:
        return None

def normalize_name(s: str) -> str:
    if not s:
        return ""
    t = s.strip()
    # unify common variants
    t = re.sub(r"\bstat[-\s]?trak\b", "StatTrak", t, flags=re.I)
    t = re.sub(r"\bfield\s*tested\b", "Field-Tested", t, flags=re.I)
    # remove special skin-site decorations
    t = t.replace("★", " ")
    t = t.replace("™", " ")
    t = t.replace("|", " ")
    # collapse non-alnum to spaces, lower, and squeeze spaces
    t = re.sub(r"[^a-zA-Z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip().lower()
    return t

def key_eq(a: str, b: str) -> bool:
    return normalize_name(a) == normalize_name(b)

def key_contains(hay: str, needle: str) -> bool:
    return normalize_name(needle) in normalize_name(hay)

# ======= ONE SHARED HTTP CLIENT (HTTP/2) =======
def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        http2=True,
        headers={
            "User-Agent": USER_AGENT,
        },
        timeout=30.0,
        follow_redirects=True,
    )

# ======= SHEET READER =======
async def fetch_sheet_csv_text(client: httpx.AsyncClient) -> str:
    last_status = None
    last_error = None
    for url in CSV_URLS:
        try:
            r = await client.get(url)
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

async def read_items_from_sheet(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    cached = cache_get("sheet_rows")
    if cached is not None:
        return cached

    text = await fetch_sheet_csv_text(client)
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
    name = market_hash_name  # Steam seems fine with the original text; it already worked for you
    params = {
        "appid": str(STEAM_APPID),
        "market_hash_name": name,
        "currency": str(STEAM_CURRENCY_CODE),
        "format": "json"
    }
    try:
        r = await client.get("https://steamcommunity.com/market/priceoverview/", params=params)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data.get("success"):
            return None
        price_str = data.get("median_price") or data.get("lowest_price")
        return extract_number_from_price_str(price_str)
    except Exception:
        return None

SKINPORT_HEADERS = {
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate, br",
    "User-Agent": USER_AGENT,
}

async def fetch_skinport_catalog(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    cached = cache_get("skinport_catalog")
    if cached is not None:
        return cached

    params = {"app_id": STEAM_APPID, "currency": SKINPORT_CURRENCY, "tradable": 1}

    # small retry loop for 502s
    for attempt in range(3):
        try:
            r = await client.get("https://api.skinport.com/v1/items", params=params, headers=SKINPORT_HEADERS)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    cache_set("skinport_catalog", data, ttl=3600)  # 1 hour
                    return data
            if 500 <= r.status_code < 600:
                await asyncio.sleep(0.8 * (attempt + 1))
            else:
                break
        except Exception:
            await asyncio.sleep(0.8 * (attempt + 1))
    return []

def best_skinport_match(catalog: List[Dict[str, Any]], target_name: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    t = normalize_name(target_name)
    # exact key match
    for item in catalog:
        n1 = normalize_name(item.get("market_hash_name") or "")
        n2 = normalize_name(item.get("name") or "")
        if t == n1 or t == n2:
            return item, (item.get("market_hash_name") or item.get("name"))
    # contains (looser)
    for item in catalog:
        n1 = normalize_name(item.get("market_hash_name") or "")
        n2 = normalize_name(item.get("name") or "")
        if t in n1 or t in n2:
            return item, (item.get("market_hash_name") or item.get("name"))
    return None, None

async def fetch_skinport_price(client: httpx.AsyncClient, name: str) -> Optional[float]:
    catalog = await fetch_skinport_catalog(client)
    if not catalog:
        return None
    match, _ = best_skinport_match(catalog, name)
    if not match:
        return None
    for k in ("price", "min_price", "market_price", "last_price", "mean_price", "median_price", "suggested_price"):
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
    sem = asyncio.Semaphore(REQUEST_CONCURRENCY)
    async with make_client() as client:
        items = await read_items_from_sheet(client)
        if not items:
            raise HTTPException(status_code=400, detail="No items found in sheet")

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

# ======= DEBUG ENDPOINTS =======
@app.get("/test_steam")
async def test_steam(name: str = Query(..., description="Exact market name, e.g. 'Gamma 2 Case'")):
    async with make_client() as client:
        price = await fetch_steam_price(client, name)
        return {"name": name, "price": price, "currency_code": STEAM_CURRENCY_CODE}

@app.get("/test_skinport")
async def test_skinport(name: str = Query(..., description="Exact market name")):
    async with make_client() as client:
        catalog = await fetch_skinport_catalog(client)
        match, matched_name = best_skinport_match(catalog, name)
        price = None
        if match:
            for k in ("price", "min_price", "market_price", "last_price", "mean_price", "median_price", "suggested_price"):
                if match.get(k) is not None:
                    try:
                        price = float(match[k]); break
                    except Exception:
                        pass
        return {"query": name, "matched": matched_name, "price": price, "currency": SKINPORT_CURRENCY}
