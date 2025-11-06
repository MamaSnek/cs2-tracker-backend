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
from fastapi import FastAPI, HTTPException

# ======= SIMPLE CONFIG =======
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "11dto0ons9kgBaRH8M2thH1dKaeeVaigprFmgSuurvIo")
SHEET_GID = os.getenv("SHEET_GID", "0")  # your URL shows gid=0
STEAM_APPID = int(os.getenv("STEAM_APPID", "730"))
STEAM_CURRENCY_CODE = int(os.getenv("STEAM_CURRENCY_CODE", "20"))  # 20 ~ CAD (Steam community endpoints)
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", str(6 * 3600)))  # 6 hours
REQUEST_CONCURRENCY = int(os.getenv("REQUEST_CONCURRENCY", "4"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.5"))
USER_AGENT = os.getenv("USER_AGENT", "cs2-tracker-backend/1.1 (+render)")

# Try multiple CSV endpoints to dodge redirects/quirks
CSV_URLS = [
    # 1) gviz CSV (usually robust)
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&gid={SHEET_GID}",
    # 2) export CSV (older method)
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={SHEET_GID}",
    # 3) publish CSV (works if you later do File -> Share -> Publish to the web)
    # If you ever publish, replace <PUB_ID> with the long publish ID Google gives you.
    # We'll leave this pattern here as a final fallback (won't be used unless you set env var PUBLISHED_CSV_URL).
]

PUBLISHED_CSV_URL = os.getenv("PUBLISHED_CSV_URL")  # optional direct publish URL
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

async def fetch_sheet_csv_text() -> str:
    """
    Try multiple CSV endpoints with redirects allowed.
    """
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
        # If we reach here, nothing worked
        detail = f"Unable to fetch sheet CSV"
        if last_status:
            detail += f" (HTTP {last_status})"
        if last_error:
            detail += f" - {last_error}"
        raise HTTPException(status_code=502, detail=detail)

# ======= READ ITEMS FROM SHEET =======
async def read_items_from_sheet() -> List[Dict[str, Any]]:
    cached = cache_get("sheet_rows")
    if cached is not None:
        return cached

    text = await fetch_sheet_csv_text()

    f = io.StringIO(text)
    reader = csv.DictReader(f)

    # Expect headers: item_name, source, paid_price, quantity
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

    cache_set("sheet_rows", rows, ttl=60)  # cache sheet content 1 minute
    return rows

# ======= PRICE FETCHERS =======
async def fetch_steam_price(client: httpx.AsyncClient, market_hash_name: str) -> Optional[float]:
    url = "https://steamcommunity.com/market/priceoverview/"
    params = {
        "appid": str(STEAM_APPID),
        "market_hash_name": market_hash_name,
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

async def fetch_skinport_price(client: httpx.AsyncClient, name: str) -> Optional[float]:
    try:
        r = await client.get(
            "https://api.skinport.com/v1/items",
            params={"appid": STEAM_APPID, "term": name},
            timeout=15
        )
        data = r.json() if r.status_code == 200 else None

        if not data:
            r2 = await client.get(
                "https://api.skinport.com/v1/items",
                params={"appid": STEAM_APPID, "query": name},
                timeout=15
            )
            data = r2.json() if r2.status_code == 200 else None

        if not data:
            return None

        if isinstance(data, list) and data:
            for item in data:
                n = (item.get("name") or item.get("market_hash_name") or "").strip().lower()
                if n == name.strip().lower():
                    p = item.get("price") or item.get("market_price") or item.get("last_price")
                    if p is not None:
                        return float(p)
            first = data[0]
            p = first.get("price") or first.get("market_price") or first.get("last_price")
            if p is not None:
                return float(p)
        elif isinstance(data, dict):
            items = data.get("items") or data.get("results") or []
            if items and isinstance(items, list):
                first = items[0]
                p = first.get("price") or first.get("market_price") or first.get("last_price")
                if p is not None:
                    return float(p)
    except Exception:
        return None
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
    async with httpx.AsyncClient(headers={"User-Agent":
