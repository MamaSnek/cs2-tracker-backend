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

# ---------- Config (env vars)
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")  # REQUIRED
SHEET_NAME = os.getenv("SHEET_NAME", "items")
STEAM_APPID = int(os.getenv("STEAM_APPID", "730"))
STEAM_CURRENCY_CODE = int(os.getenv("STEAM_CURRENCY_CODE", "20"))  # 20 = CAD
SKINPORT_API_BASE = os.getenv("SKINPORT_API_BASE", "https://api.skinport.com/v1")
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", str(6 * 3600)))  # default 6 hours
REQUEST_CONCURRENCY = int(os.getenv("REQUEST_CONCURRENCY", "4"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.5"))
USER_AGENT = os.getenv("USER_AGENT", "cs2-tracker-backend/1.0")

if not GOOGLE_SHEET_ID:
    raise RuntimeError("Please set the environment variable GOOGLE_SHEET_ID")

# Build public CSV export URL for the sheet/tab (works when sheet is public "anyone with link can view")
CSV_EXPORT_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&sheet={SHEET_NAME}"

# ---------- Simple in-memory cache
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

# ---------- Helpers
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
        # heuristic
        if len(num.split(",")[-1]) == 3:
            num = num.replace(",", "")
        else:
            num = num.replace(",", ".")
    try:
        return float(num)
    except:
        return None

# ---------- Read items from the public Google Sheet CSV
async def read_items_from_sheet() -> List[Dict[str, Any]]:
    cached = cache_get("sheet_rows")
    if cached is not None:
        return cached

    async with httpx.AsyncClient(headers={"User-Agent": USER_AGENT}) as client:
       r = await client.get(CSV_EXPORT_URL, timeout=15, follow_redirects=True)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Unable to fetch sheet CSV (HTTP {r.status_code})")
        text = r.text

    f = io.StringIO(text)
    reader = csv.DictReader(f)
    rows = []
    for row in reader:
        # Normalize header keys to lowercase no-space
        norm = {}
        for k, v in row.items():
            if k is None:
                continue
            key = k.strip().lower().replace(" ", "_")
            norm[key] = v.strip() if isinstance(v, str) else v
        # required: item_name, source, paid_price, quantity
        item_name = norm.get("item_name")
        source = (norm.get("source") or "steam").strip().lower()
        paid_price = parse_float_safe(norm.get("paid_price") or norm.get("paid") or 0)
        quantity = parse_quantity(norm.get("quantity") or 1)
        if not item_name:
            continue
        rows.append({
            "item_name": item_name,
            "source": source,
            "paid_price": paid_price,
            "quantity": quantity,
        })
    cache_set("sheet_rows", rows, ttl=60)  # small cache for sheet reading
    return rows

# ---------- Fetch prices
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
    # Try SkinPort items search endpoint
    try:
        url = f"{SKINPORT_API_BASE}/items"
        params = {"appid": STEAM_APPID, "term": name}
        r = await client.get(url, params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
        else:
            # fallback attempt
            r2 = await client.get(f"{SKINPORT_API_BASE}/items?appid={STEAM_APPID}&query={name}", timeout=15)
            if r2.status_code != 200:
                return None
            data = r2.json()

        # data usually list of items
        if isinstance(data, list) and data:
            # try exact match
            for item in data:
                n = (item.get("name") or item.get("market_hash_name") or "").strip().lower()
                if n == name.strip().lower():
                    p = item.get("price") or item.get("market_price") or item.get("last_price")
                    if p is not None:
                        return float(p)
            # fallback: first result
            first = data[0]
            p = first.get("price") or first.get("market_price") or first.get("last_price")
            if p is not None:
                return float(p)
        elif isinstance(data, dict):
            # sometimes wrapped
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
        # attach paid and qty and return
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

# ---------- FastAPI app
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
