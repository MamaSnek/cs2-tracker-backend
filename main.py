# main.py — stable, CORS-enabled, works with your sheet + Steam + SkinPort
import os
import csv
import io
import re
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# -----------------------
# Config (env with sane defaults)
# -----------------------
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "11dto0ons9kgBaRH8M2thH1dKaeeVaigprFmgSuurvIo")
SHEET_GID = os.getenv("SHEET_GID", "0")  # your sheet shows gid=0
STEAM_APPID = int(os.getenv("STEAM_APPID", "730"))
STEAM_CURRENCY_CODE = int(os.getenv("STEAM_CURRENCY_CODE", "20"))  # 20 = CAD
SKINPORT_CURRENCY = os.getenv("SKINPORT_CURRENCY", "CAD")          # CAD / USD / EUR
USER_AGENT = os.getenv("USER_AGENT", "cs2-tracker/1.0 (+render)")
REQUEST_CONCURRENCY = int(os.getenv("REQUEST_CONCURRENCY", "4"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.4"))

CSV_URLS = [
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&gid={SHEET_GID}",
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={SHEET_GID}",
]

# -----------------------
# App + CORS
# -----------------------
app = FastAPI(title="CS2 Portfolio Tracker Backend")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten later by putting your Vercel URL
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------
# One shared HTTP/2 client
# -----------------------
def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        http2=True,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    )

# -----------------------
# Helpers
# -----------------------
def num_from_price_str(s: str) -> Optional[float]:
    if not s:
        return None
    m = re.search(r"[\d\.,]+", s)
    if not m:
        return None
    num = m.group(0)
    if num.count(",") and num.count("."):
        num = num.replace(",", "")
    elif num.count(",") and not num.count("."):
        # treat comma as decimal if likely
        if len(num.split(",")[-1]) == 3:
            num = num.replace(",", "")
        else:
            num = num.replace(",", ".")
    try:
        return float(num)
    except:
        return None

def parse_float(v) -> Optional[float]:
    try:
        return float(str(v).replace(",", "").strip())
    except:
        return None

def parse_int(v) -> int:
    try:
        return max(1, int(float(str(v))))
    except:
        return 1

def normalize_name(s: str) -> str:
    if not s: return ""
    t = s.strip()
    t = re.sub(r"\bstat[-\s]?trak\b", "StatTrak", t, flags=re.I)
    t = re.sub(r"\bfield\s*tested\b", "Field-Tested", t, flags=re.I)
    t = t.replace("★", " ").replace("|", " ").replace("™", " ")
    t = re.sub(r"[^a-zA-Z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip().lower()
    return t

# -----------------------
# Sheet fetch
# -----------------------
async def fetch_sheet_csv_text(client: httpx.AsyncClient) -> str:
    last_status = None
    last_err = None
    for url in CSV_URLS:
        try:
            r = await client.get(url)
            last_status = r.status_code
            if r.status_code == 200 and "," in (r.text.splitlines()[0] if r.text else ""):
                return r.text
        except Exception as e:
            last_err = str(e)
    detail = "Unable to fetch sheet CSV"
    if last_status: detail += f" (HTTP {last_status})"
    if last_err: detail += f" - {last_err}"
    raise HTTPException(status_code=502, detail=detail)

async def read_items(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    text = await fetch_sheet_csv_text(client)
    f = io.StringIO(text)
    reader = csv.DictReader(f)

    out = []
    for row in reader:
        name = (row.get("item_name") or "").strip()
        source = (row.get("source") or "steam").strip().lower()
        paid = parse_float(row.get("paid_price") or 0) or 0.0
        qty = parse_int(row.get("quantity") or 1)
        if not name:
            continue
        out.append({"item_name": name, "source": source, "paid_price": paid, "quantity": qty})
    if not out:
        raise HTTPException(status_code=400, detail="No items found in sheet (check headers & tab)")
    return out

# -----------------------
# Price fetchers
# -----------------------
async def fetch_steam_price(client: httpx.AsyncClient, market_name: str) -> Optional[float]:
    params = {
        "appid": "730",
        "currency": str(STEAM_CURRENCY_CODE),
        "market_hash_name": market_name,
        "format": "json",
    }
    try:
        r = await client.get("https://steamcommunity.com/market/priceoverview/", params=params)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data.get("success"):
            return None
        p = data.get("median_price") or data.get("lowest_price")
        return num_from_price_str(p)
    except Exception:
        return None

SKINPORT_HEADERS = {
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate, br",
    "User-Agent": USER_AGENT,
}

async def fetch_skinport_catalog(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    # We fetch catalog once (tradable = 1), then match locally
    params = {"app_id": STEAM_APPID, "currency": SKINPORT_CURRENCY, "tradable": 1}
    for attempt in range(3):
        try:
            r = await client.get("https://api.skinport.com/v1/items", params=params, headers=SKINPORT_HEADERS)
            if r.status_code == 200 and isinstance(r.json(), list):
                return r.json()
            if 500 <= r.status_code < 600:
                await asyncio.sleep(0.8 * (attempt + 1))
        except Exception:
            await asyncio.sleep(0.8 * (attempt + 1))
    return []

def match_skinport_item(catalog: List[Dict[str, Any]], target: str) -> Optional[Dict[str, Any]]:
    key = normalize_name(target)
    # exact
    for it in catalog:
        n1 = normalize_name(it.get("market_hash_name") or "")
        n2 = normalize_name(it.get("name") or "")
        if key == n1 or key == n2:
            return it
    # contains
    for it in catalog:
        n1 = normalize_name(it.get("market_hash_name") or "")
        n2 = normalize_name(it.get("name") or "")
        if key in n1 or key in n2:
            return it
    return None

def extract_any_price(item: Dict[str, Any]) -> Optional[float]:
    for k in ("price", "min_price", "market_price", "last_price", "mean_price", "median_price", "suggested_price"):
        v = item.get(k)
        if v is not None:
            try:
                return float(v)
            except:
                pass
    return None

async def fetch_skinport_price(client: httpx.AsyncClient, name: str) -> Optional[float]:
    catalog = await fetch_skinport_catalog(client)
    if not catalog:
        return None
    m = match_skinport_item(catalog, name)
    if not m:
        return None
    return extract_any_price(m)

async def fetch_price(client: httpx.AsyncClient, source: str, name: str) -> Optional[float]:
    # prefer chosen source, then fall back
    if source == "skinport":
        p = await fetch_skinport_price(client, name)
        if p is not None:
            return p
        return await fetch_steam_price(client, name)
    else:
        p = await fetch_steam_price(client, name)
        if p is not None:
            return p
        return await fetch_skinport_price(client, name)

# -----------------------
# Routes
# -----------------------
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}

@app.get("/prices")
async def prices():
    sem = asyncio.Semaphore(REQUEST_CONCURRENCY)
    async with make_client() as client:
        items = await read_items(client)
        tasks = []
        for it in items:
            async def task(it=it):
                await asyncio.sleep(REQUEST_DELAY)
                price = await fetch_price(client, it["source"], it["item_name"])
                paid = it["paid_price"]
                qty = it["quantity"]
                if price is not None:
                    profit = round(price - paid, 2)
                    total = round(profit * qty, 2)
                    pct = round((profit / paid) * 100, 2) if paid else None
                else:
                    profit = total = pct = None
                return {
                    "item_name": it["item_name"],
                    "source": it["source"],
                    "paid_price": paid,
                    "current_price": round(price, 2) if price is not None else None,
                    "quantity": qty,
                    "profit_per_item": profit,
                    "profit_total": total,
                    "percent_change": pct,
                    "timestamp_utc": datetime.utcnow().isoformat() + "Z",
                }
            tasks.append(task())
        return await asyncio.gather(*tasks)

# Debug helpers
@app.get("/test_steam")
async def test_steam(name: str = Query(...)):
    async with make_client() as client:
        return {"name": name, "price": await fetch_steam_price(client, name)}

@app.get("/test_skinport")
async def test_skinport(name: str = Query(...)):
    async with make_client() as client:
        return {"name": name, "price": await fetch_skinport_price(client, name)}
