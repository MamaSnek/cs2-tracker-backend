# main.py — minimal & stable: Steam-only, gentle rate, CORS enabled
import os, csv, io, asyncio, re
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ---- Config (safe defaults) ----
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "11dto0ons9kgBaRH8M2thH1dKaeeVaigprFmgSuurvIo")
SHEET_GID = os.getenv("SHEET_GID", "0")
STEAM_CURRENCY_CODE = os.getenv("STEAM_CURRENCY_CODE", "20")  # 20 = CAD
USER_AGENT = os.getenv("USER_AGENT", "cs2-tracker/steam-minimal/1.0 (+render)")

# Be gentle with Steam to avoid 429/blocks
REQUEST_CONCURRENCY = int(os.getenv("REQUEST_CONCURRENCY", "2"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.7"))

CSV_URLS = [
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&gid={SHEET_GID}",
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={SHEET_GID}",
]

# ---- App & CORS ----
app = FastAPI(title="CS2 Portfolio Tracker Backend")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # lock to your Vercel origin later
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- HTTP client (HTTP/1.1 is safer for Steam) ----
def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        http2=False,
        timeout=25.0,
        follow_redirects=True,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-CA,en;q=0.8",
            "Referer": "https://steamcommunity.com/market/",
        },
    )

# ---- Helpers ----
def num_from_price_str(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    m = re.search(r"[\d\.,]+", s)
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

def to_float(v) -> float:
    try:
        return float(str(v).replace(",", "").strip())
    except:
        return 0.0

def to_int(v) -> int:
    try:
        return max(1, int(float(str(v))))
    except:
        return 1

# ---- Sheet fetch ----
async def fetch_sheet_rows(ac: httpx.AsyncClient) -> List[Dict[str, Any]]:
    last_status = None
    last_err = None
    for url in CSV_URLS:
        try:
            r = await ac.get(url)
            last_status = r.status_code
            if r.status_code == 200 and r.text and "," in r.text.splitlines()[0]:
                return list(csv.DictReader(io.StringIO(r.text)))
        except Exception as e:
            last_err = str(e)
    detail = "Unable to fetch sheet CSV"
    if last_status: detail += f" (HTTP {last_status})"
    if last_err: detail += f" - {last_err}"
    raise HTTPException(status_code=502, detail=detail)

# ---- Steam price ----
async def fetch_steam_price(ac: httpx.AsyncClient, market_name: str) -> Optional[float]:
    params = {
        "appid": "730",
        "currency": str(STEAM_CURRENCY_CODE),
        "market_hash_name": market_name,
        "format": "json",
    }
    try:
        r = await ac.get("https://steamcommunity.com/market/priceoverview/", params=params)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data.get("success"):
            return None
        price_str = data.get("median_price") or data.get("lowest_price")
        return num_from_price_str(price_str)
    except Exception:
        return None

# ---- Routes ----
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}

@app.get("/prices")
async def prices():
    sem = asyncio.Semaphore(REQUEST_CONCURRENCY)
    async with make_client() as ac:
        rows = await fetch_sheet_rows(ac)

        async def one(row: Dict[str, Any]):
            name = (row.get("item_name") or "").strip()
            source = (row.get("source") or "steam").strip().lower()
            paid = to_float(row.get("paid_price"))
            qty = to_int(row.get("quantity"))

            # be polite to Steam
            async with sem:
                await asyncio.sleep(REQUEST_DELAY)
                current = await fetch_steam_price(ac, name)

            if current is not None:
                profit = round(current - paid, 2)
                total = round(profit * qty, 2)
                pct = round((profit / paid) * 100, 2) if paid else None
            else:
                profit = total = pct = None

            return {
                "item_name": name,
                "source": source,
                "paid_price": paid,
                "current_price": round(current, 2) if current is not None else None,
                "quantity": qty,
                "profit_per_item": profit,
                "profit_total": total,
                "percent_change": pct,
                "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            }

        tasks = [one(r) for r in rows if (r.get("item_name") or "").strip()]
        return await asyncio.gather(*tasks)

# ---- Debug: see exactly what Steam returns for one name ----
@app.get("/diag_steam")
async def diag_steam(name: str = Query(..., description="Exact Steam market name")):
    async with make_client() as ac:
        params = {
            "appid": "730",
            "currency": str(STEAM_CURRENCY_CODE),
            "market_hash_name": name,
            "format": "json",
        }
        r = await ac.get("https://steamcommunity.com/market/priceoverview/", params=params)
        content_type = r.headers.get("content-type", "")
        text = await r.aread()
        preview = text[:200].decode(errors="ignore")
        return {
            "http_status": r.status_code,
            "content_type": content_type,
            "preview": preview,
            "url": str(r.request.url),
        }
