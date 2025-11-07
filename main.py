import os
import csv
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

# ================================
# CONFIG FROM ENV
# ================================
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "items")
SHEET_GID = os.getenv("SHEET_GID", "0")

STEAM_CURRENCY_CODE = os.getenv("STEAM_CURRENCY_CODE", "20")  # 20 = CAD
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

if not GOOGLE_SHEET_ID:
    raise RuntimeError("Missing GOOGLE_SHEET_ID environment variable")

# Build CSV export URL
CSV_EXPORT_URL = (
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={SHEET_GID}"
)

# ================================
# FASTAPI APP + CORS
# ================================
app = FastAPI(title="CS2 Portfolio Tracker Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],     # <-- Allows your Vercel frontend
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================================
# MODELS
# ================================
class Item(BaseModel):
    item_name: str
    source: str
    paid_price: float
    quantity: int
    current_price: Optional[float]
    profit_per_item: Optional[float]
    profit_total: Optional[float]
    percent_change: Optional[float]
    timestamp_utc: str

# ================================
# HELPERS
# ================================
async def fetch_csv_rows():
    """
    Downloads the CSV from Google Sheets and returns rows.
    """
    async with httpx.AsyncClient(follow_redirects=True, headers={"User-Agent": USER_AGENT}) as client:
        r = await client.get(CSV_EXPORT_URL, timeout=15)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Unable to fetch sheet CSV (HTTP {r.status_code})")

        decoded = r.content.decode("utf-8").splitlines()
        reader = csv.DictReader(decoded)
        return list(reader)


async def fetch_steam_price(name: str) -> Optional[float]:
    """
    Fetch lowest Steam price in CAD for a given item name.
    """
    url = (
        "https://steamcommunity.com/market/priceoverview/"
        f"?appid=730&currency={STEAM_CURRENCY_CODE}&market_hash_name={httpx.URL(name).raw_path.decode()}"
    )

    async with httpx.AsyncClient(headers={"User-Agent": USER_AGENT}) as client:
        r = await client.get(url, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data.get("succe
