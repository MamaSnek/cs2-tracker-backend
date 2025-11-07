# main.py — Steam-first with smart name fixes + explicit map for your list
import os, csv, io, re, asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ---------- Config ----------
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "11dto0ons9kgBaRH8M2thH1dKaeeVaigprFmgSuurvIo")
SHEET_GID = os.getenv("SHEET_GID", "0")
STEAM_CURRENCY_CODE = os.getenv("STEAM_CURRENCY_CODE", "20")  # 20 = CAD
USER_AGENT = os.getenv("USER_AGENT", "cs2-tracker/steam-first/1.0 (+render)")
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.25"))
REQUEST_CONCURRENCY = int(os.getenv("REQUEST_CONCURRENCY", "5"))

CSV_URLS = [
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&gid={SHEET_GID}",
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={SHEET_GID}",
]

# ---------- App & CORS ----------
app = FastAPI(title="CS2 Portfolio Tracker Backend")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # you can restrict to your Vercel origin later
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def client() -> httpx.AsyncClient:
    return httpx.AsyncClient(http2=True, timeout=25.0, follow_redirects=True, headers={"User-Agent": USER_AGENT})

# ---------- Helpers ----------
def _num_from_price_str(s: Optional[str]) -> Optional[float]:
    if not s: return None
    m = re.search(r"[\d\.,]+", s)
    if not m: return None
    num = m.group(0)
    if num.count(",") and num.count("."):
        num = num.replace(",", "")
    elif num.count(",") and not num.count("."):
        # locale guess
        if len(num.split(",")[-1]) == 3:
            num = num.replace(",", "")
        else:
            num = num.replace(",", ".")
    try:
        return float(num)
    except:
        return None

def _parse_float(v) -> float:
    try:
        return float(str(v).replace(",", "").strip())
    except:
        return 0.0

def _parse_int(v) -> int:
    try:
        return max(1, int(float(str(v).strip())))
    except:
        return 1

# ---------- Exact name map for YOUR items ----------
# Left = what you have in the sheet (case/spacing tolerant), Right = Steam market exact
NAME_MAP = {
    # Knives/Gloves
    "★ butterfly knife | fade (factory new)": "★ Butterfly Knife | Fade (Factory New)",
    "★ driver gloves | king snake (minimal wear)": "★ Driver Gloves | King Snake (Minimal Wear)",
    "nomad knife scorched (minimal wear)": "★ Nomad Knife | Scorched (Minimal Wear)",

    # Rifles/Pistols
    "m4a4 neo-noir (minimal wear)": "M4A4 | Neo-Noir (Minimal Wear)",
    "awp neo-noir (field-tested)": "AWP | Neo-Noir (Field-Tested)",
    "awp neo-noir (minimal wear)": "AWP | Neo-Noir (Minimal Wear)",
    "m4a4 dragon king (minimal wear)": "M4A4 | Dragon King (Minimal Wear)",
    "ak-47 midnight laminate (minimal wear)": "AK-47 | Midnight Laminate (Minimal Wear)",
    "m4a4 living in color (field tested)": "M4A4 | Living Color (Field-Tested)",
    "ak-47 headshot (minimal wear)": "AK-47 | Head Shot (Minimal Wear)",

    # StatTrak pistols (note USP-S, Glock-18, hyphen, ™, Field-Tested hyphen)
    "usp neo-noir (stat-trak field tested)": "StatTrak™ USP-S | Neo-Noir (Field-Tested)",
    "glock neo-noir (stat trak field tested)": "StatTrak™ Glock-18 | Neo-Noir (Field-Tested)",

    # Stickers (Steam format is "Sticker | Team/Player (Tier) | Event")
    "apeks gold paris 2023": "Sticker | Apeks (Gold) | Paris 2023",
    "mercury gold austin 2025": "Sticker | Mercury (Gold) | Austin 2025",
    "eternal fire holo copenhagen 2024": "Sticker | Eternal Fire (Holo) | Copenhagen 2024",
    "eternal fire glitter copenhagen 2024": "Sticker | Eternal Fire (Glitter) | Copenhagen 2024",
    "gamerlegion holo copenhagen 2024": "Sticker | GamerLegion (Holo) | Copenhagen 2024",
    "pgl holo copenhagen 2024": "Sticker | PGL (Holo) | Copenhagen 2024",
    "donk holo copenhagen 2024": "Sticker | donk (Holo) | Copenhagen 2024",
    "m0nesy holo paris 2023": "Sticker | m0NESY (Holo) | Paris 2023",

    # Music Kits (best-known exacts)
    "life's not out to get you (stat-trak)": "StatTrak™ Music Kit | Neck Deep, Life's Not Out To Get You",
    "the lowlife pack (stat-trak)": "StatTrak™ Music Kit | Roam, The Lowlife Pack",
    "scarlxrd:king scar (stat-trak)": "StatTrak™ Music Kit | Scarlxrd, King, Scar",

    # Cases / packages (Steam exacts)
    "gamma 2 case": "Gamma 2 Case",
    "broken fang case": "Operation Broken Fang Case",
    "riptide case": "Operation Riptide Case",
    "shattered web case": "Shattered Web Case",
    "spectrum case": "Spectrum Case",
    "esports 2013 case": "eSports 2013 Case",
    "esports 2013 winter case": "eSports 2013 Winter Case",
    "esports 2014 summer case": "eSports 2014 Summer Case",
    "stockholm 2021 dust 2 souvenir package": "Stockholm 2021 Dust II Souvenir Package",
    "berlin 2019 nuke souvenir package": "Berlin 2019 Nuke Souvenir Package",
}

def _key(s: str) -> str:
    s = s.strip().lower()
    s = s.replace("™", " tm ").replace("★", " ").replace("|", " ")
    s = re.sub(r"\s+", " ", s)
    return s

def map_to_steam_exact(name: str) -> Optional[str]:
    # try direct map
    k = _key(name)
    if k in NAME_MAP:
        return NAME_MAP[k]
    return None

# Generate smart variants to try against Steam
def generate_candidates(name: str) -> List[str]:
    cands = []
    n = name

    # fix common spelling/format
    n = re.sub(r"\bfield\s*tested\b", "Field-Tested", n, flags=re.I)
    n = re.sub(r"\bminimal\s*wear\b", "Minimal Wear", n, flags=re.I)
    n = n.replace("Stat-Trak", "StatTrak™").replace("Stat trak", "StatTrak™").replace("Stat trak", "StatTrak™")
    n = n.replace("Glock ", "Glock-").replace("USP ", "USP-").replace("USP-S", "USP-S")  # keep USP-S
    n = n.replace("Dust 2", "Dust II")

    # Ensure pipe between weapon and skin if missing (basic heuristic for weapons)
    weapon_list = ["AK-47","M4A4","AWP","Glock-18","USP-S","Galil AR","Galil","Nomad Knife","Butterfly Knife","Driver Gloves","M4A1-S","FAMAS","AUG","SSG 08","CZ75-Auto","P250","Five-SeveN","Desert Eagle","Tec-9","P2000","MP9","MP7","UMP-45","PP-Bizon","P90","MAC-10","SG 553","SCAR-20","G3SG1","Nova","XM1014","MAG-7","Negev","M249"]
    had_pipe = ("|" in n)
    for w in weapon_list:
        if n.startswith(w) and not had_pipe and "(" in n:
            # e.g. "AWP Neo-Noir (Field-Tested)" -> "AWP | Neo-Noir (Field-Tested)"
            parts = n.split("(", 1)
            left = parts[0].strip()
            rest = "(" + parts[1]
            skin = left[len(w):].strip()
            if skin:
                cands.append(f"{w} | {skin} {rest}".strip())

    # Knives/gloves often need the star prefix
    if ("Knife" in n or "Gloves" in n) and not n.startswith("★ "):
        cands.append("★ " + n)

    # Operation cases
    if "Broken Fang Case" in n and not n.startswith("Operation"):
        cands.append("Operation " + n)
    if "Riptide Case" in n and not n.startswith("Operation"):
        cands.append("Operation " + n)

    # eSports capitalization
    if n.lower().startswith("esports "):
        cands.append(n.replace("Esports", "eSports"))

    # Stickers: add "Sticker | " prefix and (Holo/Glitter/Gold) parenthesis if present
    if "copenhagen" in n.lower() or "paris 2023" in n.lower() or "austin 2025" in n.lower():
        # Try to detect tiers
        tier = None
        if "holo" in n.lower(): tier = "Holo"
        if "glitter" in n.lower(): tier = "Glitter"
        if "gold" in n.lower(): tier = "Gold"
        # Team/player token before event
        m = re.match(r"(.*?)(?:\s*\((?:stat.*)?\))?\s*(copenhagen 2024|paris 2023|austin 2025)", n, flags=re.I)
        if m:
            left, event = m.group(1).strip(), m.group(2).strip()
            left = left.replace("Holo","").replace("Glitter","").replace("Gold","").strip()
            if tier:
                cands.append(f"Sticker | {left} ({tier}) | {event.title()}")
            else:
                cands.append(f"Sticker | {left} | {event.title()}")

    # Music kits: ensure proper prefix
    if "music" in n.lower() or "stat" in n.lower() and ":" in n:
        # try StatTrak™ Music Kit | Artist, Title
        cands.append("StatTrak™ Music Kit | " + n.replace(" (Stat-Trak)", "").replace(" (StatTrak)", ""))

    # If none generated, try original cleaned variants
    cands.extend([n])

    # Deduplicate preserving order
    seen = set()
    out = []
    for x in cands:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

# ---------- Data fetch ----------
async def fetch_sheet_rows(ac: httpx.AsyncClient) -> List[Dict[str, Any]]:
    last_status, last_err = None, None
    for url in CSV_URLS:
        try:
            r = await ac.get(url)
            last_status = r.status_code
            if r.status_code == 200 and r.text and "," in r.text.splitlines()[0]:
                rows = list(csv.DictReader(io.StringIO(r.text)))
                return rows
        except Exception as e:
            last_err = str(e)
    detail = "Unable to fetch sheet CSV"
    if last_status: detail += f" (HTTP {last_status})"
    if last_err: detail += f" - {last_err}"
    raise HTTPException(status_code=502, detail=detail)

async def steam_price(ac: httpx.AsyncClient, name: str) -> Optional[float]:
    params = {
        "appid": "730",
        "currency": str(STEAM_CURRENCY_CODE),
        "market_hash_name": name,
        "format": "json",
    }
    try:
        r = await ac.get("https://steamcommunity.com/market/priceoverview/", params=params)
        if r.status_code != 200: return None
        data = r.json()
        if not data.get("success"): return None
        price_str = data.get("median_price") or data.get("lowest_price")
        return _num_from_price_str(price_str)
    except Exception:
        return None

async def price_for_item(ac: httpx.AsyncClient, raw_name: str) -> Optional[float]:
    # 1) exact map first
    mapped = map_to_steam_exact(raw_name)
    if mapped:
        p = await steam_price(ac, mapped)
        if p is not None: return p
    # 2) try generated candidates
    for cand in generate_candidates(raw_name):
        p = await steam_price(ac, cand)
        if p is not None: return p
    # 3) last resort: original raw name
    return await steam_price(ac, raw_name)

# ---------- Routes ----------
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}

@app.get("/prices")
async def prices():
    async with client() as ac:
        rows = await fetch_sheet_rows(ac)

        async def one(row: Dict[str, Any]):
            name = (row.get("item_name") or "").strip()
            source = (row.get("source") or "steam").strip().lower()
            paid = _parse_float(row.get("paid_price"))
            qty = _parse_int(row.get("quantity"))

            await asyncio.sleep(REQUEST_DELAY)
            # Steam-first so we get data even if SkinPort is flaky
            current = await price_for_item(ac, name)

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
