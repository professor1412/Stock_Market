
import os
import tempfile
import asyncio
from typing import Optional, List
from datetime import datetime

import pandas as pd
import yfinance as yf
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
    ET = ZoneInfo("US/Eastern")
except Exception:
    import pytz
    IST = pytz.timezone("Asia/Kolkata")
    ET = pytz.timezone("US/Eastern")

# ---------- CONFIG ----------
OUT_DIR = os.environ.get("OUT_DIR", "output")
CSV_SUFFIX = os.environ.get("CSV_SUFFIX", ".1m.csv")
INTERVAL = "1m"
FETCH_PERIOD = os.environ.get("FETCH_PERIOD", "2m")
API_KEY = os.environ.get("API_KEY", None)
TICKERS = os.environ.get("TICKERS", "ADANIPOWER.NS").split(",")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))
ENABLE_BACKGROUND = os.environ.get("ENABLE_BACKGROUND", "1") != "0"

os.makedirs(OUT_DIR, exist_ok=True)

app = FastAPI(title="Auto 1-min Open/Close Appender")

def ticker_fname(ticker: str) -> str:
    return os.path.join(OUT_DIR, f"{ticker.replace('.', '_')}{CSV_SUFFIX}")

def _fetch_last_1m_candle(ticker: str):
    t = yf.Ticker(ticker)
    try:
        df = t.history(period=FETCH_PERIOD, interval=INTERVAL, auto_adjust=False)
    except Exception as e:
        raise RuntimeError(f"yfinance.history error: {e}")

    if df is None or df.empty:
        return None

    if not isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()
        for c in ("Datetime", "Date", "date", "index"):
            if c in df.columns:
                df = df.set_index(pd.to_datetime(df[c], errors="coerce"))
                break

    try:
        if pd.api.types.is_datetime64tz_dtype(df.index):
            df.index = df.index.tz_convert(IST)
        else:
            df.index = df.index.tz_localize(ET).tz_convert(IST)
    except Exception:
        df.index = pd.to_datetime(df.index, errors='coerce')
        try:
            df.index = df.index.tz_localize(ET).tz_convert(IST)
        except Exception:
            pass

    df = df[~df.index.isna()]
    if df.empty:
        return None

    last_idx = df.index[-1]
    row = df.iloc[-1]

    ts_str = last_idx.strftime("%Y-%m-%d %H:%M:%S")
    if ("Open" not in row.index) or ("Close" not in row.index):
        return None

    open_v = float(row["Open"])
    close_v = float(row["Close"])
    return ts_str, open_v, close_v

def atomic_append_row_csv(fname: str, row_df: pd.DataFrame):
    os.makedirs(os.path.dirname(fname), exist_ok=True)
    if os.path.exists(fname):
        existing = pd.read_csv(fname, dtype=str)
        existing_dates = set(existing['date'].astype(str).tolist())
    else:
        existing = None
        existing_dates = set()

    new_rows = row_df[~row_df['date'].astype(str).isin(existing_dates)]
    if new_rows.empty:
        return 0

    tmp_fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(fname), prefix=".tmp_")
    os.close(tmp_fd)
    try:
        if existing is None:
            new_rows.to_csv(tmp_path, index=False)
            os.replace(tmp_path, fname)
            return len(new_rows)
        else:
            combined = pd.concat([existing, new_rows], ignore_index=True)
            combined = combined.drop_duplicates(subset=['date'], keep='last')
            combined = combined.sort_values('date').reset_index(drop=True)
            combined.to_csv(tmp_path, index=False)
            os.replace(tmp_path, fname)
            return len(new_rows)
    finally:
        if os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass

@app.get("/run")
@app.post("/run")
async def run_once(ticker: str = Query(..., description="Ticker e.g. ADANIPOWER.NS"),
                   key: Optional[str] = Query(None, description="Optional API key")):
    if API_KEY:
        if key != API_KEY:
            raise HTTPException(status_code=403, detail="Invalid API key")
    try:
        res = _fetch_last_1m_candle(ticker)
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
    if res is None:
        return {"ok": True, "appended": 0, "message": "No 1-minute data available at this moment"}
    ts_str, open_v, close_v = res
    row_df = pd.DataFrame([{"date": ts_str, "open": open_v, "close": close_v}])
    fname = ticker_fname(ticker)
    appended = atomic_append_row_csv(fname, row_df)
    return {"ok": True, "appended": appended, "timestamp": ts_str, "file": fname}

@app.get("/")
async def root():
    return {"ok": True, "time": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S %Z"),
            "background_enabled": ENABLE_BACKGROUND, "tickers": TICKERS}

background_task = None
stop_background = False

async def background_worker_loop(tickers: List[str]):
    """
    Infinite loop: every POLL_INTERVAL_SECONDS fetch last 1m for each ticker and append if new.
    """
    global stop_background
    while not stop_background:
        start = asyncio.get_event_loop().time()
        for tk in tickers:
            try:
                res = _fetch_last_1m_candle(tk)
            except Exception as e:
                # swallow errors (could log)
                print(f"[background] fetch error for {tk}: {e}")
                res = None
            if res is None:
                continue
            ts_str, open_v, close_v = res
            row_df = pd.DataFrame([{"date": ts_str, "open": open_v, "close": close_v}])
            fname = ticker_fname(tk)
            try:
                appended = atomic_append_row_csv(fname, row_df)
                if appended:
                    print(f"[background] {tk} appended {appended} row(s) at {ts_str}")
            except Exception as e:
                print(f"[background] write error for {tk}: {e}")
        elapsed = asyncio.get_event_loop().time() - start
        to_sleep = max(0, POLL_INTERVAL_SECONDS - elapsed)
        await asyncio.sleep(to_sleep)

@app.on_event("startup")
async def startup_event():
    global background_task, stop_background
    stop_background = False
    if ENABLE_BACKGROUND:
        loop = asyncio.get_event_loop()
        background_task = loop.create_task(background_worker_loop(TICKERS))
        print(f"Background worker started (tickers={TICKERS}, interval={POLL_INTERVAL_SECONDS}s)")

@app.on_event("shutdown")
async def shutdown_event():
    global background_task, stop_background
    stop_background = True
    if background_task:
        background_task.cancel()
        try:
            await background_task
        except asyncio.CancelledError:
            pass
        background_task = None
        print("Background worker stopped")
