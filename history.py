
import os
import time
import yfinance as yf
import pandas as pd
import numpy as np

# Configuration
TICKERS = [
    "ADANIPOWER.NS",
    "ATGL.NS",
    "TATAGOLD.NS",
    "ETERNAL.NS",
    "GROWWPOWER.NS",
]

PERIOD = "60d"
INTERVAL = "2m"
OUT_DIR = "output"
USE_ADJUSTED = False

os.makedirs(OUT_DIR, exist_ok=True)

def _find_datetime_column_after_reset(df: pd.DataFrame) -> str:
    """
    After df.reset_index(), find which column is the datetime column.
    Returns the column name or raises ValueError.
    """
    # candidate: named index moved to column
    # look for any column with datetime dtype
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            return col
    # fallback: check for common names
    for name in ("Date", "Datetime", "date", "datetime", "index"):
        if name in df.columns:
            return name
    raise ValueError("No datetime-like column found in DataFrame after reset_index()")

def fetch_ticker_df(ticker, period=PERIOD, interval=INTERVAL):
    """
    Robust fetch:
      - fetches t.history(...)
      - converts timestamps to IST (Asia/Kolkata)
      - creates 'date' column: for minute intervals -> 'YYYY-MM-DD HH:MM:SS',
                              for daily -> 'YYYY-MM-DD'
      - returns None if no data
    """
    t = yf.Ticker(ticker)
    try:
        df = t.history(period=period, interval=interval, auto_adjust=False)
    except Exception as e:
        print(f"Error calling history() for {ticker}: {e}")
        return None

    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        print(f"⚠️ Warning: No data returned for {ticker}")
        return None

    # --- SAFE timezone handling on the INDEX (not the DataFrame) ---
    # We expect the index to be a DatetimeIndex (maybe tz-aware, maybe tz-naive).
    try:
        idx = df.index  # DatetimeIndex usually
        # If index is not datetime, coerce it
        if not pd.api.types.is_datetime64_any_dtype(idx):
            # try to coerce index to datetime
            try:
                df = df.reset_index()
                # try common names then convert
                dtcol = None
                for name in ("Date", "Datetime", "datetime", "date", "index"):
                    if name in df.columns and pd.api.types.is_datetime64_any_dtype(df[name]):
                        dtcol = name
                        break
                if dtcol is None:
                    # coerce first column (index became 'index' after reset) 
                    df.columns = [str(c) for c in df.columns]
                    df = df.rename(columns={df.columns[0]: "tmp_dt"})
                    df["tmp_dt"] = pd.to_datetime(df["tmp_dt"], errors="coerce")
                    dtcol = "tmp_dt"
                # set as index and continue
                df = df.set_index(pd.to_datetime(df[dtcol], errors="coerce"))
            except Exception:
                # last resort: try to parse existing index to datetime
                idx = pd.to_datetime(df.index, errors='coerce')
                df.index = idx

        # by now index should be datetime-like (maybe tz-aware or tz-naive)
        # if tz-aware -> convert to Asia/Kolkata
        if pd.api.types.is_datetime64tz_dtype(df.index):
            df.index = df.index.tz_convert("Asia/Kolkata")
        else:
            # tz-naive -> localize as US/Eastern (Yahoo stores intraday in ET),
            # then convert to Asia/Kolkata.
            # BUT only localize if values look like they represent ET; this is the common case.
            try:
                df.index = df.index.tz_localize("US/Eastern").tz_convert("Asia/Kolkata")
            except Exception:
                # If localize fails (index already tz-aware or other issue), try a safe convert:
                try:
                    df.index = df.index.tz_convert("Asia/Kolkata")
                except Exception:
                    # give up on timezone conversion; keep original index
                    pass
    except Exception as e:
        # If anything in tz handling fails, print debug and continue with original df
        print(f"Warning: timezone conversion problem for {ticker}: {e}")

    # --- Build date column after index is in IST (or best-effort) ---
    # Reset index to get timestamp as a column
    df = df.reset_index()

    # Find which column contains the datetime values after reset
    datetime_col = None
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            datetime_col = col
            break
    if datetime_col is None:
        # fallback to common names
        for name in ("Date", "Datetime", "datetime", "date", "index"):
            if name in df.columns:
                datetime_col = name
                break

    if datetime_col is None:
        print(f"⚠️ Warning: could not locate datetime column for {ticker}; columns: {df.columns.tolist()}")
        return None

    # coerce to datetime (safe)
    df[datetime_col] = pd.to_datetime(df[datetime_col], errors="coerce")
    if df[datetime_col].isna().all():
        print(f"⚠️ Warning: datetime parse failed for {ticker}")
        return None

    # choose formatting: minute intervals include time; daily only date
    is_minute = str(interval).endswith("m")
    if is_minute:
        df["date"] = df[datetime_col].dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        df["date"] = df[datetime_col].dt.date.astype(str)

    # put date first and reorder desired OHLCV columns if present
    cols = ["date"] + [c for c in df.columns if c != "date"]
    df = df[cols]

    desired_front = ["date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "Dividends", "Stock Splits"]
    present_front = [c for c in desired_front if c in df.columns]
    rest = [c for c in df.columns if c not in present_front]
    df = df[present_front + rest]

    df = df.reset_index(drop=True)
    return df

def save_individual_csv(ticker, df, out_dir=OUT_DIR):
    if df is None or df.empty:
        print(f"Skipping save for {ticker}: no data.")
        return
    fname = os.path.join(out_dir, f"{ticker.replace('.', '_')}.min_csv")
    # ensure directory exists
    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(fname, index=False)
    print(f"Saved: {fname}")

def main():
    ticker_dfs = {}
    for ticker in TICKERS:
        print(f"Fetching {ticker} ...")
        try:
            df = fetch_ticker_df(ticker)
            if df is None:
                ticker_dfs[ticker] = None
                continue
            save_individual_csv(ticker, df)
            ticker_dfs[ticker] = df
            # polite short pause
            time.sleep(0.3)
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
            ticker_dfs[ticker] = None

if __name__ == "__main__":
    main()
