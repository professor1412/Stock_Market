#!/usr/bin/env python3
"""
Google Finance scraper (simple polling).
Writes a CSV where:
- First row: header: empty first cell, then company names
- Subsequent rows: timestamp, price_for_company_1, price_for_company_2, ...

Usage:
    python google_fin_scraper.py
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
import sys

# ---------- USER CONFIG ----------
# Provide either a list of full Google Finance URLs or a list of (symbol, url_suffix)
# Example URL format: "https://www.google.com/finance/quote/TCS:NSE"
COMPANY_PAGES = [
    
    ("ADANI POWER", "https://www.google.com/finance/quote/ADANIPOWER:NSE"),
    ("ADANI TOTAL GAS", "https://www.google.com/finance/quote/ATGL:NSE"),
    ("GROWW POWER", "https://www.google.com/finance/quote/GROWWPOWER:NSE"),
    ("ETERNAL", "https://www.google.com/finance/quote/ETERNAL:NSE"),
    ("TATA GOLD", "https://www.google.com/finance/quote/TATAGOLD:NSE"),
    
]

CSV_FILE = "india_prices_wide.csv"
POLL_INTERVAL_SECONDS = 5.0 
REQUEST_TIMEOUT = 10          
MAX_WORKERS = 6        
USE_IST_TIMESTAMP = True   

# Optional: rotate filename daily or use timestamped filenames. For simplicity we append to CSV_FILE.
# ---------- END CONFIG ----------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

PRICE_REGEX = re.compile(r"([0-9]+(?:[,0-9]*)(?:\.[0-9]+)?)")  # captures 1,234.56 or 1234.56 etc

def get_now_iso():
    if USE_IST_TIMESTAMP:
        # Asia/Kolkata is UTC+5:30
        ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        return ist.isoformat(timespec='milliseconds')
    else:
        return datetime.now(timezone.utc).isoformat(timespec='milliseconds')

def fetch_page(url):
    """Fetch page HTML (returns text) or raise exception."""
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text

def parse_price_from_google_finance(html):
    """
    Try multiple heuristics to find the displayed price on a Google Finance quote page.
    Google often shows price in an element with CSS like 'YMlKec fxKbKc' (class names can change).
    We'll attempt:
      1. div/span with data attributes or class patterns,
      2. meta tags (some pages embed price in JSON-LD or meta),
      3. regex fallback scanning visible numeric strings.
    Returns float or None on failure.
    """
    soup = BeautifulSoup(html, "lxml")

    # heuristic 1: common class pattern historically used by Google Finance
    # e.g. <div class="YMlKec fxKbKc">173.45</div>
    possible = soup.find_all(class_=re.compile(r"YMlKec fxKbKc"))
    for tag in possible:
        text = tag.get_text(strip=True)
        if not text:
            continue
        m = PRICE_REGEX.search(text.replace(',', ''))
        if m:
            try:
                return float(m.group(1).replace(',', ''))
            except:
                pass

    # heuristic 2: look for span with role="heading" near other known markers
    heading_spans = soup.find_all("span", role="heading")
    for s in heading_spans:
        text = s.get_text(strip=True)
        m = PRICE_REGEX.search(text.replace(',', ''))
        if m:
            try:
                return float(m.group(1).replace(',', ''))
            except:
                pass

    # heuristic 3: look for meta/itemprop or JSON-LD scripts
    # JSON-LD: <script type="application/ld+json"> ... price ... </script>
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            js = script.string or ""
            # find "price":"1234.56" style or "price": 1234.56
            m = re.search(r'"price"\s*:\s*"?(?P<p>[0-9,]+(?:\.[0-9]+)?)"?', js)
            if m:
                return float(m.group("p").replace(',', ''))
        except Exception:
            continue

    # heuristic 4: scan visible numeric tokens (take the one most likely to be price)
    # We'll collect numeric tokens and pick the one near top of document
    text = soup.get_text(" ", strip=True)
    numbers = PRICE_REGEX.findall(text.replace(',', ''))
    if numbers:
        # choose the first reasonable numeric-looking item (not a year like 2025)
        for n in numbers[:40]:  # small window
            try:
                val = float(n)
                # simple filter: price > 0 and < 1e6 (very coarse)
                if 0 < val < 1_000_000:
                    return val
            except:
                continue

    # none found
    return None

def fetch_price_for_company(name_url_tuple):
    """
    Given (name, url) fetch page and parse price.
    Returns (name, price_or_None, error_or_None)
    """
    name, url = name_url_tuple
    try:
        html = fetch_page(url)
        price = parse_price_from_google_finance(html)
        if price is None:
            return (name, None, f"parse_failed")
        return (name, price, None)
    except Exception as e:
        return (name, None, str(e))

def ensure_csv_header(path, company_names):
    """Create CSV with header row if not exists. First cell reserved for timestamp label."""
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            header = ["timestamp"] + company_names
            writer.writerow(header)

def append_row_to_csv(path, row):
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)

def main_loop():
    company_names = [c for (c, u) in COMPANY_PAGES]
    company_urls = COMPANY_PAGES

    ensure_csv_header(CSV_FILE, company_names)
    print(f"Started scraping {len(company_names)} pages every {POLL_INTERVAL_SECONDS}s. Writing to {CSV_FILE}")
    # main poll loop
    try:
        while True:
            ts = get_now_iso()
            results = {}
            # fetch concurrently
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futures = {ex.submit(fetch_price_for_company, cu): cu for cu in company_urls}
                for fut in as_completed(futures):
                    name, price, err = fut.result()
                    results[name] = (price, err)

            # build row in same order as header
            row = [ts]
            for name in company_names:
                price, err = results.get(name, (None, "no_result"))
                if price is None:
                    # choose representation for missing price — blank or "NaN"
                    row.append("")   # blank cell on failure
                else:
                    # keep as plain number (no thousands separators)
                    row.append("{:.2f}".format(price))
            append_row_to_csv(CSV_FILE, row)

            # optional: print a short log
            out_log = " | ".join(
                f"{n}:{(results[n][0] if results[n][0] is not None else 'ERR')}" for n in company_names
            )
            print(f"[{ts}] {out_log}")

            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Stopped by user. Exiting.")
        sys.exit(0)

if __name__ == "__main__":
    main_loop()
