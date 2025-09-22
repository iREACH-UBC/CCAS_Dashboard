#!/usr/bin/env python3
# fetch_ramp_data_bg.py – RAMP raw file pull by PST-based filename rule
import os, csv, requests
from pathlib import Path
from datetime import datetime, time, timedelta
import pytz

# Sensors to pull
SENSOR_IDS = ["2021","2022","2023","2024","2026","2030","2031","2032","2033","2034","2039","2040","2041","2042","2043"]

BASE_URL = "http://18.222.146.48/RAMP/v1/raw"
OUTPUT_DIR = Path("data")
TIMEOUT = 45

PST = pytz.timezone("America/Los_Angeles")  # effectively PST/PDT

def file_date_by_pst(now_pst: datetime) -> datetime.date:
    """
    Filenames are 'YYYY-MM-DD-<id>.txt' with PST rules:
      - before 06:00 → use yesterday
      - 06:00–20:59 → use today
      - ≥21:00      → use tomorrow
    """
    hhmm = now_pst.time()
    if hhmm < time(6, 0):
        return (now_pst - timedelta(days=1)).date()
    elif hhmm < time(21, 0):
        return now_pst.date()
    else:
        return (now_pst + timedelta(days=1)).date()

def parse_file(text: str):
    """
    Source format: line is key1,val1,key2,val2,...
    Header is taken from even-indexed tokens of first line; each line contributes odd-indexed values.
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return [], []
    header_tokens = lines[0].split(',')
    header = header_tokens[::2]
    data = []
    for line in lines:
        toks = line.split(',')
        if len(toks) < 2:
            continue
        data.append(toks[1::2])  # values on odd indices
    return header, data

def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    now_pst = datetime.now(PST)
    fdate = file_date_by_pst(now_pst)
    fdate_str = fdate.strftime("%Y-%m-%d")
    print(f"PST now: {now_pst.isoformat()}  → filename date: {fdate_str}")

    headers = {"User-Agent": "gh-actions/ramp/1.0", "Accept": "text/plain,*/*;q=0.1"}
    written = 0

    for sid in SENSOR_IDS:
        filename = f"{fdate_str}-{sid}.txt"
        file_url = f"{BASE_URL}/{sid}/data/{filename}"
        print(f"\n[{sid}] GET {file_url}")

        try:
            resp = requests.get(file_url, timeout=TIMEOUT, headers=headers)
        except requests.RequestException as e:
            print(f"   Request error: {e}")
            continue

        if resp.status_code != 200:
            print(f"   HTTP {resp.status_code}: {resp.text[:180]}")
            continue

        header, rows = parse_file(resp.text)
        if not rows:
            print("   No rows parsed.")
            continue

        out_csv = OUTPUT_DIR / f"{sid}_{fdate_str}.csv"
        with out_csv.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(rows)
        print(f"   Wrote {len(rows)} rows → {out_csv}")
        written += 1

    if written == 0:
        print("No files written.")
        return 1
    print(f"Done. Files written: {written}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
