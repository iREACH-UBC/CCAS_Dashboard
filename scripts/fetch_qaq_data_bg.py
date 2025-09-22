#!/usr/bin/env python3
# fetch_qaq_data_bg.py  –  QuantAQ FINAL-by-date pull (no repo writes)
import os, sys, requests, pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from requests.auth import HTTPBasicAuth

SENSOR_IDS = [
    "MOD-00616","MOD-00632","MOD-00625","MOD-00631","MOD-00623",
    "MOD-00628","MOD-00620","MOD-00627","MOD-00630","MOD-00624"
]

OUTPUT_DIR = Path("data")     # ephemeral on runner
WRITE_EMPTY_FILES = True
API_BASE = "https://api.quant-aq.com/v1"
TIMEOUT = 60

API_KEY = os.getenv("QUANTAQ_API_KEY")
if not API_KEY:
    print("ERROR: QUANTAQ_API_KEY not set", file=sys.stderr)
    sys.exit(2)

def fetch_final_by_date(sn: str, date_str: str) -> pd.DataFrame:
    url = f"{API_BASE}/devices/{sn}/data-by-date/{date_str}/"
    rows, hops = [], 0
    headers = {"Accept": "application/json", "User-Agent": "gh-actions/qaqd/1.0"}
    while url:
        r = requests.get(url, auth=HTTPBasicAuth(API_KEY, ""), headers=headers, timeout=TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"{sn} {date_str}: {r.status_code} {r.text[:180]}")
        payload = r.json() or {}
        rows.extend(payload.get("data", []) or [])
        url = (payload.get("meta") or {}).get("next_url")
        hops += 1
        if hops > 5000:  # hard stop safety
            break
    return pd.DataFrame(rows)

def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(timezone.utc).date().isoformat()
    print(f"Fetching FINAL (calibrated) for {date_str} [UTC]")

    wrote = 0
    for sn in SENSOR_IDS:
        try:
            print(f"→ {sn} {date_str}")
            df = fetch_final_by_date(sn, date_str)
            out_path = OUTPUT_DIR / f"{sn}_{date_str}.csv"
            if df.empty and not WRITE_EMPTY_FILES:
                print("   No data (skipped)")
                continue
            df.to_csv(out_path, index=False)
            print(f"   Saved {len(df)} rows → {out_path}")
            wrote += 1
        except Exception as e:
            print(f"   Error: {e}")

    if wrote == 0:
        print("No files written; failing job for visibility.", file=sys.stderr)
        return 1
    print(f"Done. Files written: {wrote}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
