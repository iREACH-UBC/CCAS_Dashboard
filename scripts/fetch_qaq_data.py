#!/usr/bin/env python3
import os, sys, requests, pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from requests.auth import HTTPBasicAuth

SENSOR_IDS = [
    "MOD-00616","MOD-00632","MOD-00625","MOD-00631","MOD-00623",
    "MOD-00628","MOD-00620","MOD-00627","MOD-00630","MOD-00624",
    "MOD-00629", "MOD-00614","MOD-00624", "MOD-00617", "MOD-00618", "MOD-00613"
]

OUTPUT_DIR = "data"          # flat folder in repo root
WRITE_EMPTY_FILES = True     # write CSV even if API returns 0 rows
API_BASE = "https://api.quant-aq.com/v1"

API_KEY = os.getenv("QUANTAQ_API_KEY")
if not API_KEY:
    raise EnvironmentError("❌ QUANTAQ_API_KEY not set")

def fetch_final_by_date(sn: str, date_str: str) -> pd.DataFrame:
    url = f"{API_BASE}/devices/{sn}/data-by-date/{date_str}/"  # FINAL (calibrated)
    all_rows, hops = [], 0
    while url:
        r = requests.get(
            url, auth=HTTPBasicAuth(API_KEY, ""),
            headers={"Accept": "application/json"}, timeout=60
        )
        if r.status_code != 200:
            raise RuntimeError(f"{sn} {date_str}: {r.status_code} {r.text}")
        payload = r.json() or {}
        rows = payload.get("data", [])
        if rows: all_rows.extend(rows)
        url = (payload.get("meta") or {}).get("next_url")
        hops += 1
        if hops > 2000: break
    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()

def main() -> int:
    outdir = (Path.cwd() / OUTPUT_DIR).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now(timezone.utc).date().isoformat()
    print(f"PWD: {Path.cwd()}")
    print(f"OUTPUT_DIR: {outdir}")
    print(f"Fetching FINAL (calibrated) for {date_str} [UTC]")

    wrote = 0
    for sn in SENSOR_IDS:
        try:
            print(f"→ {sn} {date_str}")
            df = fetch_final_by_date(sn, date_str)
            out_path = outdir / f"{sn}_{date_str}.csv"
            if df.empty and not WRITE_EMPTY_FILES:
                print("   No data (skipped write)")
                continue
            df.to_csv(out_path, index=False)
            print(f"   Saved {len(df)} rows → {out_path}")
            wrote += 1
        except Exception as e:
            print(f"   Error: {e}")

    if wrote == 0:
        print("❌ No files were written; failing job.")
        return 1
    print(f"✅ Done. Files written: {wrote}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
