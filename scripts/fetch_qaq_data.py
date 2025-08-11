#!/usr/bin/env python3
import os
import sys
import requests
import pandas as pd
from datetime import datetime, timezone
from requests.auth import HTTPBasicAuth
from pathlib import Path

# ───────────────── CONFIG ─────────────────
SENSOR_IDS = [
    "MOD-00616","MOD-00632","MOD-00625","MOD-00631","MOD-00623",
    "MOD-00628","MOD-00620","MOD-00627","MOD-00630","MOD-00624"
]

OUTPUT_DIR = "data"                     # flat folder in repo root
WRITE_EMPTY_FILES = True                # write a CSV even if API returns no rows
USE_UTC_FOR_DATE = True                 # FINAL by-date uses UTC boundaries

API_KEY = os.getenv("QUANTAQ_API_KEY")
if not API_KEY:
    raise EnvironmentError("❌ QUANTAQ_API_KEY environment variable not set.")

API_BASE = "https://api.quant-aq.com/v1"

# ─────────────── helpers ────────────────
def fetch_final_by_date(sn: str, date_str: str) -> pd.DataFrame:
    """Fetch FINAL (calibrated) data with pagination."""
    url = f"{API_BASE}/devices/{sn}/data-by-date/{date_str}/"
    all_rows = []
    hops = 0
    while url:
        r = requests.get(
            url,
            auth=HTTPBasicAuth(API_KEY, ""),
            headers={"Accept": "application/json"},
            timeout=60
        )
        if r.status_code != 200:
            raise RuntimeError(f"{sn} {date_str}: {r.status_code} {r.text}")

        payload = r.json() or {}
        data = payload.get("data", [])
        if data:
            all_rows.extend(data)

        meta = payload.get("meta", {}) or {}
        next_url = meta.get("next_url")
        url = next_url if next_url else None
        hops += 1
        if hops > 2000:  # hard guard
            break

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()

# ─────────────── main ────────────────
def main() -> int:
    cwd = Path.cwd()
    outdir = (cwd / OUTPUT_DIR).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    # Date for QuantAQ by-date window
    if USE_UTC_FOR_DATE:
        date_str = datetime.now(timezone.utc).date().isoformat()
        date_note = "UTC"
    else:
        # (kept for reference; UTC is recommended for FINAL endpoint)
        from datetime import timezone as _tz
        import pytz
        pst = pytz.timezone("America/Los_Angeles")
        date_str = datetime.now(_tz.utc).astimezone(pst).date().isoformat()
        date_note = "PST"

    print(f"PWD: {cwd}")
    print(f"OUTPUT_DIR: {outdir}")
    print(f"Fetching QuantAQ FINAL (calibrated) for {date_str} [{date_note}]")

    saved = 0
    for sn in SENSOR_IDS:
        try:
            print(f"→ {sn} {date_str}")
            df = fetch_final_by_date(sn, date_str)

            # Always write a file so we can see the run produced output
            out_path = outdir / f"{sn}_{date_str}.csv"
            if df.empty and not WRITE_EMPTY_FILES:
                print(f"   No data (skipped write)")
                continue

            df.to_csv(out_path, index=False)
            print(f"   Saved {len(df)} rows → {out_path}")
            saved += 1

        except Exception as e:
            print(f"   Error: {e}")

    if saved == 0:
        print("❌ No files were written — failing the job so this is visible.")
        return 1

    print(f"✅ Done. Files written: {saved}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
