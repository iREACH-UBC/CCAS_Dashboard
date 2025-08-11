#!/usr/bin/env python3
import os
import requests
import pandas as pd
from datetime import datetime, timezone
import pytz
from requests.auth import HTTPBasicAuth

# ----------------------------------------
# CONFIGURATION
# ----------------------------------------
SENSOR_IDS = [
    "MOD-00616", "MOD-00632", "MOD-00625", "MOD-00631", "MOD-00623",
    "MOD-00628", "MOD-00620", "MOD-00627", "MOD-00630", "MOD-00624"
]

API_KEY = os.getenv("QUANTAQ_API_KEY")
if not API_KEY:
    raise EnvironmentError("❌ QUANTAQ_API_KEY environment variable not set.")

OUTPUT_DIR = "data"  # root; we'll create per-sensor subfolders
os.makedirs(OUTPUT_DIR, exist_ok=True)

API_BASE = "https://api.quant-aq.com/v1"

# ----------------------------------------
# DETERMINE FILE DATE (Based on PST)
# Note: QuantAQ by-date endpoints use GMT (UTC) boundaries. Using PST to pick
# the date may shift what hours are included near day edges.
# ----------------------------------------
pst = pytz.timezone("America/Los_Angeles")
now_pst = datetime.now(timezone.utc).astimezone(pst)
file_date = now_pst.date()
date_str = file_date.isoformat()  # YYYY-MM-DD
print(f"Fetching QuantAQ FINAL (calibrated) data for {date_str} (PST day)")

def fetch_final_by_date(sn: str, date_str: str) -> pd.DataFrame:
    """
    Fetch final (calibrated) data for a device and date, walking pagination.
    """
    url = f"{API_BASE}/devices/{sn}/data-by-date/{date_str}/"  # FINAL (not raw)
    all_rows = []

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

        meta = payload.get("meta", {})
        next_url = meta.get("next_url")
        if next_url:
            url = next_url
        else:
            break

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()

# ----------------------------------------
# FETCH FINAL DATA BY DATE
# ----------------------------------------
for sn in SENSOR_IDS:
    try:
        print(f"→ {sn} {date_str}")
        df = fetch_final_by_date(sn, date_str)
        if df.empty:
            print(f"   No data")
            continue

        # Create per-sensor subfolder to align with merge script expectations
        sensor_dir = os.path.join(OUTPUT_DIR, sn)
        os.makedirs(sensor_dir, exist_ok=True)

        # Save with *_final.csv suffix to match your merger
        csv_path = os.path.join(sensor_dir, f"{date_str}_final.csv")
        df.to_csv(csv_path, index=False)
        print(f"   Saved {len(df)} rows → {csv_path}")

    except Exception as e:
        print(f"   Error fetching {sn}: {e}")
