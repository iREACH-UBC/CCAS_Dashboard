#!/usr/bin/env python3
# make_hellolamppost_json.py  –  2025-06-30
#
# Create HelloLamppostData.json from:
#   • pollutant_data.json      (AQHI, pollutants, advisory flag)
#   • sensor_metadata.csv      (id, name, sensor_number)

from __future__ import annotations
import json, sys
from pathlib import Path
import pandas as pd

# ───────── paths ───────────────────────────────────────────────
POLLUTANT_FILE = Path("pollutant_data.json")
META_CSV       = Path("sensor_metadata.csv")
OUTPUT_FILE    = Path("HelloLamppostData.json")

KEEP: set[str] | None = None        # whitelist if needed

# ───────── helpers ─────────────────────────────────────────────
def aqhi_label(v):
    if v == "N/A":
        return "no data"
    if v <= 3:
        return "Low health risk"
    if v <= 6:
        return "Moderate health risk"
    if v <= 10:
        return "High health risk"
    return "Very high health risk"

def load_metadata() -> dict[str, dict]:
    """
    Read id, name, sensor_number from sensor_metadata.csv,
    trim whitespace, drop duplicate IDs, return dict keyed by id.
    """
    if not META_CSV.exists():
        sys.exit(f"[FATAL] {META_CSV} missing – cannot map id→name")

    df = pd.read_csv(
        META_CSV,
        usecols=["id", "name", "sensor_number"],   # ignore lat/lon/region
        dtype=str,
        engine="python",
        skipinitialspace=True,       # trims spaces right after commas
        keep_default_na=False        # blank cells -> ""
    )

    # Trim whitespace in every string column
    for col in df.columns:
        df[col] = df[col].str.strip()

    # Ensure unique ids (keep first occurrence)
    df = df.drop_duplicates(subset="id", keep="first")

    return df.set_index("id").to_dict(orient="index")

# ───────── main build ──────────────────────────────────────────
def main():
    if not POLLUTANT_FILE.exists():
        sys.exit(f"[FATAL] {POLLUTANT_FILE} missing")

    big_json  = json.loads(POLLUTANT_FILE.read_text(encoding="utf-8"))
    meta      = load_metadata()
    kiosk_out = {}

    for s in big_json.get("sensors", []):
        sid = s["id"]
        m   = meta.get(sid, {})          # ← lookup by sensor ID

        site_name     = m.get("name")          or sid
        sensor_number = m.get("sensor_number") or sid

        if KEEP and sensor_number not in KEEP and sid not in KEEP:
            continue

        latest   = s.get("latest", {})
        aqhi_val = latest.get("aqhi", "N/A")
        primary  = latest.get("primary", "N/A")
        conc     = (
            latest.get("pollutants", {}).get(str(primary).lower(), "N/A")
            if isinstance(primary, str) else "N/A"
        )

        kiosk_out[sensor_number] = {
            "name":                    site_name,
            "label":                   aqhi_label(aqhi_val),
            "value":                   aqhi_val if aqhi_val == "N/A" else int(round(aqhi_val)),
            "top_contributor":         primary,
            "pollutant_concentration": conc if conc == "N/A" else round(conc, 2),
            "aq_advisory":             bool(s.get("active_alert", False)),
        }

    OUTPUT_FILE.write_text(json.dumps(kiosk_out, indent=4) + "\n", encoding="utf-8")
    print(f"[SUCCESS] wrote {OUTPUT_FILE} ({len(kiosk_out)} sensors)")

if __name__ == "__main__":
    main()




def load_metadata() -> dict[str, dict]:
    """
    Return {id : {name, sensor_number}} with
    • whitespace trimmed everywhere
    • quotes / stray BOM removed
    • possible “.0” suffix (if Excel saved IDs as numbers) stripped
    • duplicate IDs dropped (keep first)
    """

    if not META_CSV.exists():
        sys.exit(f"[FATAL] {META_CSV} missing – cannot map id→name")

    df = pd.read_csv(
        META_CSV,
        usecols=["id", "name", "sensor_number"],   # ignore other columns
        dtype=str,
        engine="python",        # more forgiving CSV parser
        skipinitialspace=True,  # trim space right after commas
        keep_default_na=False   # blank cells → ""
    )

    # ── clean every string column ─────────────────────────────
    for col in df.columns:
        df[col] = (
            df[col]
              .str.strip()                 # remove leading/trailing spaces
              .str.replace('"', '', regex=False)   # drop any stray quotes
              .str.replace(r'\ufeff', '', regex=True)  # remove hidden BOM
        )

    # Excel sometimes stores IDs as 2021.0 → strip trailing ".0"
    df["id"] = df["id"].str.replace(r"\.0$", "", regex=True)

    # ensure uniqueness
    df = df.drop_duplicates(subset="id", keep="first")

    # build fast lookup
    return df.set_index("id").to_dict(orient="index")
  
meta_by_id = load_metadata()          # <-- call the function first

print(meta_by_id["2021"]["name"])     # West Vancouver Memorial Library
print(meta_by_id["2040"]["name"])     # Gillies Bay Public Library
