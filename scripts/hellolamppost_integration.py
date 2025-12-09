#!/usr/bin/env python3
# make_hellolamppost_json.py  –  2025-06-30
#
# Create HelloLamppostData.json from:
#   • pollutant_data.json      (AQHI, pollutants, advisory flag)
#   • sensor_metadata.csv      (id, name, sensor_number)
#
# Note that the data is imported from the pollutant_data.json, not directly
# from the calibrated data files.

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
    """
    Map AQHI numeric value → health risk label.
    If v is None / NaN / non-numeric / N/A-like, return 'Not Available'.
    """
    # Treat missing / NA as not available
    if v is None or pd.isna(v):
        return "Not Available"

    # Handle string forms like "N/A", "5", " 7 "
    if isinstance(v, str):
        v_str = v.strip()
        if not v_str or v_str.upper() in {"N/A", "NA", "NONE"}:
            return "Not Available"
        try:
            v = float(v_str)
        except ValueError:
            return "Not Available"

    # From here we expect numeric; if not, fall back
    try:
        if v <= 3:
            return "Low health risk"
        if v <= 6:
            return "Moderate health risk"
        if v <= 10:
            return "High health risk"
        return "Very high health risk"
    except TypeError:
        return "Not Available"


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
              .str.strip()                       # remove leading/trailing spaces
              .str.replace('"', '', regex=False) # drop any stray quotes
              .str.replace(r'\ufeff', '', regex=True)  # remove hidden BOM
        )

    # Excel sometimes stores IDs as 2021.0 → strip trailing ".0"
    df["id"] = df["id"].str.replace(r"\.0$", "", regex=True)

    # ensure uniqueness
    df = df.drop_duplicates(subset="id", keep="first")

    # build fast lookup
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

        latest   = s.get("latest", {}) or {}
        aqhi_val = latest.get("aqhi", "N/A")
        primary  = latest.get("primary", "N/A")

        # ── pollutant concentration safely ─────────────────────
        val = None
        pollutants = latest.get("pollutants") or {}
        if isinstance(primary, str) and isinstance(pollutants, dict):
            # try multiple forms of the key
            keys_to_try = [primary, primary.lower(), primary.strip().lower()]
            for k in keys_to_try:
                if k in pollutants:
                    val = pollutants[k]
                    break

        # decide what to display for concentration
        if isinstance(val, (int, float)) and not pd.isna(val):
            conc = round(val, 2)
        else:
            conc = "N/A"  # string when not available

        # ── AQHI numeric value for output ──────────────────────
        if isinstance(aqhi_val, (int, float)) and not pd.isna(aqhi_val):
            aqhi_value_out = int(round(aqhi_val))
        else:
            # on None / NaN / string 'N/A' etc.
            aqhi_value_out = "N/A"

        # ── primary pollutant for display ──────────────────────
        if primary is None or (isinstance(primary, str) and not primary.strip()):
            primary_out = "Not Available"
        else:
            primary_out = primary

        kiosk_out[sensor_number] = {
            "name":                    site_name,
            "label":                   aqhi_label(aqhi_val),
            "value":                   aqhi_value_out,
            "top_contributor":         primary_out,
            "pollutant_concentration": conc if conc == "N/A" else round(conc, 2),
            "aq_advisory":             bool(s.get("active_alert", False)),
        }

    OUTPUT_FILE.write_text(json.dumps(kiosk_out, indent=4) + "\n", encoding="utf-8")
    print(f"[SUCCESS] wrote {OUTPUT_FILE} ({len(kiosk_out)} sensors)")


if __name__ == "__main__":
    main()
