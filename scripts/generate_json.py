#!/usr/bin/env python3
# build_sensor_json.py  –  2025-06-23  (CSV timestamps assumed LOCAL Pacific)
# ---------------------------------------------------------------------------
# * Walk calibrated_data/<sensor_id>/
# * For each ID in SENSORS_WANTED, grab the newest “…_calibrated_…csv”
# * Keep the last HISTORY_HOURS of data (local time)
# * Emit a dashboard-ready JSON to OUTPUT_JSON
#
# If a timestamp string in the CSV is already offset-aware (e.g. “…Z”),
# it is converted to America/Vancouver; naïve strings are *localised*
# to America/Vancouver — no unintended time-shifts.
# ---------------------------------------------------------------------------

from __future__ import annotations
import json, re, sys
from datetime import datetime
from pathlib import Path

import pandas as pd               # pip install pandas
import pytz                       # pip install pytz

# ───────────────────────────────────────────────────────────────
# EDIT ME
# ───────────────────────────────────────────────────────────────
SENSORS_WANTED: set[str] | None = {  # set to None to auto-discover all
    "2021", "2040", "2022"
}

BASE_DIR     = Path("calibrated_data")   # where each sensor sub-dir lives
META_CSV     = Path("sensor_metadata.csv")
HISTORY_HOURS = 24                       # 0 → keep whole file
OUTPUT_JSON   = Path("pollutant_data.json")

# ───────────────────────────────────────────────────────────────
PACIFIC = pytz.timezone("America/Vancouver")

KEEP_COLS = [
    "DATE", "CO", "NO", "NO2", "O3", "CO2", "PM2.5",
    "AQHI", "Top_AQHI_Contributor",
]

FILE_RE = re.compile(
    r"^(?P<id>\d+)_?calibrated_\d{4}_\d{2}_\d{2}_to_"
    r"(?P<y>\d{4})_(?P<m>\d{2})_(?P<d>\d{2})\.csv$"
)

# ── helpers ────────────────────────────────────────────────────
def newest_csv(sensor_dir: Path, sid: str) -> Path | None:
    """Return newest calibrated CSV by its ‘…_to_YYYY_MM_DD’ date."""
    best, best_date = None, None
    for p in sensor_dir.glob(f"{sid}*calibrated_*.csv"):
        m = FILE_RE.match(p.name)
        if not m:
            continue
        ts = datetime(int(m["y"]), int(m["m"]), int(m["d"]))
        if best is None or ts > best_date:
            best, best_date = p, ts
    if best:
        print(f"[INFO] {sid}: picked {best.name}", file=sys.stderr)
    else:
        print(f"[WARN] {sid}: no calibrated csvs", file=sys.stderr)
    return best


def read_meta(meta_csv: Path) -> dict[str, dict]:
    if not meta_csv.exists():
        return {}
    df = pd.read_csv(meta_csv, dtype={"id": str})
    return {str(r.id): r.to_dict() for _, r in df.iterrows()}


def to_pacific_iso(ts) -> str | None:
    """Return ISO-8601 string in America/Vancouver; ts already Pacific."""
    return None if pd.isna(ts) else ts.isoformat(timespec="minutes")


# ── core builder ───────────────────────────────────────────────
def build():
    meta = read_meta(META_CSV)
    sensors = []

    for sensor_dir in sorted(BASE_DIR.iterdir()):
        if not sensor_dir.is_dir():
            continue
        sid = sensor_dir.name
        if SENSORS_WANTED and sid not in SENSORS_WANTED:
            continue

        csv_path = newest_csv(sensor_dir, sid)
        if not csv_path:
            continue

        try:
            df = pd.read_csv(csv_path, usecols=lambda c: c in KEEP_COLS)
        except ValueError as e:
            print(f"[ERROR] {csv_path}: {e}", file=sys.stderr)
            continue

        # rename columns & ensure presence
        df.rename(columns={"Top_AQHI_Contributor": "PRIMARY",
                           "PM2.5": "PM25"}, inplace=True)
        for col in ("PRIMARY", "PM25"):
            if col not in df.columns:
                df[col] = None

        # ── timestamp handling (strip stray “Z”, parse, localise) ─────────────
        #
        # Your CSV clock-faces are already Pacific but sometimes end with “Z”.
        # We drop that single trailing “Z”, parse the string → naïve datetime,
        # then *localise* every row to America/Vancouver (no time-shift).
        #
        df["DATE"] = (
            df["DATE"]
              .astype(str)                 # ensure str for regex
              .str.replace(r"Z$", "", regex=True)   # 1️⃣ remove lone trailing Z
        )
        
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")  # 2️⃣ parse
        
        df["DATE"] = df["DATE"].map(                              # 3️⃣ localise
            lambda t: pd.NaT if pd.isna(t) else PACIFIC.localize(t)
        )
        
        df = df.dropna(subset=["DATE"]).sort_values("DATE")       # clean + sort


        # ── apply 24-hour window (local time) ────────────────
        if HISTORY_HOURS > 0 and not df.empty:
            cutoff = df["DATE"].max() - pd.Timedelta(hours=HISTORY_HOURS)
            df = df[df["DATE"] >= cutoff]

        if df.empty:
            print(f"[WARN] {sid}: dataframe empty", file=sys.stderr)
            continue

        # ── latest record ────────────────────────────────────
        last = df.iloc[-1]
        latest = {
            "timestamp": to_pacific_iso(last["DATE"]),
            "aqhi": round(last["AQHI"], 1) if pd.notna(last["AQHI"]) else None,
            "primary": last["PRIMARY"] if isinstance(last["PRIMARY"], str) else None,
            "pollutants": {
                "co":   round(last["CO"],   3) if pd.notna(last["CO"])   else None,
                "no":   round(last["NO"],   3) if pd.notna(last["NO"])   else None,
                "no2":  round(last["NO2"],  3) if pd.notna(last["NO2"])  else None,
                "o3":   round(last["O3"],   3) if pd.notna(last["O3"])   else None,
                "co2":  round(last["CO2"],  3) if pd.notna(last["CO2"])  else None,
                "pm25": round(last["PM25"], 3) if pd.notna(last["PM25"]) else None,
            },
        }

        # ── history rows ─────────────────────────────────────
        history = []
        for _, r in df.iterrows():
            history.append([
                to_pacific_iso(r["DATE"]),
                round(r["AQHI"], 1) if pd.notna(r["AQHI"]) else None,
                r["PRIMARY"] if isinstance(r["PRIMARY"], str) else None,
                round(r["CO"],   3) if pd.notna(r["CO"])   else None,
                round(r["NO"],   3) if pd.notna(r["NO"])   else None,
                round(r["NO2"],  3) if pd.notna(r["NO2"])  else None,
                round(r["O3"],   3) if pd.notna(r["O3"])   else None,
                round(r["CO2"],  3) if pd.notna(r["CO2"])  else None,
                round(r["PM25"], 3) if pd.notna(r["PM25"]) else None,
            ])

        # ── assemble sensor block ────────────────────────────
        m = meta.get(sid, {})
        sensors.append({
            "id": sid,
            "name": m.get("name"),
            "lat":  m.get("lat"),
            "lon":  m.get("lon"),
            "latest": latest,
            "history": history,
        })
        print(f"[INFO] {sid}: wrote {len(history)} rows "
              f"(to {latest['timestamp']})", file=sys.stderr)

    return {
        "generated_at": datetime.now(PACIFIC).isoformat(timespec="minutes"),
        "sensors": sensors,
    }


# ── run ────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not BASE_DIR.is_dir():
        sys.exit(f"[FATAL] {BASE_DIR} is not a directory")

    result = build()
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(f"[SUCCESS] {OUTPUT_JSON} written "
          f"({len(result['sensors'])} sensors)", file=sys.stderr)
