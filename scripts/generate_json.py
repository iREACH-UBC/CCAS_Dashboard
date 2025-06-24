#!/usr/bin/env python3
# build_sensor_json.py  –  v2025-06-23 (hard-coded sensor list)
# -------------------------------------------------------------

from __future__ import annotations
import json, re, sys
from datetime import datetime
from pathlib import Path

import pandas as pd      # pip install pandas
import pytz              # pip install pytz

# ───────────────────────────────────────────────────────────────
# 0️⃣  EDIT HERE  –  sensor IDs you care about
#     None   → all sub-folders in calibrated_data/
#     set()  → only those IDs
# ───────────────────────────────────────────────────────────────
SENSORS_WANTED = {
    "2021", "2040", "2022"        # ← update as needed, or set to None
}

# top-level folder; change if your layout differs
BASE_DIR = Path("calibrated_data")

# optional meta CSV with columns  id,name,lat,lon
META_CSV = Path("sensor_metadata.csv")

# history window in hours; 0 = keep whole file
HISTORY_HOURS = 24

# output file
OUTPUT_JSON = Path("pollutant_data.json")

# ───────────────────────────────────────────────────────────────
PACIFIC = pytz.timezone("America/Vancouver")

KEEP_COLS = [
    "DATE", "CO", "NO", "NO2", "O3", "CO2", "PM2.5",
    "AQHI", "Top_AQHI_Contributor",
]

PAT = re.compile(
    r"^(?P<id>\d+)_?calibrated_\d{4}_\d{2}_\d{2}_to_"
    r"(?P<y>\d{4})_(?P<m>\d{2})_(?P<d>\d{2})\.csv$"
)


# ── helpers ────────────────────────────────────────────────────
def newest_csv(sensor_dir: Path, sid: str) -> Path | None:
    best = None
    best_date = None
    for p in sensor_dir.glob(f"{sid}*calibrated_*.csv"):
        m = PAT.match(p.name)
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


def read_meta(meta_path: Path) -> dict[str, dict]:
    if not meta_path.exists():
        return {}
    df = pd.read_csv(meta_path, dtype={"id": str})
    return {str(r.id): r.to_dict() for _, r in df.iterrows()}


def iso_local(ts):
    """
    Convert pandas Timestamp → ISO-8601 string in America/Vancouver.
    If ts is naïve, treat it as already Pacific (NO extra shift).
    """
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = PACIFIC.localize(ts)          # ← change is here
    else:
        ts = ts.astimezone(PACIFIC)
    return ts.isoformat(timespec="minutes")   # 2025-06-23T14:15-07:00



# ── main builder ───────────────────────────────────────────────
def build():
    meta = read_meta(META_CSV)
    sensors = []

    for sensor_dir in sorted(BASE_DIR.iterdir()):
        if not sensor_dir.is_dir():
            continue
        sid = sensor_dir.name
        if SENSORS_WANTED is not None and sid not in SENSORS_WANTED:
            continue

        csv_path = newest_csv(sensor_dir, sid)
        if csv_path is None:
            continue

        try:
            df = pd.read_csv(csv_path, usecols=lambda c: c in KEEP_COLS)
        except ValueError as e:
            print(f"[ERROR] {csv_path}: {e}", file=sys.stderr)
            continue

        df.rename(columns={"Top_AQHI_Contributor": "PRIMARY",
                           "PM2.5": "PM25"}, inplace=True)
        for col in ("PRIMARY", "PM25"):
            if col not in df.columns:
                df[col] = None

        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        df = df.dropna(subset=["DATE"]).sort_values("DATE")

        if HISTORY_HOURS > 0:
            df = df[df["DATE"] >= df["DATE"].max() -
                    pd.Timedelta(hours=HISTORY_HOURS)]
        if df.empty:
            print(f"[WARN] {sid}: dataframe empty", file=sys.stderr)
            continue

        last = df.iloc[-1]
        latest = {
            "timestamp": iso_local(last["DATE"]),
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

        history = []
        for _, r in df.iterrows():
            history.append([
                iso_local(r["DATE"]),
                round(r["AQHI"], 1) if pd.notna(r["AQHI"]) else None,
                r["PRIMARY"] if isinstance(r["PRIMARY"], str) else None,
                round(r["CO"],   3) if pd.notna(r["CO"])   else None,
                round(r["NO"],   3) if pd.notna(r["NO"])   else None,
                round(r["NO2"],  3) if pd.notna(r["NO2"])  else None,
                round(r["O3"],   3) if pd.notna(r["O3"])   else None,
                round(r["CO2"],  3) if pd.notna(r["CO2"])  else None,
                round(r["PM25"], 3) if pd.notna(r["PM25"]) else None,
            ])

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

    data = build()
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    print(f"[SUCCESS] {OUTPUT_JSON} written ({len(data['sensors'])} sensors)",
          file=sys.stderr)
