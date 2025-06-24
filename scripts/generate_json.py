#!/usr/bin/env python3
"""
build_sensor_json.py  –  v2025-06-23
────────────────────────────────────
Create dashboard-ready sensors.json from
calibrated_data/<id>/<id>[_]calibrated_YYYY_MM_DD_to_YYYY_MM_DD.csv
"""

from __future__ import annotations
import argparse, json, re, sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz


# ── configuration ──────────────────────────────────────────────
PACIFIC = pytz.timezone("America/Vancouver")

# columns you actually need in the JSON
KEEP = ["DATE", "CO", "NO", "NO2", "O3", "CO2", "PM2.5",     # pollutants
        "AQHI", "Top_AQHI_Contributor"]                       # meta

HISTORY_ORDER = ["DATE", "AQHI", "Top_AQHI_Contributor",
                 "CO", "NO", "NO2", "O3", "CO2", "PM2.5"]

# regex for “…calibrated_YYYY_MM_DD_to_YYYY_MM_DD.csv”
PAT = re.compile(
    r"^(?P<id>\d+)_?calibrated_(\d{4})_(\d{2})_(\d{2})_to_(?P<y>\d{4})_(?P<m>\d{2})_(?P<d>\d{2})\.csv$"
)


# ── helpers ────────────────────────────────────────────────────
def newest_csv(sensor_dir: Path, sid: str) -> Path | None:
    """Return the file with the most-recent '_to_' date for this sensor."""
    best_date = None
    best_path = None

    for p in sensor_dir.glob(f"{sid}*calibrated_*.csv"):
        m = PAT.match(p.name)
        if not m:
            continue
        date = datetime(int(m["y"]), int(m["m"]), int(m["d"]))
        if best_date is None or date > best_date:
            best_date, best_path = date, p

    if not best_path:
        print(f"[WARN] {sid}: no calibrated csvs found", file=sys.stderr)
    else:
        print(f"[INFO] {sid}: picked {best_path.name}", file=sys.stderr)
    return best_path


def read_meta(meta_csv: Path) -> dict[str, dict]:
    if not meta_csv.exists():
        return {}
    df = pd.read_csv(meta_csv, dtype={"id": str})
    return {str(r.id): r.to_dict() for _, r in df.iterrows()}


def iso_local(ts) -> str | None:
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.astimezone(PACIFIC).isoformat(timespec="minutes")


# ── main builder ───────────────────────────────────────────────
def build(base_dir: Path, meta_path: Path, hours: int | None):
    meta = read_meta(meta_path)
    sensors = []

    for sensor_dir in sorted(base_dir.iterdir()):
        if not sensor_dir.is_dir():
            continue
        sid = sensor_dir.name
        csv_file = newest_csv(sensor_dir, sid)
        if csv_file is None:
            continue

        try:
            df = pd.read_csv(csv_file, usecols=lambda c: c in KEEP)
        except ValueError as e:
            print(f"[ERROR] {csv_file}: {e}", file=sys.stderr)
            continue

        df.rename(columns={"Top_AQHI_Contributor": "PRIMARY",
                           "PM2.5": "PM25"}, inplace=True)

        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        df = df.dropna(subset=["DATE"]).sort_values("DATE")

        # last N hours?
        if hours and hours > 0:
            df = df[df["DATE"] >= df["DATE"].max() - pd.Timedelta(hours=hours)]

        if df.empty:
            print(f"[WARN] {sid}: dataframe empty", file=sys.stderr)
            continue

        last = df.iloc[-1]

        latest = {
            "timestamp": iso_local(last["DATE"]),
            "aqhi": round(last["AQHI"], 1) if "AQHI" in df else None,
            "primary": last.get("PRIMARY"),
            "pollutants": {
                "co": round(last.get("CO"), 3)   if "CO"   in df else None,
                "no": round(last.get("NO"), 3)   if "NO"   in df else None,
                "no2": round(last.get("NO2"), 3) if "NO2"  in df else None,
                "o3": round(last.get("O3"), 3)   if "O3"   in df else None,
                "co2": round(last.get("CO2"), 3) if "CO2"  in df else None,
                "pm25": round(last.get("PM25"), 3) if "PM25" in df else None,
            },
        }

        history = []
        for _, r in df[HISTORY_ORDER].iterrows():
            history.append([
                iso_local(r["DATE"]),
                round(r["AQHI"], 1) if not pd.isna(r["AQHI"]) else None,
                r["PRIMARY"] if isinstance(r["PRIMARY"], str) else None,
                round(r["CO"], 3)   if "CO"   in df else None,
                round(r["NO"], 3)   if "NO"   in df else None,
                round(r["NO2"], 3)  if "NO2"  in df else None,
                round(r["O3"], 3)   if "O3"   in df else None,
                round(r["CO2"], 3)  if "CO2"  in df else None,
                round(r["PM25"], 3) if "PM25" in df else None,
            ])

        m = meta.get(sid, {})
        sensors.append({
            "id": sid,
            "name": m.get("name"),
            "lat":  m.get("lat"),
            "lon":  m.get("lon"),
            "latest": latest,
            "history": history
        })

        print(f"[INFO] {sid}: wrote {len(history)} rows (to {latest['timestamp']})",
              file=sys.stderr)

    return {
        "generated_at": datetime.now(PACIFIC).isoformat(timespec="minutes"),
        "sensors": sensors,
    }


# ── CLI ─────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Generate sensors.json for dashboard")
    ap.add_argument("--base-dir", default="calibrated_data",
                    help="top-level folder that holds one sub-dir per sensor")
    ap.add_argument("--meta", default="sensor_metadata.csv",
                    help="optional csv with id,name,lat,lon")
    ap.add_argument("--hours", type=int, default=24,
                    help="history window (0 = entire file)")
    ap.add_argument("--out", default="sensors.json",
                    help="output file")
    args = ap.parse_args()

    base = Path(args.base_dir)
    if not base.is_dir():
        sys.exit(f"[FATAL] {base} is not a directory")

    data = build(base, Path(args.meta), None if args.hours <= 0 else args.hours)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    print(f"[SUCCESS] {out} written ({len(data['sensors'])} sensors)",
          file=sys.stderr)


if __name__ == "__main__":
    main()
