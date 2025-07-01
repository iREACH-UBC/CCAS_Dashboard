#!/usr/bin/env python3
# build_sensor_json.py  –  2025-06-30 (patched)
# ---------------------------------------------------------------------------
# * Walk calibrated_data/<sensor_id>/
# * For each ID in SENSORS_WANTED, grab the newest “…_calibrated_…csv”
# * Keep the last HISTORY_HOURS of data (local time)
# * Emit a dashboard‑ready JSON to OUTPUT_JSON
#
# Extra features (2025‑06‑30):
# • sensor_metadata.csv may now contain “sensor_number” and “region”
# • Each sensor entry gains an "active_alert" bool taken from AQAdvisories.json
# ---------------------------------------------------------------------------

from __future__ import annotations
import json, re, sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytz

# ───────────────────────────────────────────────────────────────
# EDIT ME
# ───────────────────────────────────────────────────────────────
SENSORS_WANTED: set[str] | None = {
    "2021", "2022", "2040",   # set to None ⇒ auto‑discover all sub‑folders
}

BASE_DIR       = Path("calibrated_data")          # per‑sensor sub‑folders
META_CSV       = Path("sensor_metadata.csv")
ADVISORY_JSON  = Path("AQAdvisories.json")
HISTORY_HOURS  = 24                                 # 0 → keep whole file
OUTPUT_JSON    = Path("pollutant_data.json")

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


def _clean_str(x: Any) -> str | None:
    """Helper: strip spaces & BOM; return None for empty strings."""
    if isinstance(x, str):
        s = x.strip().lstrip("\ufeff")
        return s or None
    return None


def read_meta(meta_csv: Path) -> dict[str, dict]:
    """Return dict keyed by sensor id → metadata row (as dict).

    Robust to:
    • leading spaces after commas
    • stray BOMs
    • IDs stored as numbers (e.g. 2021.0)
    • extra unquoted commas in the ‘region’ field (they get re‑joined)
    """
    if not meta_csv.exists():
        print(f"[WARN] metadata file {meta_csv} missing → metadata disabled", file=sys.stderr)
        return {}

    # 1️⃣ read very permissively
    df = pd.read_csv(
        meta_csv,
        dtype=str,
        skipinitialspace=True,   # trim leading spaces after commas
        engine="python",        # handles ragged rows
        keep_default_na=False,   # keep empty strings, avoid NaN -> None later
    )

    # 2️⃣ normalise strings & strip “.0” if Excel saved IDs as floats
    df = df.applymap(_clean_str)
    df["id"] = df["id"].str.replace(r"\.0$", "", regex=True)

    # 3️⃣ if region got split by unquoted commas, stitch the leftovers back
    expected = ["id", "lat", "lon", "name", "sensor_number", "region"]
    if len(df.columns) > len(expected):
        region_parts = df.columns[len(expected)-1:]
        df["region"] = (
            df[region_parts]
              .astype(str)
              .apply(lambda row: ", ".join([_clean_str(c) for c in row if _clean_str(c)]), axis=1)
        )
        df = df[expected]  # keep only the canonical columns

    # 4️⃣ drop duplicate IDs (keep first)
    df = df[~df["id"].duplicated(keep="first")]

    return {str(r.id): r.to_dict() for _, r in df.iterrows()}


def read_advisories(advisory_json: Path) -> dict[str, bool]:
    """Return dict keyed by region name → ActiveAlert bool."""
    if not advisory_json.exists():
        print(f"[WARN] advisory file {advisory_json} missing → alerts disabled", file=sys.stderr)
        return {}
    data = json.loads(advisory_json.read_text(encoding="utf-8"))
    return {a.get("Region", ""): bool(a.get("ActiveAlert")) for a in data.get("Advisories", [])}


def to_pacific_iso(ts) -> str | None:
    """Return ISO‑8601 string in America/Vancouver; ts already Pacific."""
    return None if pd.isna(ts) else ts.isoformat(timespec="minutes")


# ── core builder ───────────────────────────────────────────────

def build() -> dict:
    meta       = read_meta(META_CSV)
    alerts     = read_advisories(ADVISORY_JSON)
    sensors_js = []

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

        # ── rename / sanity‑check columns ──────────────────────
        df.rename(columns={"Top_AQHI_Contributor": "PRIMARY", "PM2.5": "PM25"}, inplace=True)
        for col in ("PRIMARY", "PM25"):
            if col not in df.columns:
                df[col] = None

        # ── timestamp handling ────────────────────────────────
        df["DATE"] = (
            df["DATE"].astype(str)
                       .str.replace(r"Z$", "", regex=True)       # 1️⃣ strip stray Z
        )
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")  # 2️⃣ parse
        df["DATE"] = df["DATE"].map(lambda t: pd.NaT if pd.isna(t) else PACIFIC.localize(t))  # 3️⃣ localise

        df = df.dropna(subset=["DATE"]).sort_values("DATE")

        # ── apply rolling window ──────────────────────────────
        if HISTORY_HOURS > 0 and not df.empty:
            cutoff = df["DATE"].max() - pd.Timedelta(hours=HISTORY_HOURS)
            df = df[df["DATE"] >= cutoff]

        if df.empty:
            print(f"[WARN] {sid}: dataframe empty", file=sys.stderr)
            continue

        # ── latest record ─────────────────────────────────────
        last = df.iloc[-1]
        latest = {
            "timestamp": to_pacific_iso(last["DATE"]),
            "aqhi": round(float(last["AQHI"]), 1) if pd.notna(last["AQHI"]) else None,
            "primary": last["PRIMARY"] if isinstance(last["PRIMARY"], str) else None,
            "pollutants": {
                "co":   round(float(last["CO"]),   3) if pd.notna(last["CO"])   else None,
                "no":   round(float(last["NO"]),   3) if pd.notna(last["NO"])   else None,
                "no2":  round(float(last["NO2"]),  3) if pd.notna(last["NO2"])  else None,
                "o3":   round(float(last["O3"]),   3) if pd.notna(last["O3"])   else None,
                "co2":  round(float(last["CO2"]),  3) if pd.notna(last["CO2"])  else None,
                "pm25": round(float(last["PM25"]), 3) if pd.notna(last["PM25"]) else None,
            },
        }

        # ── history list ──────────────────────────────────────
        history = [
            [
                to_pacific_iso(r["DATE"]),
                round(float(r["AQHI"]), 1) if pd.notna(r["AQHI"]) else None,
                r["PRIMARY"] if isinstance(r["PRIMARY"], str) else None,
                round(float(r["CO"]),   2) if pd.notna(r["CO"])   else None,
                round(float(r["NO"]),   2) if pd.notna(r["NO"])   else None,
                round(float(r["NO2"]),  2) if pd.notna(r["NO2"])  else None,
                round(float(r["O3"]),   2) if pd.notna(r["O3"])   else None,
                round(float(r["CO2"]),  2) if pd.notna(r["CO2"])  else None,
                round(float(r["PM25"]), 2) if pd.notna(r["PM25"]) else None,
            ]
            for _, r in df.iterrows()
        ]

        # ── assemble sensor block ─────────────────────────────
        m = meta.get(sid, {})
        region        = _clean_str(m.get("region")) or None
        active_alert  = alerts.get(region, False)

        sensors_js.append({
            "id":            sid,
            "name":          _clean_str(m.get("name")),
            "sensor_number": _clean_str(m.get("sensor_number")),
            "region":        region,
            "lat":           float(m.get("lat")) if m.get("lat") else None,
            "lon":           float(m.get("lon")) if m.get("lon") else None,
            "active_alert":  active_alert,
            "latest":        latest,
            "history":       history,
        })

        print(
            f"[INFO] {sid}: wrote {len(history)} rows (to {latest['timestamp']}), "
            f"alert={active_alert}",
            file=sys.stderr,
        )

    return {
        "generated_at": datetime.now(PACIFIC).isoformat(timespec="minutes"),
        "sensors":      sensors_js,
    }


# ── run ────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not BASE_DIR.is_dir():
        sys.exit(f"[FATAL] {BASE_DIR} is not a directory")

    result = build()
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")

    print(
        f"[SUCCESS] {OUTPUT_JSON} written ({len(result['sensors'])} sensors)",
        file=sys.stderr,
    )
