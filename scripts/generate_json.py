#!/usr/bin/env python3
# generate_json.py –  2025-08-04 (patched by HDR)
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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytz

# ───────────────────────────────────────────────────────────────
# EDIT ME with sensors
# ───────────────────────────────────────────────────────────────
SENSORS_WANTED: set[str] | None = {
    "2021", "2022","2033", "2040", "2042", "2043", "2024", "2030", "2039", "2025", "2031",
    "MOD-00632", "MOD-00616", "MOD-00625", "MOD-00631", "MOD-00623",
    "2023", "MOD-00628", "MOD-00627" # set to None ⇒ auto‑discover all sub‑folders
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
    "AQHI", "Top_AQHI_contributor",
]

KEEP_COLS_LC = {c.lower() for c in KEEP_COLS}
CANON_MAP    = {c.lower(): c for c in KEEP_COLS}

# Accept purely‑numeric IDs (“2040”) **and** alphanumeric ones with dashes
# (“MOD‑00616”, “AIR‑123” …)
FILE_RE = re.compile(
    r"""
    ^(?P<id>[\w-]+)              # sensor id  (letters / digits / _ / -)
    _calibrated_                 # literal
    \d{4}_\d{2}_\d{2}_to_        # “YYYY_MM_DD_to_”
    (?P<y>\d{4})_(?P<m>\d{2})_(?P<d>\d{2})  # end‑date “YYYY_MM_DD”
    \.csv$
    """,
    re.VERBOSE,
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


# -------------------- NEGATIVE CONC HANDLING ------------------------|


def safe_round(val, ndigits: int, *, allow_negative: bool = False):
    """
    Round numeric values safely:
    - return None if NaN / non-numeric
    - return None if value < 0 and allow_negative=False
    """
    if pd.isna(val):
        return None
    try:
        v = float(val)
    except (TypeError, ValueError):
        return None
    if (not allow_negative) and v < 0:
        return None
    return round(v, ndigits)

# -------------------- OFFLINE SENSOR HANDLING -----------------------|


def make_offline_history(now_ts: datetime, history_hours: int) -> list[list[Any]]:
    """
    Build a synthetic history timeline for an offline sensor:
    - One point per hour over the past `history_hours`
    - All values are None (JSON null)
    """
    if history_hours <= 0:
        return []

    history: list[list[Any]] = []
    # Generate timestamps from (now - history_hours + 1 h) up to now
    for i in range(history_hours):
        ts = now_ts - timedelta(hours=history_hours - 1 - i)
        history.append([
            to_pacific_iso(ts),  # timestamp
            None,                # AQHI
            None,                # PRIMARY (top contributor)
            None,                # CO
            None,                # NO
            None,                # NO2
            None,                # O3
            None,                # CO2
            None,                # PM25
        ])
    return history


OFFLINE_PRIMARY   = "Not Available"
OFFLINE_POLLUTANT = "N/A"


def make_offline_sensor(
    sid: str,
    meta: dict[str, dict],
    alerts: dict[str, bool],
    *,
    now_ts: datetime,
    history_hours: int,
    reason: str | None = None,
) -> dict:
    """Build a sensor block for an offline / no-data sensor."""
    m = meta.get(sid, {}) or {}

    region = _clean_str(m.get("region")) or None
    active_alert = alerts.get(region, False)

    if reason:
        print(f"[WARN] {sid}: marked offline – {reason}", file=sys.stderr)

    offline_history = make_offline_history(now_ts, history_hours)

    return {
        "id":            sid,
        "name":          _clean_str(m.get("name")),
        "sensor_number": _clean_str(m.get("sensor_number")),
        "region":        region,
        "lat":           float(m.get("lat")) if m.get("lat") else None,
        "lon":           float(m.get("lon")) if m.get("lon") else None,
        "active_alert":  active_alert,
        "latest": {
            "timestamp": None,
            "aqhi":      None,
            "primary":   OFFLINE_PRIMARY,
            "pollutants": {
                "co":   OFFLINE_POLLUTANT,
                "no":   OFFLINE_POLLUTANT,
                "no2":  OFFLINE_POLLUTANT,
                "o3":   OFFLINE_POLLUTANT,
                "co2":  OFFLINE_POLLUTANT,
                "pm25": OFFLINE_POLLUTANT,
            },
        },
        "history": offline_history,
    }


# ── core builder ───────────────────────────────────────────────

def build() -> dict:
    meta   = read_meta(META_CSV)
    alerts = read_advisories(ADVISORY_JSON)
    sensors_js: list[dict] = []
    
    now_ts = datetime.now(PACIFIC).replace(second=0, microsecond=0)

    # Decide which sensor IDs we’re going to emit
    if SENSORS_WANTED is not None:
        sensor_ids = sorted(SENSORS_WANTED)
    else:
        sensor_ids = sorted(
            p.name for p in BASE_DIR.iterdir() if p.is_dir()
        )

    for sid in sensor_ids:
        sensor_dir = BASE_DIR / sid

        # If there’s not even a directory, still emit an offline sensor
        if not sensor_dir.is_dir():
            sensors_js.append(
                make_offline_sensor(
                    sid, meta, alerts,
                    now_ts=now_ts,
                    history_hours=HISTORY_HOURS,
                    reason="no directory under calibrated_data"
                )
            )
            continue

        try:
            csv_path = newest_csv(sensor_dir, sid)
            if not csv_path:
                # No calibrated CSVs found – offline
                sensors_js.append(
                    make_offline_sensor(
                        sid, meta, alerts,
                        now_ts=now_ts,
                        history_hours=HISTORY_HOURS,
                        reason="no calibrated CSVs found"
                    )
                )
                continue

            try:
                # read only wanted columns, case-insensitive
                df = pd.read_csv(csv_path, usecols=lambda c: c.lower() in KEEP_COLS_LC)
            except ValueError as e:
                print(f"[ERROR] {csv_path}: {e}", file=sys.stderr)
                sensors_js.append(
                    make_offline_sensor(
                        sid, meta, alerts,
                        now_ts=now_ts,
                        history_hours=HISTORY_HOURS,
                        reason="CSV read failed"
                    )
                )
                continue

            # normalise headers
            df.rename(columns=lambda c: CANON_MAP.get(c.lower(), c), inplace=True)
            df.rename(columns={"Top_AQHI_contributor": "PRIMARY", "PM2.5": "PM25"},
                      inplace=True)

            for col in ("PRIMARY", "PM25"):
                if col not in df.columns:
                    df[col] = None

            # timestamp handling
            if "DATE" not in df.columns:
                sensors_js.append(
                    make_offline_sensor(
                        sid, meta, alerts,
                        now_ts=now_ts,
                        history_hours=HISTORY_HOURS,
                        reason="DATE column missing"
                    )
                )
                continue

            df["DATE"] = pd.to_datetime(df["DATE"])

            # full_df for “do we have any data at all?”
            full_df = df.copy()

            # apply rolling window
            if HISTORY_HOURS > 0 and not df.empty:
                cutoff = df["DATE"].max() - pd.Timedelta(hours=HISTORY_HOURS)
                df = df[df["DATE"] >= cutoff]

            # If no rows survive the time window, mark as offline (no stale values)
            if df.empty:
                if full_df.empty:
                    reason = "no rows in CSV"
                else:
                    reason = f"no rows in last {HISTORY_HOURS}h (stale data suppressed)"
                sensors_js.append(
                    make_offline_sensor(
                        sid, meta, alerts,
                        now_ts=now_ts,
                        history_hours=HISTORY_HOURS,
                        reason=reason
                    )
                )
                continue

            # ── latest record ─────────────────────────────────
            last = df.iloc[-1]
            print(
                f"[DEBUG] {sid}: Final NO value: {last.get('NO')} → "
                f"rounded: {round(float(last['NO']), 3) if pd.notna(last['NO']) else None}",
                file=sys.stderr,
            )

            latest = {
                "timestamp": to_pacific_iso(last["DATE"]),
                # If you also want to suppress negative AQHI, leave allow_negative=False
                "aqhi": safe_round(last["AQHI"], 1, allow_negative=False) if "AQHI" in df.columns else None,
                "primary": last["PRIMARY"] if isinstance(last["PRIMARY"], str) else None,
                "pollutants": {
                    "co":   safe_round(last["CO"],   3) if "CO"   in df.columns else None,
                    "no":   safe_round(last["NO"],   3) if "NO"   in df.columns else None,
                    "no2":  safe_round(last["NO2"],  3) if "NO2"  in df.columns else None,
                    "o3":   safe_round(last["O3"],   3) if "O3"   in df.columns else None,
                    "co2":  safe_round(last["CO2"],  3) if "CO2"  in df.columns else None,
                    "pm25": safe_round(last["PM25"], 3) if "PM25" in df.columns else None,
                },
            }


            # ── history list ─────────────────────────────────
            history = [
                [
                    to_pacific_iso(r["DATE"]),
                    safe_round(r["AQHI"], 1, allow_negative=False) if "AQHI" in df.columns else None,
                    r["PRIMARY"] if isinstance(r["PRIMARY"], str) else None,
                    safe_round(r["CO"],   2) if "CO"   in df.columns else None,
                    safe_round(r["NO"],   2) if "NO"   in df.columns else None,
                    safe_round(r["NO2"],  2) if "NO2"  in df.columns else None,
                    safe_round(r["O3"],   2) if "O3"   in df.columns else None,
                    safe_round(r["CO2"],  2) if "CO2"  in df.columns else None,
                    safe_round(r["PM25"], 2) if "PM25" in df.columns else None,
                ]
                for _, r in df.iterrows()
            ]

            # ── assemble sensor block ─────────────────────────
            m = meta.get(sid, {}) or {}
            region       = _clean_str(m.get("region")) or None
            active_alert = alerts.get(region, False)

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

        except Exception as e:
            # Last-resort safety: never crash the whole JSON build for one bad sensor
            print(f"[ERROR] {sid}: unexpected failure {e!r} → marking offline", file=sys.stderr)
            sensors_js.append(
                make_offline_sensor(
                    sid, meta, alerts,
                    now_ts=now_ts,
                    history_hours=HISTORY_HOURS,
                    reason="unexpected exception during build()"
                )
            )
            continue

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
