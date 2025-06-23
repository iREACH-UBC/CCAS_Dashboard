#!/usr/bin/env python3
"""
Build a 24-hour JSON snapshot for every calibrated sensor file.

• Walks `calibrated_data/**/<sensor_id>_…_to_….csv`
• Guarantees the history covers the exact last 24 h, even if that
  means reading several daily files.
• Uses one explicit column-map so renaming can never silently fail.
• Writes `pollutant_data.json` with the structure the dashboard expects.
"""

# ───────────── standard lib ───────────────────────────────
import json, glob, os, re, math
from datetime import datetime, timezone, timedelta

# ───────────── third-party ────────────────────────────────
import pandas as pd

# ───────────────────────── CONFIG ─────────────────────────
CAL_DIR    = "calibrated_data"          # root with YYYY/ sub-folders
META_FILE  = "sensor_metadata.csv"      # id,lat,lon,name (optional)
OUT_FILE   = "pollutant_data.json"
LOCAL_TZ   = timezone(timedelta(hours=-7))   # fixed PST (UTC-07:00)
WINDOW_HRS = 24                               # rolling window length
# ──────────────────────────────────────────────────────────

# CSV file-name pattern → extract sensor id and end-date
FNAME_RE = re.compile(
    r"^(?P<id>\d+)_.*?(?P<y1>\d{4})_(?P<m1>\d{2})_(?P<d1>\d{2})_to_"
    r"(?P<y2>\d{4})_(?P<m2>\d{2})_(?P<d2>\d{2})\.csv$"
)

# Column map (lower-case originals → JSON names)
COL_MAP = {
    "date":                 "date",
    "aqhi":                 "aqhi",
    "top_aqhi_contributor": "primary",
    "co":                   "co",
    "no":                   "no",
    "no2":                  "no2",
    "o3":                   "o3",
    "co2":                  "co2",
    "pm1.0":                "pm1",
    "pm2.5":                "pm25",
    "pm10":                 "pm10",
}

# ───────────── helpers ────────────────────────────────────
def iso(ts: pd.Timestamp) -> str:
    """Local ISO-8601 string with minute precision."""
    return ts.astimezone(LOCAL_TZ).isoformat(timespec="minutes")

def safe(val, ndigits=2):
    """Round floats; keep None for NaN / missing so JSON shows null."""
    if pd.isna(val) or (isinstance(val, float) and math.isnan(val)):
        return None
    return round(val, ndigits)


# ───────────── metadata (fallback if csv missing) ─────────
def get_meta() -> pd.DataFrame:
    if os.path.exists(META_FILE):
        return pd.read_csv(META_FILE)

    sensor_ids = {
        m.group("id")
        for p in glob.glob(os.path.join(CAL_DIR, "**", "*.csv"), recursive=True)
        if (m := FNAME_RE.match(os.path.basename(p)))
    }
    return pd.DataFrame(
        {"id": sorted(sensor_ids), "lat": None, "lon": None, "name": None}
    )

# ───────────── file discovery ─────────────────────────────
def files_for_sensor(sensor_id: str):
    """Return [(end_date, path), …] newest → oldest for one sensor."""
    paths = glob.glob(
        os.path.join(CAL_DIR, "**", f"{sensor_id}_*_to_*.csv"), recursive=True
    )
    out = []
    for p in paths:
        if m := FNAME_RE.match(os.path.basename(p)):
            if m["id"] != sensor_id:
                continue
            end_date = datetime(int(m["y2"]), int(m["m2"]), int(m["d2"])).date()
            out.append((end_date, p))
    return sorted(out, key=lambda t: t[0], reverse=True)


# ───────────── read just enough files to cover 24 h ───────
def load_last_24h(sensor_id: str) -> pd.DataFrame:
    paths = files_for_sensor(sensor_id)
    if not paths:
        raise FileNotFoundError(f"No calibrated files for sensor {sensor_id}")

    cutoff = pd.Timestamp.now(tz=LOCAL_TZ) - pd.Timedelta(hours=WINDOW_HRS)

    parts = []
    for _end, path in paths:                      # newest → older
        df_part = (
            pd.read_csv(path)
              .rename(columns=lambda c: c.strip().lower())   # trim + lower
        )
        df_part["date"] = (
            pd.to_datetime(df_part["date"], utc=True)
              .dt.tz_convert(LOCAL_TZ)
        )
        parts.append(df_part)
        if df_part["date"].min() <= cutoff:       # enough history reached
            break

    df = pd.concat(parts, ignore_index=True)

    # apply JSON-friendly names + keep only mapped cols present
    df = (
        df.rename(columns=COL_MAP)
          .loc[:, [c for c in COL_MAP.values() if c in df.columns]]
    )

    return df[df["date"] >= cutoff].sort_values("date", ignore_index=True)


# ───────────── main build loop ────────────────────────────
def main():
    meta = get_meta()
    sensors_out = []

    for sid, lat, lon, name in meta.itertuples(index=False):
        try:
            df = load_last_24h(str(sid))
        except FileNotFoundError as e:
            print(f"  – {e}")
            continue

        pm_cols = [c for c in ("pm1", "pm25", "pm10") if c in df.columns]

        last = df.iloc[-1]
        prim = last.get("primary")                # may be absent

        latest = {
            "timestamp": iso(last.date),
            "aqhi":      safe(last.aqhi, 1),
            "primary":   str(prim) if pd.notna(prim) else None,
            "pollutants": {
                col: safe(last[col], 2)
                for col in ["co", "no", "no2", "o3", "co2", *pm_cols]
            },
        }

        history = []
        for r in df.itertuples(index=False):
            primary_val = getattr(r, "primary", None)
            row = [
                iso(r.date),
                safe(r.aqhi, 2),
                str(primary_val) if pd.notna(primary_val) else None,
                safe(r.co, 3),
                safe(r.no, 3),
                safe(r.no2, 3),
                safe(r.o3, 3),
                safe(r.co2, 1),
            ]
            for c in pm_cols:                     # add PM if present
                row.append(safe(getattr(r, c), 2))
            history.append(row)

        sensors_out.append(
            {
                "id": str(sid),
                "name": name,
                "lat": lat,
                "lon": lon,
                "latest": latest,
                "history": history,
            }
        )

    # ───────────── write JSON ───────────────────────────────
    payload = {
        "generated_at": iso(pd.Timestamp.now(tz=LOCAL_TZ)),
        "sensors": sensors_out,
    }

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)

    total_rows = sum(len(s["history"]) for s in sensors_out)
    print(f"\n✅  Wrote {OUT_FILE} with {len(sensors_out)} sensors "
          f"({total_rows} rows).")

# ───────────── run as script ──────────────────────────────
if __name__ == "__main__":
    main()
