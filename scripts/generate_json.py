import json, glob, os, re, math
import pandas as pd
from datetime import datetime, timezone, timedelta

# ───────────────────────── CONFIG ─────────────────────────
CAL_DIR    = "calibrated_data"         # root with YYYY/ sub-folders
META_FILE  = "sensor_metadata.csv"     # id,lat,lon,name  (optional)
OUT_FILE   = "pollutant_data.json"
LOCAL_TZ   = timezone(timedelta(hours=-7))      # PST (fixed −07:00)
WINDOW_HRS = 24                                   # rolling window
# ──────────────────────────────────────────────────────────

FNAME_RE = re.compile(                      # 2021_<anything>_YYYY_MM_DD_to_YYYY_MM_DD.csv
    r"^(?P<id>\d+)_.*?(?P<y1>\d{4})_(?P<m1>\d{2})_(?P<d1>\d{2})_to_"
    r"(?P<y2>\d{4})_(?P<m2>\d{2})_(?P<d2>\d{2})\.csv$"
)

# ───────────── helpers ────────────────────────────────────
def iso(ts: pd.Timestamp) -> str:
    return ts.astimezone(LOCAL_TZ).isoformat(timespec="minutes")

def safe(val, ndigits=2):
    """Round number or return None for NaN / missing."""
    if pd.isna(val) or (isinstance(val, float) and math.isnan(val)):
        return None
    return round(val, ndigits)

# ───────────── metadata ───────────────────────────────────
def get_meta() -> pd.DataFrame:
    if os.path.exists(META_FILE):
        return pd.read_csv(META_FILE)

    sensor_ids = {
        m.group("id")
        for path in glob.glob(os.path.join(CAL_DIR, "**", "*.csv"), recursive=True)
        if (m := FNAME_RE.match(os.path.basename(path)))
    }
    return pd.DataFrame(
        {"id": sorted(sensor_ids), "lat": None, "lon": None, "name": None}
    )

# ───────────── file discovery ─────────────────────────────
def files_for_sensor(sensor_id: str):
    paths = glob.glob(os.path.join(CAL_DIR, "**", f"{sensor_id}_*_to_*.csv"), recursive=True)
    out = []
    for p in paths:
        m = FNAME_RE.match(os.path.basename(p))
        if m and m.group("id") == sensor_id:
            end_date = datetime(int(m["y2"]), int(m["m2"]), int(m["d2"])).date()
            out.append((end_date, p))
    return sorted(out, key=lambda t: t[0], reverse=True)

def load_last_24h(sensor_id: str) -> pd.DataFrame:
    chosen, hrs = [], 0
    for _end, path in files_for_sensor(sensor_id):
        chosen.append(path);  hrs += 24
        if hrs >= WINDOW_HRS: break
    if not chosen:
        raise FileNotFoundError(f"No calibrated files for sensor {sensor_id}")

    df = pd.concat(map(pd.read_csv, chosen)).rename(columns=str.lower)
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert(LOCAL_TZ)
    cutoff = df["date"].max() - pd.Timedelta(hours=WINDOW_HRS)
    return df[df["date"] >= cutoff]

# ───────────── main build loop ────────────────────────────
meta = get_meta()
sensors = []

for sid, lat, lon, name in meta.itertuples(index=False):
    try:
        df = load_last_24h(str(sid))
    except FileNotFoundError as e:
        print(f"  – {e}")
        continue

    base = ["date", "aqhi", "top_aqhi_contributor", "co", "no", "no2", "o3", "co2"]
    pm_cols = [c for c in ["pm1.0", "pm2.5", "pm10"] if c in df.columns]
    df = (df[base + pm_cols]
          .rename(columns={"top_aqhi_contributor": "primary",
                           "pm1.0": "pm1", "pm2.5": "pm25"}))

    last = df.iloc[-1]
    latest = {
        "timestamp": iso(last.date),
        "aqhi":      safe(last.aqhi, 1),
        "primary":   str(last.primary) if pd.notna(last.primary) else None,
        "pollutants": {col: safe(last[col], 2) for col in
                       ["co","no","no2","o3","co2",
                        *[c for c in ["pm1","pm25","pm10"] if c in df.columns]]}
    }

    history = []
    for r in df.itertuples(index=False):
        row = [iso(r.date),
               safe(r.aqhi, 2),
               str(r.primary) if pd.notna(r.primary) else None,
               safe(r.co, 3), safe(r.no, 3), safe(r.no2, 3),
               safe(r.o3, 3), safe(r.co2, 1)]
        if "pm1"  in df.columns: row.append(safe(r.pm1, 2))
        if "pm25" in df.columns: row.append(safe(r.pm25, 2))
        if "pm10" in df.columns: row.append(safe(r.pm10, 2))
        history.append(row)

    sensors.append({
        "id":   str(sid),
        "name": name,
        "lat":  lat,
        "lon":  lon,
        "latest": latest,
        "history": history
    })

# ───────────── write JSON ─────────────────────────────────
payload = {"generated_at": iso(pd.Timestamp.now(tz=LOCAL_TZ)),
           "sensors": sensors}

os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
with open(OUT_FILE, "w") as f:
    json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)

print(f"\n✅  Wrote {OUT_FILE} with {len(sensors)} sensors "
      f"({sum(len(s['history']) for s in sensors)} rows).")
