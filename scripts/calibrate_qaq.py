#!/usr/bin/env python3
"""
Calibrate QuantAQ “MOD‑” sensors, mimicking the RAMP pipeline.

Usage (CI):  python calibrate_qaq.py MOD‑00616
Usage (local): python calibrate_qaq.py           # runs all fallback IDs
"""
import sys, os, glob, pytz, tempfile
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

# ── CONFIG / INPUTS ────────────────────────────────────────────
fallback_ids = ["MOD-00616", "MOD-00632", "MOD-00625", "MOD-00631",
                "MOD-00623", "MOD-00628", "MOD-00620",
                "MOD-00627", "MOD-00630", "MOD-00624"]

sensor_ids = sys.argv[1:] or fallback_ids
data_folder = Path("data")
output_root = Path("calibrated_data")
output_root.mkdir(exist_ok=True)

model_path = Path(os.environ.get("CAL_MODEL_PATH", ""))
if not model_path.is_file():
    raise RuntimeError("CAL_MODEL_PATH env‑var missing or invalid.")

# ── R bridge (rpy2) ────────────────────────────────────────────
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
pandas2ri.activate()

# Load the same helper that RAMPs use
ro.r['source']("scripts/apply_caps_calibration.R")
apply_caps = ro.globalenv['apply_caps_calibration']

# ── TIME WINDOW ────────────────────────────────────────────────
pst = pytz.timezone("America/Los_Angeles")
now_pst = datetime.now(pst)
past_24h = now_pst - timedelta(hours=24)

# ── HELPERS ────────────────────────────────────────────────────
def find_recent_files(sensor):
    """Return the two most‑recent CSVs (today + yesterday) for `sensor`."""
    pattern = data_folder / f"{sensor}-*.csv"
    files = sorted(glob.glob(str(pattern)), key=os.path.getmtime, reverse=True)
    return files[:2]

def calibrate_file(sensor, raw_path):
    """Call the R helper, get a calibrated pandas‑df back."""
    calib_r = apply_caps(sensor_id=sensor,
                         data_file=str(raw_path),
                         model_path=str(model_path))
    return pandas2ri.ri2py(calib_r)

def apply_aqhi(df):
    """Add AQHI & contributor columns (identical to the RAMP logic)."""
    roll3  = df[["NO2", "O3", "PM2.5"]].rolling("3h").mean()
    roll1  = df[["PM2.5"]].rolling("1h").mean()

    NO2_c  = np.exp(0.000871 * roll3["NO2"]/1000) - 1
    O3_c   = np.exp(0.000537 * roll3["O3"]/1000)  - 1
    PM25_c = np.exp(0.000487 * roll3["PM2.5"])    - 1
    comp_sum = NO2_c + O3_c + PM25_c

    raw_aqhi = (10/10.4) * 100 * comp_sum
    aqhi     = np.maximum(raw_aqhi.round(),
                          np.ceil(roll1["PM2.5"] / 10)).astype(int)

    df["AQHI"]          = aqhi
    df["NO2_contrib"]   = NO2_c  / comp_sum
    df["O3_contrib"]    = O3_c   / comp_sum
    df["PM25_contrib"]  = PM25_c / comp_sum
    df["Top_AQHI_Contributor"] = (df[["NO2_contrib","O3_contrib","PM25_contrib"]]
                                   .idxmax(axis=1)
                                   .str.replace("_contrib",""))
    return df

# ── MAIN LOOP ───────────────────────────────────────────────────
for sid in sensor_ids:
    print(f"── {sid} ───────────")

    recent_files = find_recent_files(sid)
    if not recent_files:
        print("  ⚠️  no raw files – skipping.")
        continue

    # Calibrate just enough files to span 24 h
    parts, earliest = [], now_pst
    for f in recent_files:
        print(f"   • calibrating {Path(f).name}")
        parts.append(calibrate_file(sid, f))
        earliest = min(earliest, parts[-1]["date"].min())
        if earliest <= past_24h:
            break

    if not parts:
        print("  ⚠️  calibration returned 0 rows – skipping.")
        continue

    df = pd.concat(parts, ignore_index=True)

    # Harmonise column names & types exactly like the RAMP script
    df["DATE"] = pd.to_datetime(df["date"]).dt.tz_convert(pst) + pd.Timedelta(hours=2)
    df = (df
          .rename(columns={"PM2_5":"PM2.5"})
          .drop(columns=["date"])
          .sort_values("DATE")
          .query("DATE >= @past_24h"))

    df = apply_aqhi(df)

    desired = ["DATE","TE","CO","NO","NO2","O3","CO2","T","RH",
               "PM1.0","PM2.5","PM10","AQHI","Top_AQHI_Contributor"]
    for col in desired:
        if col not in df:
            df[col] = np.nan
    df = df[desired]

    # ── write ---------------------------------------------------
    out_dir = output_root / sid
    out_dir.mkdir(exist_ok=True)
    first_day = df["DATE"].min().strftime("%Y_%m_%d")
    last_day  = now_pst.strftime("%Y_%m_%d")
    out_path  = out_dir / f"{sid}_calibrated_{first_day}_to_{last_day}.csv"
    df.to_csv(out_path, index=False)
    print(f"  ✅ wrote {out_path.relative_to(Path.cwd())}")
