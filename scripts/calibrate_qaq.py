import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

# -------------------------------
# CONFIGURATION
# -------------------------------
sensor_ids = [
    "MOD-00616", "MOD-00632", "MOD-00625", "MOD-00631", "MOD-00623",
    "MOD-00628", "MOD-00620", "MOD-00627", "MOD-00630", "MOD-00624"
]
data_folder = "data"
output_folder = "calibrated_data"
os.makedirs(output_folder, exist_ok=True)

# -------------------------------
# Timezone
# -------------------------------
pst_tz = pytz.timezone("America/Los_Angeles")
now_pst = datetime.now(pst_tz)

# -------------------------------
# Dummy Calibration Functions
# -------------------------------
def calibrate(x, factor=1.0, offset=0.0):
    return x * factor + offset

calibration_functions = {
    'CO': lambda x: calibrate(x, 1.1),
    'NO': lambda x: calibrate(x, 0.9),
    'NO2': lambda x: calibrate(x, 1.05),
    'O3': lambda x: calibrate(x, 1.2),
    'CO2': lambda x: x,
    'T': lambda x: calibrate(x, 1.0, 0.5),
    'RH': lambda x: x,
    'PM1.0': lambda x: x,
    'PM2.5': lambda x: x,
    'PM10': lambda x: x
}

# -------------------------------
# Process Each Sensor
# -------------------------------
for sensor in sensor_ids:
    pattern = os.path.join(data_folder, f"{sensor}-*.csv")
    files = sorted(glob.glob(pattern))[-2:]

    if not files:
        print(f"No raw files for {sensor}")
        continue

    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")

    if not dfs:
        continue

    df = pd.concat(dfs, ignore_index=True)

    # Rename and extract required columns
    rename_map = {
        "timestamp_local": "DATE",
        "gases.co.we": "CO",
        "gases.no.we": "NO",
        "gases.no2.we": "NO2",
        "gases.o3.we": "O3",
        "gases.co2.raw": "CO2",
        "met.temp": "T",
        "met.rh": "RH",
        "opc.pm1": "PM1.0",
        "opc.pm25": "PM2.5",
        "opc.pm10": "PM10"
    }

    df = df.rename(columns=rename_map)
    df = df[list(rename_map.values())].copy()

    # Parse DATE column
    pst = pytz.timezone("America/Los_Angeles")
    df['DATE'] = pd.to_datetime(df['DATE']).dt.tz_localize(pst)
    df = df.sort_values('DATE')
    df = df[df['DATE'] >= now_pst - timedelta(hours=24)].copy()

    # Apply calibration
    for col, func in calibration_functions.items():
        if col in df.columns:
            df[col] = df[col].apply(func)

    # Calculate AQHI
    required = {'NO2', 'O3', 'PM2.5'}
    if required.issubset(df.columns):
        df.set_index('DATE', inplace=True)
        rolling = df[['NO2', 'O3', 'PM2.5']].rolling("3H").mean()
        aqhi = (
            (10 / 10.4) * 100 * (
                (np.exp(0.000871 * rolling['NO2']) - 1) +
                (np.exp(0.000537 * rolling['O3']) - 1) +
                (np.exp(0.000487 * rolling['PM2.5']) - 1)
            )
        )
        df['AQHI'] = aqhi
        df.reset_index(inplace=True)
    else:
        df['AQHI'] = np.nan
        df.reset_index(inplace=True)

    # Ensure correct column order
    final_cols = ["DATE", "TE", "CO", "NO", "NO2", "O3", "CO2", "T", "RH", "PM1.0", "PM2.5", "PM10", "AQHI"]
    df['TE'] = sensor  # Use sensor ID as TE identifier for now
    for col in final_cols:
        if col not in df.columns:
            df[col] = np.nan
    df = df[final_cols]

    # Output
    start = df['DATE'].min().strftime('%Y-%m-%d') if not df.empty else 'unknown'
    end = df['DATE'].max().strftime('%Y-%m-%d') if not df.empty else 'unknown'
    out_path = os.path.join(output_folder, f"{sensor}_calibrated_{start}_to_{end}.csv")
    df.to_csv(out_path, index=False)
    print(f"✅ Calibrated data saved to {out_path}")
