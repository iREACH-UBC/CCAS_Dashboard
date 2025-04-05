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
# Define Time Range
# -------------------------------
pst_tz = pytz.timezone("America/Los_Angeles")
now_pst = datetime.now(pst_tz)

# -------------------------------
# Dummy Calibration Functions (to be replaced with R.obj or similar)
# -------------------------------
calibration_functions = {
    'co': lambda x: x * 1.1,
    'no': lambda x: x * 0.9,
    'no2': lambda x: x * 1.05,
    'o3': lambda x: x * 1.2,
    'co2': lambda x: x,
    'temp_box': lambda x: x + 0.5,
    'rh_manifold': lambda x: x,
    'pm1': lambda x: x,
    'pm25': lambda x: x,
    'pm10': lambda x: x,
    'wind_dir': lambda x: x,
    'wind_speed': lambda x: x * 1.1
}

# -------------------------------
# Process Each Sensor
# -------------------------------
for sensor in sensor_ids:
    pattern = os.path.join(data_folder, f"*{sensor}.csv")
    files = glob.glob(pattern)
    if not files:
        print(f"No raw data files found for sensor {sensor} in {data_folder}")
        continue

    latest_file = max(files, key=os.path.getmtime)
    print(f"Sensor {sensor}: Processing {latest_file}")

    try:
        df = pd.read_csv(latest_file)
        if "timestamp_local" in df.columns:
            df['timestamp_local'] = pd.to_datetime(df['timestamp_local'])
            if df['timestamp_local'].dt.tz is None:
                df['timestamp_local'] = df['timestamp_local'].dt.tz_localize(pst_tz)
            df = df.set_index('timestamp_local')
        else:
            print(f"No 'timestamp_local' column in {latest_file}. Skipping.")
            continue
    except Exception as e:
        print(f"Error reading {latest_file}: {e}")
        continue

    past_24h = now_pst - timedelta(hours=24)
    recent_df = df[df.index >= past_24h].copy()
    if recent_df.empty:
        print(f"No data in the past 24 hours for sensor {sensor}")
        continue

    for col, func in calibration_functions.items():
        if col in recent_df.columns:
            recent_df[col] = recent_df[col].apply(func)

    # Calculate dummy AQHI using NO2, O3, PM2.5
    try:
        rolling_means = recent_df[["no2", "o3", "pm25"]].rolling("3H").mean()
        aqhi = (
            (10 / 10.4) * 100 * (
                (np.exp(0.000871 * rolling_means["no2"]) - 1) +
                (np.exp(0.000537 * rolling_means["o3"]) - 1) +
                (np.exp(0.000487 * rolling_means["pm25"]) - 1)
            )
        )
        recent_df["AQHI"] = aqhi
    except Exception as e:
        print(f"Error calculating AQHI for {sensor}: {e}")

    # Reset index and export
    recent_df.reset_index(inplace=True)

    output_file = os.path.join(
        output_folder,
        f"{sensor}_calibrated_{now_pst.strftime('%Y-%m-%d')}.csv"
    )
    recent_df.to_csv(output_file, index=False)
    print(f"Calibrated data with AQHI for sensor {sensor} saved to {output_file}")
