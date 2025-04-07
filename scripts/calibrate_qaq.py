import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import ast

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
# Dummy Calibration Functions
# -------------------------------
calibration_functions = {
    'CO': lambda x: x * 1.1,
    'NO': lambda x: x * 0.9,
    'NO2': lambda x: x * 1.05,
    'O3': lambda x: x * 1.2,
    'CO2': lambda x: x,
    'T': lambda x: x + 0.5,
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
    files = glob.glob(pattern)
    if not files:
        print(f"No raw data files found for sensor {sensor} in {data_folder}")
        continue

    dfs = []
    for file in sorted(files)[-2:]:
        try:
            df = pd.read_csv(file)
            df["source_file"] = os.path.basename(file)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    if not dfs:
        print(f"No valid data for sensor {sensor} in recent files.")
        continue

    joined_df = pd.concat(dfs, ignore_index=True)

    try:
        joined_df['timestamp_local'] = pd.to_datetime(joined_df['timestamp_local'])
        if joined_df['timestamp_local'].dt.tz is None:
            joined_df['timestamp_local'] = joined_df['timestamp_local'].dt.tz_localize(pst_tz)
    except Exception as e:
        print(f"Error converting timestamp_local column: {e}")
        continue

    past_24h = now_pst - timedelta(hours=24)
    recent_df = joined_df[joined_df['timestamp_local'] >= past_24h].copy()
    if recent_df.empty:
        print(f"No data in the past 24 hours for sensor {sensor}")
        continue

    # Extract temperature and RH from 'met' field
    def extract_from_met(met_str, key):
        try:
            met_dict = ast.literal_eval(met_str) if pd.notnull(met_str) else {}
            return met_dict.get(key, np.nan)
        except:
            return np.nan

    recent_df['T'] = recent_df['met'].apply(lambda x: extract_from_met(x, 'temp'))
    recent_df['RH'] = recent_df['met'].apply(lambda x: extract_from_met(x, 'rh'))

    # Rename columns to uppercase for consistency
    recent_df.rename(columns={
        'co': 'CO',
        'no': 'NO',
        'no2': 'NO2',
        'o3': 'O3',
        'co2': 'CO2',
        'pm1': 'PM1.0',
        'pm10': 'PM10',
        'pm25': 'PM2.5'
    }, inplace=True)

    # Apply calibrations
    for col, func in calibration_functions.items():
        if col in recent_df.columns:
            recent_df[col] = recent_df[col].apply(func)

    # Sort by timestamp_local and set as index
    recent_df = recent_df.sort_values("timestamp_local")
    recent_df.set_index("timestamp_local", inplace=True)

    # Calculate 3-hour rolling AQHI (still using lowercase for original calc)
    required_cols = {"no2", "o3", "pm25"}
    if not required_cols.issubset(joined_df.columns.str.lower()):
        print(f"Missing required lowercase columns for AQHI in sensor {sensor}")
        continue

    try:
        rolling_means = recent_df[['NO2', 'O3', 'PM2.5']].rolling("3H").mean()
        aqhi = (
            (10 / 10.4) * 100 * (
                (np.exp(0.000871 * rolling_means["NO2"]) - 1) +
                (np.exp(0.000537 * rolling_means["O3"]) - 1) +
                (np.exp(0.000487 * rolling_means["PM2.5"]) - 1)
            )
        )
        recent_df["AQHI"] = aqhi
    except Exception as e:
        print(f"Error calculating AQHI for sensor {sensor}: {e}")
        continue

    recent_df.reset_index(inplace=True)

    # Add DATE column
    recent_df['DATE'] = recent_df['timestamp_local'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Ensure TE column exists
    recent_df["TE"] = np.nan

    # Final output column order
    output_columns = ["DATE", "TE", "CO", "NO", "NO2", "O3", "CO2", "T", "RH", "PM1.0", "PM2.5", "PM10", "AQHI"]
    for col in output_columns:
        if col not in recent_df.columns:
            recent_df[col] = np.nan

    recent_df = recent_df[output_columns]

    # Determine output filename based on last timestamp
    file_date = recent_df["DATE"].max()
    try:
        file_date = datetime.strptime(file_date, "%Y-%m-%d %H:%M:%S")
    except:
        file_date = now_pst

    output_file = os.path.join(output_folder, f"{sensor}_{file_date.strftime('%Y-%m-%d')}.csv")
    recent_df.to_csv(output_file, index=False)
    print(f"✅ Calibrated data with AQHI for sensor {sensor} saved to {output_file}")
