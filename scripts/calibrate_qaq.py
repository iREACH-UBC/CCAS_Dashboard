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

def parse_filename_date(filename):
    base = os.path.basename(filename)
    date_part = base.split("-")[-1].split(".")[0]
    return datetime.strptime(date_part, "%Y-%m-%d").date()

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
    'PM10': lambda x: x,
}

# -------------------------------
# Process Each Sensor
# -------------------------------
for sensor in sensor_ids:
    pattern = os.path.join(data_folder, f"{sensor}-*.csv")
    files = glob.glob(pattern)
    if not files:
        print(f"No raw data files found for sensor {sensor}")
        continue

    file_dates = {parse_filename_date(f) for f in files}
    sorted_dates = sorted(file_dates)
    last_two_dates = sorted_dates[-2:]

    print(f"Sensor {sensor}: Processing files for dates: {last_two_dates}")
    
    dfs = []
    for d in last_two_dates:
        file_path = os.path.join(data_folder, f"{sensor}-{d}.csv")
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                dfs.append(df)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
    
    if not dfs:
        print(f"No valid data for sensor {sensor}")
        continue

    df = pd.concat(dfs, ignore_index=True)

    # Convert timestamp column and rename to DATE
    if "timestamp_local" not in df.columns:
        print(f"No timestamp_local column for {sensor}")
        continue

    try:
        df["DATE"] = pd.to_datetime(df["timestamp_local"]).dt.tz_localize(None)
    except Exception as e:
        print(f"Error parsing timestamp for {sensor}: {e}")
        continue

    past_24h = now_pst.replace(tzinfo=None) - timedelta(hours=24)
    recent_df = df[df["DATE"] >= past_24h].copy()
    if recent_df.empty:
        print(f"No data in the past 24 hours for sensor {sensor}")
        continue

    # Apply dummy calibration
    varmap = {
        "gases.co.we": "CO",
        "gases.no.we": "NO",
        "gases.no2.we": "NO2",
        "gases.o3.we": "O3",
        "met.temp": "T",
        "met.rh": "RH",
        "opc.pm1": "PM1.0",
        "opc.pm25": "PM2.5",
        "opc.pm10": "PM10",
        "gases.co2.raw": "CO2"
    }
    for raw, std in varmap.items():
        if raw in recent_df.columns:
            recent_df[std] = calibration_functions[std](pd.to_numeric(recent_df[raw], errors="coerce"))
        else:
            recent_df[std] = np.nan

    # Dummy TE column (unavailable in QAQ)
    recent_df["TE"] = np.nan

    # Reorder and ensure numeric columns
    final_columns = [
        "DATE", "TE", "CO", "NO", "NO2", "O3", "CO2", "T", "RH",
        "PM1.0", "PM2.5", "PM10"
    ]
    for col in final_columns:
        if col not in recent_df.columns:
            recent_df[col] = np.nan
        if col != "DATE":
            recent_df[col] = pd.to_numeric(recent_df[col], errors="coerce")

    recent_df = recent_df.sort_values("DATE")
    recent_df.set_index("DATE", inplace=True)

    # AQHI calculation (requires 3-hour rolling average)
    required = {"NO2", "O3", "PM2.5"}
    if not required.issubset(recent_df.columns):
        print(f"Missing required columns for AQHI in sensor {sensor}")
        continue

    rolling = recent_df[["NO2", "O3", "PM2.5"]].rolling("3h").mean()
    aqhi = (
        (10 / 10.4) * 100 * (
            (np.exp(0.000871 * rolling["NO2"]) - 1) +
            (np.exp(0.000537 * rolling["O3"]) - 1) +
            (np.exp(0.000487 * rolling["PM2.5"]) - 1)
        )
    )
    recent_df["AQHI"] = aqhi

    # Reset index, finalize column order
    recent_df.reset_index(inplace=True)
    final_columns.append("AQHI")
    recent_df = recent_df[final_columns]

    output_file = os.path.join(
        output_folder,
        f"{sensor}_calibrated_{last_two_dates[0]}_to_{last_two_dates[-1]}.csv"
    )
    recent_df.to_csv(output_file, index=False)
    print(f"✅ Calibrated QAQ data with AQHI for sensor {sensor} saved to {output_file}")
