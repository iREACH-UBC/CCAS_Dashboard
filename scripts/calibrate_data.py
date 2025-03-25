import os
import glob
import pandas as pd
from datetime import datetime, timedelta
import pytz

# -------------------------------
# CONFIGURATION
# -------------------------------
# List of sensor IDs (add more as needed)
sensor_ids = ["2021","2022", "2023", "2024","2026","2030","2031","2032","2033","2034","2039","2040","2041","2042","2043"]
# Folder where raw data files are stored.
# Files are named: <sensor_id>_<YYYY>-<MM>-<DD>.csv
data_folder = "data"
# Folder to save calibrated output
output_folder = "calibrated_data"
os.makedirs(output_folder, exist_ok=True)

# -------------------------------
# Define Time Range
# -------------------------------
pst_tz = pytz.timezone("America/Los_Angeles")
now_pst = datetime.now(pst_tz)

def parse_filename_date(filename, sensor_id):
    # Expects filename in format: <sensor_id>_<YYYY>-<MM>-<DD>.csv
    base = os.path.basename(filename)
    parts = base.split('_')
    if len(parts) != 2:
        raise ValueError("Filename does not match expected format")
    date_part = parts[1].split('.')[0]
    return datetime.strptime(date_part, "%Y-%m-%d").date()

# -------------------------------
# Dummy Calibration Functions
# -------------------------------
calibration_functions = {
    'CO': lambda x: x * 1.1,      # Increase CO by 10%
    'NO': lambda x: x * 0.9,      # Decrease NO by 10%
    'NO2': lambda x: x * 1.05,    # Increase NO2 by 5%
    'O3': lambda x: x * 1.2,      # Increase O3 by 20%
    'CO2': lambda x: x,          # No change for CO2
    'T': lambda x: x + 0.5,       # Increase temperature by 0.5 degrees
    'RH': lambda x: x,           # No change for RH
    'PM1.0': lambda x: x,        # No change for PM1.0
    'PM2.5': lambda x: x,        # No change for PM2.5
    'PM10': lambda x: x,         # No change for PM10
    'WD': lambda x: x,           # No change for wind direction
    'WS': lambda x: x * 1.1      # Increase wind speed by 10%
}

def calculate_aqi(row):
    # Dummy calculation: AQI = 0.8 * PM2.5 + 0.2 * O3
    try:
        aqi = 0.8 * row['PM2.5'] + 0.2 * row['O3']
    except Exception:
        aqi = None
    return aqi

# -------------------------------
# Process Each Sensor
# -------------------------------
for sensor in sensor_ids:
    pattern = os.path.join(data_folder, f"{sensor}_*.csv")
    files = glob.glob(pattern)
    if not files:
        print(f"No raw data files found for sensor {sensor} in {data_folder}")
        continue
    
    # Parse dates from filenames and get distinct dates
    file_dates = {parse_filename_date(f, sensor) for f in files}
    sorted_dates = sorted(file_dates)
    if len(sorted_dates) < 1:
        print(f"Not enough data files for sensor {sensor}")
        continue
    # Take the last two dates (if available; if only one exists, use it)
    last_two_dates = sorted_dates[-2:]
    print(f"Sensor {sensor}: Processing files for dates: {last_two_dates}")
    
    dfs = []
    for d in last_two_dates:
        file_pattern = os.path.join(data_folder, f"{sensor}_{d.strftime('%Y-%m-%d')}.csv")
        matched = glob.glob(file_pattern)
        if matched:
            try:
                df = pd.read_csv(matched[0])
                dfs.append(df)
            except Exception as e:
                print(f"Error reading {matched[0]}: {e}")
    if not dfs:
        print(f"No valid data for sensor {sensor} on the last two dates.")
        continue
    
    # Join the two days of data
    joined_df = pd.concat(dfs, ignore_index=True)
    
    # Convert DATE column to datetime and localize to PST if needed.
    try:
        joined_df['DATE'] = pd.to_datetime(joined_df['DATE'])
        if joined_df['DATE'].dt.tz is None:
            joined_df['DATE'] = joined_df['DATE'].dt.tz_localize(pst_tz)
    except Exception as e:
        print(f"Error converting DATE column: {e}")
        continue
    
    # Filter to only include data from the past 24 hours (based on current PST)
    past_24h = now_pst - timedelta(hours=24)
    recent_df = joined_df[joined_df['DATE'] >= past_24h].copy()
    if recent_df.empty:
        print(f"No data in the past 24 hours for sensor {sensor}")
        continue
    
    # Apply calibration functions to the specified columns.
    for col, func in calibration_functions.items():
        if col in recent_df.columns:
            recent_df[col] = recent_df[col].apply(func)
    
    # Calculate AQI for each row based on the calibrated data
    recent_df['AQI'] = recent_df.apply(calculate_aqi, axis=1)
    
    print(f"Columns before dropping for sensor {sensor}:")
    print(recent_df.columns.tolist())
    
    # Drop unwanted columns before saving
    columns_to_remove = ["WD", "WS", "PWR", "BATT", "CHRG", "RUN", "SD", "RAW",
                         "108", "24", "0", "-29", "6", "0.1", "0.00000", "0.00000.1",
                         "0.2", "348142736", "118", "19", "-5", "-28", "302302278"]
    recent_df.drop(columns=columns_to_remove, errors='ignore', inplace=True)
    
    # Save the calibrated data and AQI to a new file in the "calibrated_data" folder.
    output_file = os.path.join(output_folder,
                               f"{sensor}_calibrated_{last_two_dates[0].strftime('%Y-%m-%d')}_to_{last_two_dates[-1].strftime('%Y-%m-%d')}.csv")
    recent_df.to_csv(output_file, index=False)
    print(f"Calibrated data for sensor {sensor} saved to {output_file}")
