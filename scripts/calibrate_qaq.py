import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import smtplib
from email.message import EmailMessage

# ----------------------------------------
# CONFIGURATION
# ----------------------------------------
sensor_ids = [
    "MOD-00616", "MOD-00632", "MOD-00625", "MOD-00631", "MOD-00623",
    "MOD-00628", "MOD-00620", "MOD-00627", "MOD-00630", "MOD-00624"
]

data_folder = "data"
output_folder = "calibrated_data"
os.makedirs(output_folder, exist_ok=True)

# ----------------------------------------
# TIMEZONE
# ----------------------------------------
pst = pytz.timezone("America/Los_Angeles")
now_pst = datetime.now(pst)
past_24h = now_pst.replace(tzinfo=None) - timedelta(hours=24)

# ----------------------------------------
# DUMMY CALIBRATION FUNCTIONS
# ----------------------------------------
calibration_functions = {
    "CO": lambda x: x * 1.1,
    "NO": lambda x: x * 0.9,
    "NO2": lambda x: x * 1.05,
    "O3": lambda x: x * 1.2,
    "CO2": lambda x: x,
    "T": lambda x: x + 0.5,
    "RH": lambda x: x,
    "PM1.0": lambda x: x,
    "PM2.5": lambda x: x,
    "PM10": lambda x: x,
    "TE": lambda x: "QAQ",  # Placeholder value
}

varmap = {
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

# ----------------------------------------
# PROCESS EACH SENSOR
# ----------------------------------------
for sensor in sensor_ids:
    pattern = os.path.join(data_folder, f"{sensor}-*.csv")
    files = glob.glob(pattern)
    if not files:
        print(f"No files found for {sensor}")
        continue

    latest_file = max(files, key=os.path.getmtime)
    print(f"📄 Processing {latest_file}")

    try:
        df = pd.read_csv(latest_file)
    except Exception as e:
        print(f"❌ Failed to read {latest_file}: {e}")
        continue

    # Use timestamp_local if available
    if "timestamp_local" not in df.columns:
        print(f"❌ 'timestamp_local' missing in {latest_file}")
        continue

    df = df[df["DATE"] >= past_24h].copy()
    
    if df.empty or df["DATE"].max() < past_24h:
        print(f"⚠️ Data for {sensor} is stale. Inserting -1 as fallback.")
        # Construct fallback DataFrame
        fallback_date = now_pst.replace(tzinfo=None).isoformat()
        fallback = {col: -1 for col in ["TE", "CO", "NO", "NO2", "O3", "CO2", "T", "RH", "PM1.0", "PM2.5", "PM10", "AQHI"]}
        fallback["DATE"] = fallback_date
        fallback["Top_AQHI_Contributor"] = "-1"
        df = pd.DataFrame([fallback])


    # Apply dummy calibration
    for raw, std in varmap.items():
        if raw in df.columns:
            df[std] = calibration_functions[std](pd.to_numeric(df[raw], errors="coerce"))
        else:
            df[std] = np.nan

    df["TE"] = calibration_functions["TE"](None)  # Add TE field

    # Calculate 3-hour rolling mean for AQHI
    df = df.sort_values("DATE")
    df.set_index("DATE", inplace=True)

    required = {"NO2", "O3", "PM2.5"}
    if not required.issubset(df.columns):
        print(f"❌ Missing columns for AQHI in {sensor}")
        continue

    rolling = df[["NO2", "O3", "PM2.5"]].rolling("3h").mean()

    # Calculate individual AQHI components
    NO2_component = (np.exp(0.000871 * rolling["NO2"] / 1000) - 1)
    O3_component  = (np.exp(0.000537 * rolling["O3"] / 1000) - 1)
    PM25_component = (np.exp(0.000487 * rolling["PM2.5"]) - 1)
    
    # Store for contribution analysis
    component_sum = NO2_component + O3_component + PM25_component
    df["NO2_contrib"] = NO2_component / component_sum
    df["O3_contrib"] = O3_component / component_sum
    df["PM25_contrib"] = PM25_component / component_sum
    
    # Final AQHI computation
    df["AQHI"] = (10 / 10.4) * 100 * component_sum
    
    # Identify dominant contributor
    df["Top_AQHI_Contributor"] = df[["NO2_contrib", "O3_contrib", "PM25_contrib"]].idxmax(axis=1).str.replace("_contrib", "")


    df.reset_index(inplace=True)

    # Convert DATE column to ISO8601 strings
    df["DATE"] = df["DATE"].apply(lambda dt: dt.isoformat())

    # Reorder columns
    # Calculate individual AQHI components
    NO2_component = (np.exp(0.000871 * rolling["NO2"] / 1000) - 1)
    O3_component  = (np.exp(0.000537 * rolling["O3"] / 1000) - 1)
    PM25_component = (np.exp(0.000487 * rolling["PM2.5"]) - 1)
    
    # Store for contribution analysis
    component_sum = NO2_component + O3_component + PM25_component
    df["NO2_contrib"] = NO2_component / component_sum
    df["O3_contrib"] = O3_component / component_sum
    df["PM25_contrib"] = PM25_component / component_sum
    
    # Final AQHI computation
    df["AQHI"] = (10 / 10.4) * 100 * component_sum

    # Identify dominant contributor
    df["Top_AQHI_Contributor"] = df[["NO2_contrib", "O3_contrib", "PM25_contrib"]].idxmax(axis=1).str.replace("_contrib", "")
    for col in desired_cols:
        if col not in df.columns:
            df[col] = np.nan
    df = df[desired_cols]

    # Save
    date_str = df["DATE"].apply(lambda d: d.split("T")[0]).min()  # Extract date portion

    # Create sensor-specific subfolder
    sensor_output_folder = os.path.join(output_folder, sensor)
    os.makedirs(sensor_output_folder, exist_ok=True)

    # Save to sensor folder
    output_path = os.path.join(sensor_output_folder, f"{sensor}_calibrated_{date_str}_to_{now_pst.date()}.csv")
    df.to_csv(output_path, index=False)
    print(f"✅ Saved calibrated file: {output_path}")

