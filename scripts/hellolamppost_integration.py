import os
import glob
import pandas as pd
import json
from datetime import datetime, timedelta

# List your sensor IDs here
sensor_ids = [
    "2021","2022", "2023", "2024","2026","2030","2031",
    "2032","2033","2034","2039","2040","2041","2042","2043",
    "MOD-00616", "MOD-00632", "MOD-00625", "MOD-00631", "MOD-00623",
    "MOD-00628", "MOD-00620", "MOD-00627", "MOD-00630", "MOD-00624"
]

sensor_names = {
  "2021" : "Location 1",
  "2022" : "Location 2", 
  "2023" : "Location 3", 
  "2024" : "Location 4",
  "2026" : "Location 5",
  "2030" : "Location 6",
  "2031" : "Location 7",
  "2032" : "Location 8",
  "2033" : "Location 9",
  "2034" : "Location 10",
  "2039" : "Location 11",
  "2040" : "Location 12",
  "2041" : "Location 13",
  "2042" : "Location 14",
  "2043" : "Location 15",
  "MOD-00616" : "Location 16", 
  "MOD-00632" : "Location 17", 
  "MOD-00625" : "Location 18", 
  "MOD-00631" : "Location 19", 
  "MOD-00623" : "Location 20",
  "MOD-00628" : "Location 21", 
  "MOD-00620" : "Location 22", 
  "MOD-00627" : "Location 23", 
  "MOD-00630" : "Location 24", 
  "MOD-00624" : "Location 25"
}


# Paths
input_folder = "calibrated_data"
output_file = "HelloLamppostData.json"

# AQHI label mapping function
def get_aqhi_label(value):
    if value == "N/A":
        return "no data"
    elif value <= 3:
        return "Low health risk"
    elif value <= 6:
        return "Moderate health risk"
    elif value <= 10:
        return "High health risk"
    else:
        return "Very high health risk"

output_json = []
past_24h = datetime.now() - timedelta(hours=24)

for sensor_id in sensor_ids:
    sensor_folder = os.path.join(input_folder, sensor_id)
    pattern = os.path.join(sensor_folder, f"{sensor_id}_calibrated_*.csv")
    files = sorted(glob.glob(pattern), reverse=True)

    # Default values
    value = "N/A"
    contributor = "N/A"
    pollutant_conc = "N/A"

    if files:
        latest_file = files[0]
        try:
            df = pd.read_csv(latest_file, parse_dates=["DATE"])
            if not df.empty:
                df = df.sort_values("DATE")
                latest = df.iloc[-1]

                latest_date = pd.to_datetime(latest["DATE"])
                aqhi_val = latest.get("AQHI", "N/A")

                if (
                    latest_date >= past_24h
                    and pd.notnull(aqhi_val)
                    and float(aqhi_val) != -1
                ):
                    value = float(aqhi_val)
                    contributor = str(latest.get("Top_AQHI_Contributor", "N/A"))

                    # Try to fetch pollutant concentration value
                    if contributor in latest and pd.notnull(latest[contributor]):
                        pollutant_conc = float(round(latest[contributor], 2))
        except Exception as e:
            print(f"Failed to process {sensor_id}: {e}")

    label = get_aqhi_label(value)

    output_json.append({
        sensor_names[sensor_id]: {
            "label": label,
            "value": value if value == "N/A" else int(round(value)),
            "top_contributor": contributor,
            "pollutant_concentration": pollutant_conc
        }
    })

# Write JSON output
with open(output_file, "w") as f:
    json.dump(output_json, f, indent=4)

print(f"Written to {output_file}")
