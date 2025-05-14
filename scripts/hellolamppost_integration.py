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

# Paths (relative to repo root)
input_folder = "calibrated_data"
output_file = "HelloLamppostData.json"

# AQHI label mapping function
def get_aqhi_label(value):
    if value == "N/A":
        return "no data"
    elif value <= 3:
        return "low"
    elif value <= 6:
        return "moderate"
    elif value <= 10:
        return "high"
    else:
        return "very high"

output_json = []
past_24h = datetime.now() - timedelta(hours=24)

for sensor_id in sensor_ids:
    sensor_folder = os.path.join(input_folder, sensor_id)
    pattern = os.path.join(sensor_folder, f"{sensor_id}_calibrated_*.csv")
    files = sorted(glob.glob(pattern), reverse=True)

    # Default values
    value = "N/A"
    contributor = "N/A"
    contrib_value = "N/A"

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
                    contrib_value = (
                        float(round(latest[contributor], 2))
                        if contributor in latest and pd.notnull(latest[contributor])
                        else "N/A"
                    )
        except Exception as e:
            print(f"Failed to process {sensor_id}: {e}")

    label = get_aqhi_label(value)

    output_json.append({
        sensor_id: {
            "label": label,
            "value": value if value == "N/A" else int(round(value)),
            "top_contributor": contributor,
            "contribution": contrib_value
        }
    })

# Write JSON output
with open(output_file, "w") as f:
    json.dump(output_json, f, indent=4)

print(f"Written to {output_file}")
