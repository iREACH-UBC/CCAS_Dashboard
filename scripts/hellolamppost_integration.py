import os
import glob
import pandas as pd
import json

# List your sensor IDs here
sensor_ids = ["MOD-00630"]  # Replace with actual IDs

# Paths (relative to repo root)
input_folder = "hugo/calibrated_data"
output_file = "hugo/HelloLamppostData.json"

# AQHI label mapping function
def get_aqhi_label(value):
    if value <= 3:
        return "low"
    elif value <= 6:
        return "moderate"
    elif value <= 10:
        return "high"
    else:
        return "very high"

output_json = []

for sensor_id in sensor_ids:
    pattern = os.path.join(input_folder, f"{sensor_id}_calibrated_*.csv")
    files = sorted(glob.glob(pattern), reverse=True)

    if not files:
        print(f"No files found for {sensor_id}")
        continue

    latest_file = files[0]
    try:
        df = pd.read_csv(latest_file, parse_dates=["DATE"])
        if df.empty:
            print(f"Empty file for {sensor_id}")
            continue

        latest = df.sort_values("DATE").iloc[-1]
        value = float(latest["AQHI"])
        label = get_aqhi_label(value)

        output_json.append({
            sensor_id: {
                "label": label,
                "value": round(value, 2)
            }
        })

    except Exception as e:
        print(f"Failed to process {sensor_id}: {e}")

# Write JSON output
with open(output_file, "w") as f:
    json.dump(output_json, f, indent=4)

print(f"Written to {output_file}")
