import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import csv
import os
import pytz

# -------------------------------
# CONFIGURATION
# -------------------------------
# List of sensor IDs (add more as needed)
sensor_ids = ["2021","2022", "2023", "2024","2026","2030","2031","2032","2033","2034","2039","2040","2041","2042","2043"]
# Base URL for RAMP data
base_url = "http://18.222.146.48/RAMP/v1/raw"

# -------------------------------
# Determine the File Date Based on PST
# -------------------------------
# Filenames are "YYYY-MM-DD-<sensor_id>.txt". Use the following rules:
# - If current PST time is before 6:00 AM, use yesterday's file.
# - If current PST time is between 6:00 AM and 9:00 PM, use today's file.
# - If current PST time is 9:00 PM or later, use tomorrow's file.
pst_tz = pytz.timezone("America/Los_Angeles")
current_time_pst = datetime.now(timezone.utc).astimezone(pst_tz)

pst = pytz.timezone("America/Los_Angeles")
now_pst = datetime.now(timezone.utc).astimezone(pst)
file_date = now_pst.date()


print(f"📡 Downloading data for date {file_date} (PST) for sensors: {sensor_ids}")

# -------------------------------
# OUTPUT DIRECTORY: Relative path "data"
# -------------------------------
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

def parse_file_data(text):
    """
    Process file content by splitting lines by comma.
    Assumes the first line contains keys (in even positions) and subsequent lines
    have the corresponding values (in odd positions).
    """
    lines = text.splitlines()
    if not lines:
        return [], []
    header_tokens = lines[0].split(',')
    header = header_tokens[::2]
    data = []
    for line in lines:
        tokens = line.split(',')
        if len(tokens) < 2:
            continue
        values = tokens[1::2]
        data.append(values)
    return header, data

# -------------------------------
# PROCESS EACH SENSOR
# -------------------------------
for sensor_id in sensor_ids:
    # Construct the filename: "YYYY-MM-DD-<sensor_id>.txt"
    file_date_str = file_date.strftime("%Y-%m-%d")
    filename = f"{file_date_str}-{sensor_id}.txt"
    sensor_url = f"{base_url}/{sensor_id}/data"
    file_url = f"{sensor_url}/{filename}"
    
    print(f"\n🔍 Processing sensor {sensor_id}")
    print(f"Downloading file from: {file_url}")
    
    response = requests.get(file_url)
    if response.status_code != 200:
        print(f"❌ Failed to download {file_url} (status code: {response.status_code})")
        continue
    
    header, all_data = parse_file_data(response.text)
    if not all_data:
        print(f"⚠️ No data found in file {filename}")
        continue

    # Construct CSV filename (overwritten each run)
    csv_filename = os.path.join(output_dir, f"{sensor_id}_{file_date_str}.csv")
    print(f"💾 Saving data to {csv_filename}")
    
    with open(csv_filename, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(all_data)
    
    print(f"✅ Data for sensor {sensor_id} saved successfully (file overwritten each run).")
