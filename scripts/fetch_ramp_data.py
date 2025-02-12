import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import csv
import os
import pytz

# -------------------------------
# CONFIGURATION
# -------------------------------
sensor_ids = ["2035"]  # List of sensor IDs
base_url = "http://18.222.146.48/RAMP/v1/raw"
# (For each sensor, data is at: base_url/<sensor_id>/data)

# -------------------------------
# TIME RANGE: Past 6 hours in PST/PDT
# -------------------------------
pst_tz = pytz.timezone("America/Los_Angeles")
current_time_pst = datetime.now(timezone.utc).astimezone(pst_tz)
end_date = current_time_pst
start_date = end_date - timedelta(hours=6) # download the last 6 hours of data
print(f"📡 Fetching data from {start_date.strftime('%Y-%m-%d %H:%M %Z')} to {end_date.strftime('%Y-%m-%d %H:%M %Z')}")

# -------------------------------
# OUTPUT DIRECTORY: Relative path "data"
# -------------------------------
output_dir = "data"

def parse_file_datetime(file_name):
    """
    Expects file names like 'YYYY-MM-DD-HH-XXXX.txt'
    Uses the first 4 tokens to build the datetime.
    """
    parts = file_name.split('-')
    if len(parts) < 4:
        raise ValueError("File name does not match expected format")
    date_str = "-".join(parts[:4])  # e.g., '2025-02-11-18'
    dt = datetime.strptime(date_str, "%Y-%m-%d-%H")
    return pst_tz.localize(dt)

# -------------------------------
# PROCESS EACH SENSOR
# -------------------------------
for sensor_id in sensor_ids:
    sensor_url = f"{base_url}/{sensor_id}/data"
    print(f"\n🔍 Processing sensor {sensor_id} from {sensor_url}")
    
    response = requests.get(sensor_url)
    if response.status_code != 200:
        print(f"❌ Failed to connect to {sensor_url} (status code: {response.status_code})")
        continue
    
    soup = BeautifulSoup(response.text, "html.parser")
    data_files = soup.find_all("a", href=True)
    print(f"Found {len(data_files)} files for sensor {sensor_id}.")
    
    all_data = []
    header = None

    for file in data_files:
        file_name = file['href']
        try:
            file_dt = parse_file_datetime(file_name)
        except Exception:
            continue  # Skip files not matching expected format

        if start_date <= file_dt <= end_date:
            print(f"📂 Downloading file: {file_name} (date: {file_dt.strftime('%Y-%m-%d %H:%M %Z')})")
            file_url = f"{sensor_url}/{file_name}"
            file_response = requests.get(file_url)
            if file_response.status_code == 200:
                lines = file_response.text.splitlines()
                if not lines:
                    continue
                if header is None:
                    tokens = lines[0].split(',')
                    header = tokens[::2]  # Assume header keys are in even positions
                for line in lines:
                    tokens = line.split(',')
                    if len(tokens) < 2:
                        continue
                    values = tokens[1::2]
                    all_data.append(values)
            else:
                print(f"❌ Failed to download {file_url} (status code: {file_response.status_code})")
    
    if header is None:
        header = ["Field1", "Field2", "Field3"]

    # Build CSV filename
    csv_filename = os.path.join(
        output_dir,
        f"{sensor_id}_{start_date.strftime('%Y-%m-%d_%H')}_{end_date.strftime('%Y-%m-%d_%H')}.csv"
    )
    print(f"💾 Saving data to {csv_filename}")
    
    with open(csv_filename, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(all_data)
    
    print(f"✅ Data for sensor {sensor_id} saved successfully.")
