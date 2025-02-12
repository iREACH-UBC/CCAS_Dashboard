import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import csv
import os
import pytz

# -------------------------------
# CONFIGURATION
# -------------------------------
# List of sensor IDs (you can add more sensor IDs)
sensor_ids = ["2035"]
# Base URL for RAMP data
base_url = "http://18.222.146.48/RAMP/v1/raw"

# -------------------------------
# TIME RANGE: Past 6 hours in PST/PDT
# -------------------------------
pst_tz = pytz.timezone("America/Los_Angeles")
current_time_pst = datetime.now(timezone.utc).astimezone(pst_tz)
end_date = current_time_pst
start_date = end_date - timedelta(hours=6)

print(f"📡 Fetching data from {start_date.strftime('%Y-%m-%d %H:%M %Z')} to {end_date.strftime('%Y-%m-%d %H:%M %Z')}")

# -------------------------------
# OUTPUT DIRECTORY: Relative path "data"
# -------------------------------
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

def parse_file_datetime(file_name):
    """
    Expects file names like 'YYYY-MM-DD-HHmm.txt'.
    For example: '2025-02-12-2035.txt' will be parsed as February 12, 2025, 20:35 PST.
    """
    # Remove the file extension
    base = os.path.splitext(file_name)[0]  # "2025-02-12-2035"
    parts = base.split('-')
    if len(parts) != 4:
        raise ValueError("File name does not match expected format")
    # Build a string in the format "YYYY-MM-DD-HHmm"
    dt_str = f"{parts[0]}-{parts[1]}-{parts[2]}-{parts[3]}"
    dt = datetime.strptime(dt_str, "%Y-%m-%d-%H%M")
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
    files_downloaded = 0

    for file in data_files:
        file_name = file['href']
        try:
            file_dt = parse_file_datetime(file_name)
        except Exception as e:
            # Skip files that don't match expected format
            continue

        if start_date <= file_dt <= end_date:
            print(f"📂 Downloading file: {file_name} (date: {file_dt.strftime('%Y-%m-%d %H:%M %Z')})")
            file_url = f"{sensor_url}/{file_name}"
            file_response = requests.get(file_url)
            if file_response.status_code == 200:
                lines = file_response.text.splitlines()
                if not lines:
                    continue
                # Use the first matching file to set the header (assume keys are in even positions)
                if header is None:
                    tokens = lines[0].split(',')
                    header = tokens[::2]
                for line in lines:
                    tokens = line.split(',')
                    if len(tokens) < 2:
                        continue
                    values = tokens[1::2]
                    all_data.append(values)
                files_downloaded += 1
            else:
                print(f"❌ Failed to download {file_url} (status code: {file_response.status_code})")
    
    if files_downloaded == 0:
        print(f"⚠️ No files found in the past 6 hours for sensor {sensor_id}")
    else:
        print(f"✅ Downloaded {files_downloaded} file(s) for sensor {sensor_id}.")

    if header is None:
        header = ["Field1", "Field2", "Field3"]

    # Construct the CSV filename (it will be overwritten on each run)
    csv_filename = os.path.join(
        output_dir,
        f"{sensor_id}_{start_date.strftime('%Y-%m-%d_%H')}_{end_date.strftime('%Y-%m-%d_%H')}.csv"
    )
    print(f"💾 Saving data to {csv_filename}")
    
    with open(csv_filename, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(all_data)
    
    print(f"✅ Data for sensor {sensor_id} saved successfully (file overwritten each run).")
