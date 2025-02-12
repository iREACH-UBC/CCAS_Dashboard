import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import csv
import os
import pytz
import subprocess

# -------------------------------
# Configuration
# -------------------------------
sensor_id = "2035"
base_url = "http://18.222.146.48/RAMP/v1/raw"  # RAMP data homepage
sensor_url = f"{base_url}/{sensor_id}/data"

# -------------------------------
# Set up Time Range in PST (past 6 hours)
# -------------------------------
pst_tz = pytz.timezone("America/Los_Angeles")
current_time_pst = datetime.now(timezone.utc).astimezone(pst_tz)
end_date = current_time_pst
start_date = end_date - timedelta(hours=6)

print(f"📡 Fetching data for sensor {sensor_id} from {start_date.strftime('%Y-%m-%d %H:%M %Z')} to {end_date.strftime('%Y-%m-%d %H:%M %Z')}")

# -------------------------------
# Fetch and Parse Data Files
# -------------------------------
response = requests.get(sensor_url)
if response.status_code != 200:
    print(f"❌ Failed to connect to {sensor_url}, status code: {response.status_code}")
    exit(1)

soup = BeautifulSoup(response.text, 'html.parser')
data_files = soup.find_all('a', href=True)
print(f"🔍 Found {len(data_files)} files on the server.")

def parse_file_datetime(file_name):
    """
    Expects file names like 'YYYY-MM-DD-HH-XXXX.txt'
    Uses the first 4 tokens to get the datetime.
    """
    parts = file_name.split('-')
    if len(parts) < 4:
        raise ValueError("File name does not match expected format")
    date_str = "-".join(parts[:4])  # e.g., '2025-02-11-18'
    dt = datetime.strptime(date_str, '%Y-%m-%d-%H')
    return pst_tz.localize(dt)

all_data = []
header = None

for file in data_files:
    file_name = file['href']
    try:
        file_dt = parse_file_datetime(file_name)
    except Exception as e:
        # Skip files that don't match the expected format
        continue

    if start_date <= file_dt <= end_date:
        print(f"📂 Downloading file: {file_name} (date: {file_dt.strftime('%Y-%m-%d %H:%M %Z')})")
        file_url = f"{sensor_url}/{file_name}"
        file_response = requests.get(file_url)
        if file_response.status_code == 200:
            data_text = file_response.text
            lines = data_text.splitlines()
            if not lines:
                continue
            # Assume the first file provides the header by splitting by comma and taking every other token
            if header is None:
                tokens = lines[0].split(',')
                header = tokens[::2]
            for line in lines:
                tokens = line.split(',')
                if len(tokens) < 2:
                    continue
                values = tokens[1::2]
                all_data.append(values)
        else:
            print(f"❌ Failed to download {file_url}")

if header is None:
    header = ["Field1", "Field2", "Field3"]

# -------------------------------
# Save Data to CSV File
# -------------------------------
csv_dir = os.path.abspath("data")
os.makedirs(csv_dir, exist_ok=True)
csv_filename = os.path.join(csv_dir, f"{sensor_id}_{start_date.strftime('%Y-%m-%d_%H')}_{end_date.strftime('%Y-%m-%d_%H')}.csv")
print(f"💾 Saving data to {csv_filename}")

with open(csv_filename, mode='w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(header)
    writer.writerows(all_data)

print("✅ Data file saved.")

# -------------------------------
# Force Git Commit and Push
# -------------------------------
print("🚀 Forcing Git commit and push...")

# Set Git author identity (in case it's not set)
subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"], check=True)
subprocess.run(["git", "c
