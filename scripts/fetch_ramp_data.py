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
end_date = current_time_pst  # Current time in PST
start_date = end_date - timedelta(hours=6)  # Six hours before the current time

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
    Expects file names like 'YYYY-MM-DD-HH-XXXX.txt'.
    Uses the first 4 tokens (YYYY, MM, DD, HH) to build the datetime.
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
        continue  # Skip files that don't match expected format

    if start_date <= file_dt <= end_date:
        print(f"📂 Downloading file: {file_name} (date: {file_dt.strftime('%Y-%m-%d %H:%M %Z')})")
        file_url = f"{sensor_url}/{file_name}"
        file_response = requests.get(file_url)
        if file_response.status_code == 200:
     
