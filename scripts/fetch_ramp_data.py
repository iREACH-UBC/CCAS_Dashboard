import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import csv
import os
import pytz
import subprocess

sensor_id = "2035"

# Define PST time zone
pst_tz = pytz.timezone("America/Los_Angeles")

# Get the current time in UTC and convert to PST
current_time_pst = datetime.now(timezone.utc).astimezone(pst_tz)

# Automatically set date range to the past hour in PST
end_date = current_time_pst  # Current time in PST
start_date = end_date - timedelta(hours=1)  # One hour before the current time

# Print for debugging
print(f"Fetching data from {start_date.strftime('%Y-%m-%d %H:%M %Z')} to {end_date.strftime('%Y-%m-%d %H:%M %Z')}")

# Ensure the directory exists
csv_filepath = os.path.abspath("data")  # Convert to absolute path
os.makedirs(csv_filepath, exist_ok=True)

# CSV filename
csv_filename = os.path.join(csv_filepath, f"{sensor_id}_{start_date.strftime('%Y-%m-%d_%H')}_{end_date.strftime('%Y-%m-%d_%H')}.csv")

# ✅ Debugging print
print(f"💾 Saving file to: {csv_filename}")

# ✅ Always overwrite the file
with open(csv_filename, mode='w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["Timestamp", "AQI", "OtherData"])  # Default header
    writer.writerows([])  # Empty file for testing

print("✅ Data file saved (always overwrites).")

# ✅ Ensure Git recognizes the file
print("🚀 Forcing Git commit and push...")
subprocess.run(["git", "add", csv_filename], check=True)

# ✅ Add a forced change to ensure Git always sees a new commit
with open(csv_filename, "a") as f:
    f.write("\n")

subprocess.run(["git", "commit", "-m", "Forced data update"], check=True)
subprocess.run(["git", "push", "origin", "main"], check=True)
print("✅ Git push completed!")
