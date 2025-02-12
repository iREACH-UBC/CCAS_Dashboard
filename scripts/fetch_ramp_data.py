import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import csv
import os
import pytz  # For timezone handling

# Define PST time zone
pst_tz = pytz.timezone("America/Los_Angeles")

# Get the current time in UTC and convert to PST
current_time_pst = datetime.now(timezone.utc).astimezone(pst_tz)

# Automatically set date range to the past hour in PST
end_date = current_time_pst  # Current time in PST
start_date = end_date - timedelta(hours=1)  # One hour before the current time

# Print for debugging
print(f"Fetching data from {start_date.strftime('%Y-%m-%d %H:%M %Z')} to {end_date.strftime('%Y-%m-%d %H:%M %Z')}")

# RAMP data homepage
base_url = "http://18.222.146.48/RAMP/v1/raw"

# Sensor ID to pull data from
sensor_id = "2035"

# URL for the specific sensor's folder
sensor_url = f"{base_url}/{sensor_id}/data"

# Send a GET request to the sensor's 'data' folder
response = requests.get(sensor_url)

# If the request was successful, loop through and save the data
if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all links (<a> tags) that represent the data files (only works for .txt files)
    data_files = soup.find_all('a', href=True)
    print(f"🔍 Found {len(data_files)} data files.")

    # List to store the data
    all_data = []

    # Function to parse file dates and ensure they are timezone-aware
    def parse_pst_datetime(date_str):
        naive_date = datetime.strptime(date_str, '%Y-%m-%d')  # Parse as naive datetime
        return pst_tz.localize(naive_date)  # Convert to PST timezone-aware datetime

    # Loop through each file in the 'data' folder
    for file in data_files:
        file_name = file['href']  # Extract the file name

        try:
            # Extract the date part of the filename (assumes format YYYY-MM-DD-HH-XXXX.txt)
            date_part = file_name.split('-')[0:3]  # Extract 'YYYY-MM-DD'
            file_date = parse_pst_datetime('-'.join(date_part))  # Convert to PST

            # Check if file falls within the past hour
            if start_date <= file_date <= end_date:
                print(f"📂 Downloading: {file_name}")

                file_url = f"{sensor_url}/{file_name}"
                file_response = requests.get(file_url)

                if file_response.status_code == 200:
                    # Process the file content (text data)
                    data = file_response.text
                    lines = data.splitlines()

                    first_line = lines[0].split(',')
                    header = first_line[::2]  # Keys are in the even positions

                    for line in lines:
                        line_values = line.split(',')
                        if len(line_values) > 1:
                            values = line_values[1::2]  # Extract values
                            all_data.append(values)
                else:
                    print(f"❌ Failed to download: {file_url}")

        except ValueError:
            continue  # Skip files that don't match expected format

    # Write headers/data into one CSV file
    if all_data:
        csv_filepath = "data"
        os.makedirs(csv_filepath, exist_ok=True)  # Ensure directory exists
        csv_filename = f"{csv_filepath}/{sensor_id}_{start_date.strftime('%Y-%m-%d_%H')}_{end_date.strftime('%Y-%m-%d_%H')}.csv"

        print(f"💾 Saving file: {csv_filename}")
        with open(csv_filename, mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(header)
            writer.writerows(all_data)

        print("✅ Data successfully saved!")
    else:
        print("⚠️ No new data available.")
else:
    print(f"❌ Failed to connect to {sensor_url}. Status Code: {response.status_code}")
