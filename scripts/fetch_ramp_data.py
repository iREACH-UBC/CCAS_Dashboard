import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import csv
import os

# RAMP data homepage
base_url = "http://18.222.146.48/RAMP/v1/raw"

# Sensor ID to pull data from
sensor_id = "2019"

# Dynamically calculate the past hour
end_date = datetime.utcnow()
start_date = end_date - timedelta(hours=12)

# Format for filenames
start_str = start_date.strftime('%Y-%m-%d-%H')
end_str = end_date.strftime('%Y-%m-%d-%H')

# URL for the specific sensor's folder
sensor_url = f"{base_url}/{sensor_id}/data"

# Send a GET request to the sensor's 'data' folder
response = requests.get(sensor_url)

# If the request was successful, loop through and save the data
if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all links (<a> tags) that represent the data files (only works for .txt files)
    data_files = soup.find_all('a', href=True)

    # List to store the data
    all_data = []
    
    # Loop through each file in the 'data' folder
    for file in data_files:
        file_name = file['href']  # Extract the file name
        
        try:
            # Extract the date part of the filename (assumes format YYYY-MM-DD-HH-XXXX.txt)
            date_part = file_name.split('-')[0:4]  # Extract YYYY-MM-DD-HH
            file_datetime = datetime.strptime('-'.join(date_part), '%Y-%m-%d-%H')

            # Check if file falls within the past hour
            if start_date <= file_datetime <= end_date:
                print(f"Found data for {file_datetime.strftime('%Y-%m-%d %H:%M')}: {file_name}")

                # Construct the full URL to download the file
                file_url = f"{sensor_url}/{file_name}"

                # Send a GET request to fetch the file's content
                file_response = requests.get(file_url)

                if file_response.status_code == 200:
                    # Process the file content (text data)
                    data = file_response.text
                    lines = data.splitlines()
                    
                    # Extract headers and values from the first line
                    first_line = lines[0].split(',')
                    header = first_line[::2]  # Keys are in the even positions
                    
                    # Append only values (not headers) to the data list
                    for line in lines:
                        line_values = line.split(',')
                        if len(line_values) > 1:
                            values = line_values[1::2]  # Extract values
                            all_data.append(values)
                else:
                    print(f"Failed to retrieve file: {file_url}")

        except ValueError:
            continue  # Skip files that don't match expected format

    # Write headers/data into one CSV file
    if all_data:
        csv_filepath = "data"
        os.makedirs(csv_filepath, exist_ok=True)  # Ensure directory exists
        csv_filename = f"{csv_filepath}/{sensor_id}_{start_str}_{end_str}.csv"
        
        with open(csv_filename, mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(header)  # Write header row
            writer.writerows(all_data)  # Write all data 
        
        print(f"Data saved to {csv_filename}")
    else:
        print("No data found for the past hour.")

