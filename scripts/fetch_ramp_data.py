print(f"Fetching data from {sensor_url} for {start_str} to {end_str}")
response = requests.get(sensor_url)

if response.status_code == 200:
    print("✅ Successfully connected to the RAMP data folder.")
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all <a> links
    data_files = soup.find_all('a', href=True)
    print(f"🔍 Found {len(data_files)} data files.")

    all_data = []
    
    for file in data_files:
        file_name = file['href']
        try:
            date_part = file_name.split('-')[0:4]
            file_datetime = datetime.strptime('-'.join(date_part), '%Y-%m-%d-%H')

            if start_date <= file_datetime <= end_date:
                print(f"📂 Downloading: {file_name}")

                file_url = f"{sensor_url}/{file_name}"
                file_response = requests.get(file_url)

                if file_response.status_code == 200:
                    data = file_response.text
                    lines = data.splitlines()
                    
                    first_line = lines[0].split(',')
                    header = first_line[::2]

                    for line in lines:
                        line_values = line.split(',')
                        if len(line_values) > 1:
                            values = line_values[1::2]
                            all_data.append(values)
                else:
                    print(f"❌ Failed to download: {file_url}")

        except ValueError:
            continue

    if all_data:
        csv_filepath = "data"
        os.makedirs(csv_filepath, exist_ok=True)
        csv_filename = f"{csv_filepath}/{sensor_id}_{start_str}_{end_str}.csv"
        
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
