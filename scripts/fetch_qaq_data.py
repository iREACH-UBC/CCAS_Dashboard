import os
import pytz
from datetime import datetime, timedelta, timezone
import pandas as pd

# -------------------------------
# CONFIGURATION
# -------------------------------
# Hardcoded list of QuantAQ sensor serial numbers
sn = ["MOD-00616", "MOD-00632", "MOD-00625", "MOD-00631", "MOD-00623",
      "MOD-00628", "MOD-00620", "MOD-00627", "MOD-00630", "MOD-00624"]

# -------------------------------
# DETERMINE FILE DATE BASED ON PST
# -------------------------------
pst_tz = pytz.timezone("America/Los_Angeles")
current_time_pst = datetime.now(timezone.utc).astimezone(pst_tz)

if current_time_pst.hour < 6:
    file_date = current_time_pst.date() - timedelta(days=1)
elif current_time_pst.hour >= 21:
    file_date = current_time_pst.date() + timedelta(days=1)
else:
    file_date = current_time_pst.date()

print(f"📡 Downloading QuantAQ data for date {file_date} (PST) for sensors: {sn}")

# -------------------------------
# OUTPUT DIRECTORY
# -------------------------------
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

# -------------------------------
# FETCH AND SAVE DATA PER SENSOR
# -------------------------------
for sensor in sn:
    try:
        print(f"\n🔍 Fetching data for sensor {sensor}")
        
        # Fetch data using QuantAQ client
        df = to_dataframe(client.data.bydate(sn=sensor, date=str(file_date)))

        if df.empty:
            print(f"⚠️ No data for sensor {sensor} on {file_date}")
            continue

        # Format CSV filename to match fetch_ramp: "YYYY-MM-DD-<sensor_id>.csv"
        csv_filename = os.path.join(output_dir, f"{file_date}-{sensor}.csv")
        df.to_csv(csv_filename, index=False)
        print(f"✅ Data saved to {csv_filename}")

    except Exception as e:
        print(f"❌ Failed to fetch or save data for {sensor}: {e}")
