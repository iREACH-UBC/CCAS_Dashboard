import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import pytz
from requests.auth import HTTPBasicAuth

# ----------------------------------------
# CONFIGURATION
# ----------------------------------------
SENSOR_IDS = [
    "MOD-00616", "MOD-00632", "MOD-00625", "MOD-00631", "MOD-00623",
    "MOD-00628", "MOD-00620", "MOD-00627", "MOD-00630", "MOD-00624"
]

API_KEY = os.getenv("QUANTAQ_API_KEY")
if not API_KEY:
    raise EnvironmentError("❌ QUANTAQ_API_KEY environment variable not set.")

OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ----------------------------------------
# DETERMINE FILE DATE (Based on PST)
# ----------------------------------------
pst = pytz.timezone("America/Los_Angeles")
now_pst = datetime.now(timezone.utc).astimezone(pst)

if now_pst.hour < 6:
    file_date = now_pst.date() - timedelta(days=1)
elif now_pst.hour >= 21:
    file_date = now_pst.date() + timedelta(days=1)
else:
    file_date = now_pst.date()

date_str = str(file_date)
print(f"Fetching QuantAQ data for {date_str}")

# ----------------------------------------
# FETCH FINAL DATA BY DATE
# ----------------------------------------
for sn in SENSOR_IDS:
    try:
        print(f"Fetching data for {sn}...")

        url = f"https://api.quant-aq.com/v1/devices/{sn}/data-by-date/{date_str}/"
        response = requests.get(
            url,
            auth=HTTPBasicAuth(API_KEY, ""),
            headers={"Accept": "application/json"}
        )

        if response.status_code != 200:
            print(f"Failed to fetch {sn}: {response.status_code} - {response.text}")
            continue

        data = response.json().get("data", [])
        if not data:
            print(f"No data found for {sn}")
            continue

        df = pd.DataFrame(data)
        csv_path = os.path.join(OUTPUT_DIR, f"{sn}-{file_date}.csv")
        df.to_csv(csv_path, index=False)
        print(f"Saved to {csv_path}")

    except Exception as e:
        print(f"Error fetching {sn}: {e}")
