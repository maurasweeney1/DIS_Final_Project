# ingest_eia.py
import requests, csv, io, time
from google.cloud import storage

# EIA Open Data 
# https://api.eia.gov/v2/electricity/rto/region-data/data/

BALANCING_AUTHORITIES = ["BPAT","CISO","ERCO","MISO","PJM","NYIS","ISNE","SWPP","AZPS"]

client = storage.Client()
bucket = client.bucket("cpsc482-final-raw")

for ba in BALANCING_AUTHORITIES:
    all_rows = []
    offset = 0
    while True:
        url = (
            "https://api.eia.gov/v2/electricity/rto/region-data/data/"
            f"?frequency=hourly&data[0]=value&facets[respondent][]={ba}"
            f"&start=2018-01-01&end=2024-01-01&sort[0][column]=period"
            f"&sort[0][direction]=asc&offset={offset}&length=5000"
        )
        r = requests.get(url).json()
        records = r.get("response", {}).get("data", [])
        if not records:
            break
        all_rows.extend(records)
        offset += 5000
        time.sleep(0.2)

    buf = io.StringIO()
    if all_rows:
        writer = csv.DictWriter(buf, fieldnames=all_rows[0].keys())
        writer.writeheader(); writer.writerows(all_rows)
        blob = bucket.blob(f"eia/{ba}.csv")
        blob.upload_from_string(buf.getvalue())
        print(f"{ba}: {len(all_rows)} rows uploaded")