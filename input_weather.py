# ingest_weather.py
import requests, json, csv, time
from google.cloud import storage

CITIES = [
    {"name": "seattle", "lat": 47.6, "lon": -122.3, "balancing_authority": "BPAT"},
    {"name": "chicago", "lat": 41.9, "lon": -87.6, "balancing_authority": "PJM"},
    # ... 48 more cities for all major BA regions
]

VARIABLES = "temperature_2m,apparent_temperature,relative_humidity_2m,wind_speed_10m,precipitation,shortwave_radiation,cloud_cover,weather_code"
START, END = "2018-01-01", "2023-12-31"

client = storage.Client()
bucket = client.bucket("cpsc482-final-raw")

for city in CITIES:
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={city['lat']}&longitude={city['lon']}"
        f"&hourly={VARIABLES}&start_date={START}&end_date={END}"
        f"&timezone=UTC"
    )
    r = requests.get(url)
    data = r.json()

    # Flatten JSON to CSV rows
    rows = []
    hours = data["hourly"]["time"]
    for i, ts in enumerate(hours):
        row = {"city": city["name"], "balancing_authority": city["balancing_authority"],
               "lat": city["lat"], "lon": city["lon"], "timestamp": ts}
        for var in VARIABLES.split(","):
            row[var] = data["hourly"].get(var, [None]*len(hours))[i]
        rows.append(row)

    # Write CSV to GCS
    import io
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader(); writer.writerows(rows)
    blob = bucket.blob(f"weather/{city['name']}.csv")
    blob.upload_from_string(buf.getvalue(), content_type="text/csv")
    print(f"Uploaded {city['name']}: {len(rows)} rows")
    time.sleep(0.5)  # rate limit 