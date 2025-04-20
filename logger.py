import requests, json, os
from datetime import datetime
import csv

# --- config ---
TARGET_KEYWORDS = [
    "Worli", "Bandra Kurla Complex, Mumbai - MPCB", "Malad West, Mumbai - iitm", "Mazgaon",
    "chembur", "Navy", "Powai", "Siddharth", "Sion Mumbai MPCB", "kurla", 
    "byculla", "Vile", "Kherwadi_Bandra East", "Borivali", "Chakala", 
    "Chhatrapati Shivaji Intl", "Colaba", "Deonar Mumbai ITM", "Shivaji Nagar, Mumbai", 
    "Kandivali East", "Khindipada", "Bandra Kurla Complex, Mumbai - IITM"
]
POLLUTANTS = ["PM2.5", "PM10", "NO", "NO2", "NOx", "NH3", "SO2", "CO", "Ozone"]
API_URL = "https://airquality.cpcb.gov.in/caaqms/iit_rss_feed_with_coordinates"

# Ensure output folder exists
os.makedirs("output", exist_ok=True)

def matches(name):
    return any(k.lower() in name.lower() for k in TARGET_KEYWORDS)

def extract(raw):
    d = {p: "NA" for p in POLLUTANTS}
    for item in raw:
        pid = item["indexId"].upper().replace(".", "")
        for p in POLLUTANTS:
            if pid == p.upper().replace(".", ""):
                d[p] = item.get("avg", "NA")
    return [d[p] for p in POLLUTANTS]

def run():
    r = requests.get(API_URL).json()
    records = []

    for st in sum((city["stationsInCity"]
                   for state in r["country"]
                   for city in state["citiesInState"]), []):
        name = st["stationName"]
        if matches(name):
            # handle date and time from lastUpdate
            api_time = st.get("lastUpdate", "")
            if api_time:
                dt = datetime.strptime(api_time, "%d-%m-%Y %H:%M:%S")
                date = dt.strftime("%Y-%m-%d")
                time = dt.strftime("%H:%M:%S")
            else:
                now = datetime.utcnow()
                date = now.strftime("%Y-%m-%d")
                time = now.strftime("%H:%M:%S")

            row = {
                "location": name,
                "date": date,
                "time": time,
            }
            # add pollutants
            vals = extract(st.get("pollutants", []))
            for p, val in zip(POLLUTANTS, vals):
                row[p] = val
            # add meta
            row["predominant"] = st.get("predominantParameter", "NA")
            row["AQI"] = st.get("airQualityIndexValue", "NA")
            records.append(row)

    # write JSON
    with open("output/data.json", "w", encoding="utf-8") as f:
        json.dump({
            "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "data": records
        }, f, indent=2)

    # write CSV
    with open("output/data.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["location", "date", "time", *POLLUTANTS, "predominant", "AQI"])
        for r in records:
            w.writerow([r["location"], r["date"], r["time"]] +
                       [r[p] for p in POLLUTANTS] +
                       [r["predominant"], r["AQI"]])

if __name__ == "__main__":
    run()
