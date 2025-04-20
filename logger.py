
import requests, json, os
from datetime import datetime

# --- config ---
TARGET_KEYWORDS = [
    "Worli","Kurla","Malad West","Mazgaon","Chembur","Navy",
    "Powai","Siddharth","Sion","Byculla","Vile","Kherwadi",
    "Borivali","Chakala","Chhatrapati","Colaba","Deonar",
    "Shivaji Nagar","Kandivali","Khindipada"
]
POLLUTANTS = ["PM2.5","PM10","NO","NO2","NOx","NH3","SO2","CO","Ozone"]
API_URL = "https://airquality.cpcb.gov.in/caaqms/iit_rss_feed_with_coordinates"

# Ensure output folder
os.makedirs("output", exist_ok=True)

def matches(name):
    return any(k.lower() in name.lower() for k in TARGET_KEYWORDS)

def extract(raw):
    d = {p:"NA" for p in POLLUTANTS}
    for item in raw:
        pid = item["indexId"].upper().replace(".","")
        for p in POLLUTANTS:
            if pid == p.upper().replace(".",""):
                d[p] = item.get("avg","NA")
    return [d[p] for p in POLLUTANTS]

def run():
    r = requests.get(API_URL).json()
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    records = []

    for st in sum((city["stationsInCity"]
                   for state in r["country"]
                   for city in state["citiesInState"]), []):
        name = st["stationName"]
        if matches(name):
            row = {
                "location": name,
                "timestamp": ts,
            }
            # add pollutants
            vals = extract(st.get("pollutants",[]))
            for p,val in zip(POLLUTANTS, vals):
                row[p] = val
            # add meta
            row["predominant"] = st.get("predominantParameter","NA")
            row["AQI"]         = st.get("airQualityIndexValue","NA")
            records.append(row)

    # write JSON
    with open("output/data.json","w",encoding="utf-8") as f:
        json.dump({"last_updated": ts, "data": records}, f, indent=2)

    # also write CSV
    import csv
    with open("output/data.csv","w",newline="",encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["location","timestamp", *POLLUTANTS, "predominant","AQI"])
        for r in records:
            w.writerow([r["location"],r["timestamp"]]+[r[p] for p in POLLUTANTS]+[r["predominant"],r["AQI"]])

if __name__=="__main__":
    run()
