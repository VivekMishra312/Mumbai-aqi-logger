import requests, json, os, csv
from datetime import datetime
from pytz import timezone

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
OUTPUT_DIR = "output"
IST = timezone('Asia/Kolkata')

os.makedirs(OUTPUT_DIR, exist_ok=True)

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

def sanitize_filename(name):
    return name.replace(",", "").replace(".", "").replace(" ", "_")

def run():
    r = requests.get(API_URL).json()
    records = []

    for st in sum((city["stationsInCity"] for state in r["country"] for city in state["citiesInState"]), []):
        name = st["stationName"]
        if matches(name):
            api_time = st.get("lastUpdate", "")
            if api_time:
                dt = datetime.strptime(api_time, "%d-%m-%Y %H:%M:%S")
                dt = timezone("UTC").localize(dt).astimezone(IST)
            else:
                dt = datetime.utcnow()
                dt = timezone("UTC").localize(dt).astimezone(IST)

            date = dt.strftime("%Y-%m-%d")
            time = dt.strftime("%H:%M:%S")
            timestamp = dt.strftime("%Y%m%d_%H%M%S")

            row = {
                "location": name,
                "date": date,
                "time": time
            }
            vals = extract(st.get("pollutants", []))
            for p, val in zip(POLLUTANTS, vals):
                row[p] = val

            row["predominant"] = st.get("predominantParameter", "NA")
            row["AQI"] = st.get("airQualityIndexValue", "NA")
            records.append(row)

            # Save to individual folder
            folder_name = os.path.join(OUTPUT_DIR, sanitize_filename(name))
            os.makedirs(folder_name, exist_ok=True)

            # Append to per-location CSV
            csv_file = os.path.join(folder_name, f"{sanitize_filename(name)}.csv")
            file_exists = os.path.exists(csv_file)
            with open(csv_file, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if not file_exists:
                    w.writerow(["location", "date", "time", *POLLUTANTS, "predominant", "AQI"])
                w.writerow([row["location"], row["date"], row["time"]] +
                           [row[p] for p in POLLUTANTS] +
                           [row["predominant"], row["AQI"]])

            # Save JSON snapshot
            json_file = os.path.join(folder_name, f"{timestamp}.json")
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(row, f, indent=2)

    # Save master JSON
    with open(os.path.join(OUTPUT_DIR, "data.json"), "w", encoding="utf-8") as f:
        json.dump({
            "last_updated": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            "data": records
        }, f, indent=2)

    # Save master CSV
    with open(os.path.join(OUTPUT_DIR, "data.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["location", "date", "time", *POLLUTANTS, "predominant", "AQI"])
        for r in records:
            w.writerow([r["location"], r["date"], r["time"]] +
                       [r[p] for p in POLLUTANTS] +
                       [r["predominant"], r["AQI"]])

if __name__ == "__main__":
    run()
