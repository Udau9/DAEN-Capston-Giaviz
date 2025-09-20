import requests
import pandas as pd
from pandas import json_normalize
from datetime import datetime
import itertools
import os
import json

# ----------------------------
# Config
# ----------------------------
URL = "https://data.511-atis-ttrip-prod.iteriscloud.com/smarterRoads/other/vDOTRoadClosures/current/RoadClosures_current.json"
TOKEN = "$2b$10$PV4h67dDHewvUmQUSw.4EehnJ2kMRXNxo0M2/edpg2Q9/1KvHQnk2"

BRONZE_DIR = "./bronze/vdot_incidents"
os.makedirs(BRONZE_DIR, exist_ok=True)

# ----------------------------
# Helpers
# ----------------------------
def preview(obj, max_chars=500):
    """Show small preview of JSON"""
    try:
        s = json.dumps(obj, ensure_ascii=False)[:max_chars]
        return s + ("..." if len(s) == max_chars else "")
    except:
        return str(obj)[:max_chars]

def find_records(payload):
    """Find record list in API response"""
    container = payload
    if isinstance(payload, dict):
        for k in ("roadClosures", "RoadClosures", "data", "items", "records"):
            if k in payload:
                container = payload[k]
                break
    if isinstance(container, list):
        recs = [r for r in container if isinstance(r, dict)]
        return recs, f"Found list with {len(recs)} dict records."
    if isinstance(container, dict):
        values = list(container.values())
        if values and all(isinstance(v, dict) for v in values):
            recs = [{"record_id": k, **v} for k, v in container.items()]
            return recs, f"Found dict keyed by IDs with {len(recs)} records."
    return [], "No record-like container found."

# ----------------------------
# Main pipeline
# ----------------------------
def main():
    # Fetch API
    r = requests.get(
        URL,
        params={"token": TOKEN},
        headers={"Accept": "application/json"},
        timeout=60
    )
    r.raise_for_status()
    data = r.json()

    print("Top-level type:", type(data).__name__)
    if isinstance(data, dict):
        print("Top-level keys:", list(itertools.islice(data.keys(), 0, 15)))
    print("Top-level preview:", preview(data))

    # Extract records
    records, how = find_records(data)
    print(how)

    if not records:
        print("⚠️ No records found (could mean no current closures).")
        return

    # Flatten JSON → DataFrame
    df = json_normalize(records, sep="_")
    print("Row count:", len(df))
    print("Column count:", len(df.columns))

    # Save to Bronze
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(BRONZE_DIR, f"road_closures_raw_{timestamp}.csv")
    df.to_csv(file_path, index=False)
    print(f"✅ Raw VDOT closures saved to Bronze: {file_path}")

if __name__ == "__main__":
    main()
