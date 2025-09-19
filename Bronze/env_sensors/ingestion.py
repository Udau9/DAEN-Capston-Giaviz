# -*- coding: utf-8 -*-

import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timezone, timedelta

# --- API Configuration ---
URL = "https://data.511-atis-ttrip-prod.iteriscloud.com/smarterRoads/weather/weatherTMDD/current/weather_tmdd.xml"
TOKEN = "$2b$10$PpYJ8iwoT3SoV8tu45vGi.PHzy3ry4C7vAcU4EzRbV0vyb6CFxaGG"

# --- XML Namespace ---
ns = {
    "orci": "http://www.openroadsconsulting.org/weather",
    "ess": "http://www.openroadsconsulting.org/orci_ess",
    "qfree": "http://www.qfree.com/common",
    "tmdd": "http://www.tmdd.org/3/messages",
}

# --- Helper Functions for XML Parsing ---
def first(elem, path):
    """Finds the first child element matching the given path."""
    return elem.find(path, ns) if elem is not None else None

def txt(elem):
    """Extracts and strips text from an element."""
    return elem.text.strip() if (elem is not None and elem.text) else None

def to_iso(d, t, off):
    """Converts a date, time, and offset into an ISO 8601 formatted string."""
    if not d or not t or not off:
        return None
    dt = datetime.strptime(str(d)+str(t), "%Y%m%d%H%M%S")
    sign = 1 if str(off).startswith("+") else -1
    hh = int(str(off)[1:3])
    mm = int(str(off)[3:5])
    tz = timezone(sign * timedelta(hours=hh, minutes=mm))
    return dt.replace(tzinfo=tz).isoformat()

def ingest_data():
    """
    Fetches XML data from the API and converts it into a pandas DataFrame
    with a standardized long-format schema.

    Returns:
        tuple: A tuple containing the raw XML content and the long-format DataFrame.
               Returns (None, None) if the API call fails.
    """
    try:
        resp = requests.get(URL, headers={"Accept": "application/xml"}, params={"token": TOKEN}, timeout=90)
        resp.raise_for_status()
        raw_xml = resp.content
        
        root = ET.fromstring(raw_xml)
        long_rows = []
        for stn in root.findall("orci:station", ns):
            inv = first(stn, "orci:inventory")
            if inv is None:
                continue
            
            org_id = txt(first(inv, ".//organization-information/organization-id"))
            device_id = txt(first(inv, ".//device-inventory-header/device-id"))
            device_name = txt(first(inv, ".//device-name"))
            lat_raw = txt(first(inv, ".//device-location/latitude"))
            lon_raw = txt(first(inv, ".//device-location/longitude"))
            lat = float(lat_raw) / 1e6 if lat_raw and lat_raw.lstrip("-").isdigit() else None
            lon = float(lon_raw) / 1e6 if lon_raw and lon_raw.lstrip("-").isdigit() else None
            
            data = first(stn, "orci:data")
            if data is None:
                continue
                
            for es in data.findall(".//ess-sensor-list/ess-sensor", ns):
                ess_id = txt(first(es, "ess-sensor-id"))
                ts = first(es, "ess-observation-timestamp")
                obs_date = txt(first(ts, "date"))
                obs_time = txt(first(ts, "time"))
                obs_offset = txt(first(ts, "offset"))
                obs_iso = txt(first(es, "qfree:dateTimeHolder/qfree:iso-8601")) or to_iso(obs_date, obs_time, obs_offset)

                base = {
                    "org_id": org_id,
                    "station_device_id": device_id,
                    "station_device_name": device_name,
                    "lat": lat,
                    "lon": lon,
                    "ess_sensor_id": ess_id,
                    "obs_iso8601": obs_iso,
                }

                obs_type = first(es, "ess:ess-observation-type") or first(es, "ess-observation-type")
                if obs_type is not None:
                    for kind in ["weather-data", "surface-data", "subsurface-data"]:
                        bucket = first(obs_type, f"ess:{kind}")
                        if bucket is None:
                            continue
                        for child in list(bucket):
                            tag = child.tag.split("}", 1)[-1]
                            val = txt(child)
                            long_rows.append({
                                **base,
                                "metric_full": f"{kind}__{tag}",
                                "value": val,
                            })
        
        df = pd.DataFrame(long_rows)
        return raw_xml, df
        
    except requests.exceptions.RequestException as e:
        print(f"Error during ingestion: {e}")
        return None, None