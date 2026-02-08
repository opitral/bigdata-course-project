#!/usr/bin/env python
# coding: utf-8

# In[12]:


#!/usr/bin/env python3
import os
import json
import datetime as dt
from typing import List, Dict

import boto3
from botocore.config import Config


def r2_client(account_id: str, access_key_id: str, secret_access_key: str):
    """
    Cloudflare R2 is S3-compatible. Endpoint format:
    https://<account_id>.r2.cloudflarestorage.com
    """
    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

    # R2 doesn't require a real AWS region; "auto" is commonly used.
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )
    return s3


def put_text(s3, bucket: str, key: str, text: str, content_type: str = "text/plain; charset=utf-8"):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType=content_type,
    )


def put_bytes(s3, bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream"):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType=content_type,
    )


def put_folder_marker(s3, bucket: str, prefix: str):
    """
    Optional: creates a zero-byte object with trailing slash.
    Not required for S3/R2, but sometimes helpful for UI browsing.
    """
    if not prefix.endswith("/"):
        prefix += "/"
    s3.put_object(Bucket=bucket, Key=prefix, Body=b"")


def build_bronze_layout(base_prefix: str, ingest_date: str, hour: str) -> List[Dict]:
    """
    Returns a list of objects to create:
    { "folder": "<prefix>", "file_key": "<key>", "content": "<text>", "content_type": "<type>" }
    """
    # Domains & sources (adjust to your real ones)
    layout = [
        # Traffic events
        {
            "folder": f"{base_prefix}/bronze/domain=traffic/source=demo_traffic_api/schema_v=1/ingest_date={ingest_date}/hour={hour}",
            "file_key": f"{base_prefix}/bronze/domain=traffic/source=demo_traffic_api/schema_v=1/ingest_date={ingest_date}/hour={hour}/part-0001.jsonl",
            "content_type": "application/json",
            "content": "\n".join(
                [
                    json.dumps(
                        {
                            "ingest_ts": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
                            "source": "demo_traffic_api",
                            "schema_v": 1,
                            "event_id": "evt-traffic-001",
                            "payload": {
                                "event_time": ingest_date + f"T{hour}:00:05Z",
                                "road_id": "M01",
                                "speed_kmh": 17.2,
                                "lat": 50.4501,
                                "lon": 30.5234,
                                "confidence": 0.92,
                            },
                        }
                    ),
                    json.dumps(
                        {
                            "ingest_ts": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
                            "source": "demo_traffic_api",
                            "schema_v": 1,
                            "event_id": "evt-traffic-002",
                            "payload": {
                                "event_time": ingest_date + f"T{hour}:02:10Z",
                                "road_id": "M01",
                                "speed_kmh": 11.8,
                                "lat": 50.4510,
                                "lon": 30.5250,
                                "confidence": 0.88,
                            },
                        }
                    ),
                ]
            ),
        },
        # Weather events
        {
            "folder": f"{base_prefix}/bronze/domain=weather/source=demo_weather_api/schema_v=1/ingest_date={ingest_date}/hour={hour}",
            "file_key": f"{base_prefix}/bronze/domain=weather/source=demo_weather_api/schema_v=1/ingest_date={ingest_date}/hour={hour}/part-0001.jsonl",
            "content_type": "application/json",
            "content": json.dumps(
                {
                    "ingest_ts": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    "source": "demo_weather_api",
                    "schema_v": 1,
                    "event_id": "evt-weather-001",
                    "payload": {
                        "event_time": ingest_date + f"T{hour}:00:00Z",
                        "temp_c": -2.4,
                        "precip_mm": 0.0,
                        "wind_mps": 4.1,
                        "lat": 50.4501,
                        "lon": 30.5234,
                    },
                }
            )
            + "\n",
        },
        # Alerts events
        {
            "folder": f"{base_prefix}/bronze/domain=alerts/source=demo_city_alerts/schema_v=1/ingest_date={ingest_date}/hour={hour}",
            "file_key": f"{base_prefix}/bronze/domain=alerts/source=demo_city_alerts/schema_v=1/ingest_date={ingest_date}/hour={hour}/part-0001.jsonl",
            "content_type": "application/json",
            "content": json.dumps(
                {
                    "ingest_ts": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    "source": "demo_city_alerts",
                    "schema_v": 1,
                    "event_id": "evt-alert-001",
                    "payload": {
                        "event_time": ingest_date + f"T{hour}:05:00Z",
                        "type": "accident",
                        "severity": "medium",
                        "road_id": "M01",
                        "message": "ДТП: часткове перекриття смуги",
                    },
                }
            )
            + "\n",
        },
        # Roads snapshot (OSM) – versioned dataset (not hourly)
        {
            "folder": f"{base_prefix}/bronze/domain=roads/source=osm_snapshot/dataset_version={ingest_date}",
            "file_key": f"{base_prefix}/bronze/domain=roads/source=osm_snapshot/dataset_version={ingest_date}/roads_osm_demo.txt",
            "content_type": "text/plain; charset=utf-8",
            "content": "OSM snapshot demo placeholder\n",
        },
    ]
    return layout


def main():
    # ---- REQUIRED ENV VARS ----
    account_id = 'eb6d4c726cb12dea7f6d42071c3f5fb4'#os.environ["R2_ACCOUNT_ID"]          # e.g. "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    access_key = '3e6a98f17c3a477dedfd8d4f7792b670'#os.environ["R2_ACCESS_KEY_ID"]       # S3 API Access Key ID
    secret_key = '973833353d78529cc727c5e11bea6c8b0aec3d643d810563bd3db3abaa6f0a0e'#os.environ["R2_SECRET_ACCESS_KEY"]   # S3 API Secret Access Key
    bucket = 'data'#os.environ["R2_BUCKET"]                  # bucket name

    # Optional: prefix inside bucket (e.g., "project-city-traffic"). Empty = root
    base_prefix ='raw'#os.environ.get("R2_BASE_PREFIX", "").strip().strip("/")
    base_prefix = base_prefix if base_prefix else ""  # "" or "some/prefix"
    
    # Ingest partition (you can fix or compute current)
    ingest_date = os.environ.get("INGEST_DATE") or dt.date.today().isoformat()
    hour = os.environ.get("INGEST_HOUR") or f"{dt.datetime.now().hour:02d}"

    s3 = r2_client(account_id, access_key, secret_key)
    # Build objects to create
    layout = build_bronze_layout(base_prefix, ingest_date, hour)

    # Create folders (markers) + files
    for item in layout:
        folder = item["folder"].lstrip("/")
        file_key = item["file_key"].lstrip("/")
        content = item["content"]
        content_type = item["content_type"]

        # Optional marker
        put_folder_marker(s3, bucket, folder + "/")

        # Put file
        put_text(s3, bucket, file_key, content, content_type=content_type)

        print(f"OK: s3://{bucket}/{file_key}")

    print("\nDone. Open your R2 bucket and browse the prefixes.")


# In[11]:


main()


# In[4]:





# In[ ]:




