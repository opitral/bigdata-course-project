from typing import Dict, List
from urllib.parse import urlencode

from bronze.http_client import DEFAULT_TIMEOUT, shared_session
from bronze.record import build_record, stable_event_id

SOURCE_NAME = "open_meteo_current"
API_ENDPOINT = "https://api.open-meteo.com/v1/forecast"

CITIES = (
    {"city": "Kyiv", "latitude": 50.4501, "longitude": 30.5234},
    {"city": "Lviv", "latitude": 49.8397, "longitude": 24.0297},
    {"city": "Kharkiv", "latitude": 49.9935, "longitude": 36.2304},
    {"city": "Odesa", "latitude": 46.4825, "longitude": 30.7233},
    {"city": "Dnipro", "latitude": 48.4647, "longitude": 35.0462},
)

CURRENT_METRICS = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "surface_pressure",
)


def _build_url() -> str:
    params = {
        "latitude": ",".join(str(c["latitude"]) for c in CITIES),
        "longitude": ",".join(str(c["longitude"]) for c in CITIES),
        "current": ",".join(CURRENT_METRICS),
        "timezone": "UTC",
    }
    return f"{API_ENDPOINT}?{urlencode(params)}"


def collect(author: str) -> List[Dict]:
    response = shared_session().get(_build_url(), timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    body = response.json()
    items = body if isinstance(body, list) else [body]

    records: List[Dict] = []
    for index, city in enumerate(CITIES):
        item = items[index] if index < len(items) else {}
        current = item.get("current") or {}
        units = item.get("current_units") or {}
        event_time = current.get("time") or ""
        payload = {
            "city": city["city"],
            "latitude": item.get("latitude", city["latitude"]),
            "longitude": item.get("longitude", city["longitude"]),
            "elevation_m": item.get("elevation"),
            "observed_at": event_time,
            "metrics": {metric: current.get(metric) for metric in CURRENT_METRICS},
            "units": {metric: units.get(metric) for metric in CURRENT_METRICS},
        }
        event_id = stable_event_id(SOURCE_NAME, f"{city['city']}|{event_time}")
        records.append(build_record(SOURCE_NAME, author, payload, event_id=event_id))
    return records
