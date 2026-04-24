from typing import Dict, List

from lab1_ingest.http_client import shared_session
from lab1_ingest.record import build_record, stable_event_id

SOURCE_NAME = "overpass_kyiv_highways"
OVERPASS_ENDPOINT = "https://overpass-api.de/api/interpreter"
OVERPASS_TIMEOUT = 180

KYIV_BBOX = (50.2133, 30.2394, 50.5908, 30.8259)

PRIMARY_CLASSES = (
    "motorway",
    "trunk",
    "primary",
    "secondary",
    "tertiary",
)


def _build_query() -> str:
    south, west, north, east = KYIV_BBOX
    classes = "|".join(PRIMARY_CLASSES)
    return (
        f"[out:json][timeout:120];"
        f'way["highway"~"^({classes})$"]({south},{west},{north},{east});'
        f"out tags center;"
    )


def collect(author: str) -> List[Dict]:
    response = shared_session().post(
        OVERPASS_ENDPOINT,
        data={"data": _build_query()},
        timeout=OVERPASS_TIMEOUT,
    )
    response.raise_for_status()
    body = response.json()
    elements = body.get("elements", [])

    records: List[Dict] = []
    for element in elements:
        if element.get("type") != "way":
            continue
        tags = element.get("tags") or {}
        center = element.get("center") or {}
        way_id = element.get("id")
        payload = {
            "way_id": way_id,
            "highway_class": tags.get("highway"),
            "name": tags.get("name"),
            "name_en": tags.get("name:en"),
            "ref": tags.get("ref"),
            "maxspeed": tags.get("maxspeed"),
            "lanes": tags.get("lanes"),
            "surface": tags.get("surface"),
            "oneway": tags.get("oneway"),
            "center_lat": center.get("lat"),
            "center_lon": center.get("lon"),
            "tags": tags,
        }
        event_id = stable_event_id(SOURCE_NAME, f"way/{way_id}")
        records.append(build_record(SOURCE_NAME, author, payload, event_id=event_id))
    return records
