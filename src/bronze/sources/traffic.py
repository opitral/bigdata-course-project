import base64
import hashlib
from typing import Dict, List

from bronze.http_client import DEFAULT_TIMEOUT, shared_session
from bronze.record import build_record, stable_event_id

SOURCE_NAME = "lviv_gtfs_rt"
API_ENDPOINT = "https://track.ua-gis.com/gtfs/lviv/vehicle_position"


def collect(author: str) -> List[Dict]:
    response = shared_session().get(API_ENDPOINT, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    raw_bytes = response.content
    digest = hashlib.sha256(raw_bytes).hexdigest()
    payload = {
        "endpoint": API_ENDPOINT,
        "content_type": response.headers.get("Content-Type", "application/x-protobuf"),
        "content_length": len(raw_bytes),
        "content_sha256": digest,
        "encoding": "base64",
        "payload_b64": base64.b64encode(raw_bytes).decode("ascii"),
    }
    event_id = stable_event_id(SOURCE_NAME, digest)
    return [build_record(SOURCE_NAME, author, payload, event_id=event_id)]
