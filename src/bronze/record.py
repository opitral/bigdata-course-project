import datetime as dt
import hashlib
import uuid
from typing import Any, Dict, Optional

from common.constants import SCHEMA_VERSION


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_event_id(source: str, natural_key: str) -> str:
    digest = hashlib.sha1(f"{source}:{natural_key}".encode("utf-8")).hexdigest()
    return f"{source}-{digest[:16]}"


def random_event_id(source: str) -> str:
    return f"{source}-{uuid.uuid4().hex}"


def build_record(
    source: str,
    author: str,
    payload: Dict[str, Any],
    event_id: Optional[str] = None,
    ingested_at: Optional[dt.datetime] = None,
) -> Dict[str, Any]:
    when = ingested_at or utc_now()
    return {
        "ingest_ts": iso_utc(when),
        "source": source,
        "schema_v": SCHEMA_VERSION,
        "author": author,
        "event_id": event_id or random_event_id(source),
        "payload": payload,
    }
