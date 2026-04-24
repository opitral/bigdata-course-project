from typing import Dict, List

from lab1_ingest.http_client import DEFAULT_TIMEOUT, shared_session
from lab1_ingest.record import build_record, stable_event_id

SOURCE_NAME = "vadimkin_air_raid"
GITHUB_TREE_URL = (
    "https://api.github.com/repos/Vadimkin/ukrainian-air-raid-sirens-dataset/git/trees/main?recursive=1"
)
RAW_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/Vadimkin/ukrainian-air-raid-sirens-dataset/main/{path}"
)
PREFERRED_SUFFIXES = ("official_data_en.csv", "official_en.csv")


def _resolve_csv_url() -> str:
    session = shared_session()
    response = session.get(GITHUB_TREE_URL, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    tree = response.json().get("tree", [])
    paths = [entry["path"] for entry in tree if entry.get("type") == "blob"]

    for suffix in PREFERRED_SUFFIXES:
        for path in paths:
            if path.endswith(suffix):
                return RAW_URL_TEMPLATE.format(path=path)

    for path in paths:
        if path.startswith("datasets/") and path.endswith(".csv") and "official" in path:
            return RAW_URL_TEMPLATE.format(path=path)

    raise RuntimeError("Cannot locate an official air-raid alerts CSV in the dataset repo")


def _split_csv(text: str) -> List[Dict[str, str]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return []
    header = [field.strip() for field in lines[0].split(",")]
    rows: List[Dict[str, str]] = []
    for line in lines[1:]:
        values = [value.strip() for value in line.split(",")]
        if len(values) < len(header):
            values.extend([""] * (len(header) - len(values)))
        rows.append({header[i]: values[i] for i in range(len(header))})
    return rows


def collect(author: str) -> List[Dict]:
    csv_url = _resolve_csv_url()
    response = shared_session().get(csv_url, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    rows = _split_csv(response.text)

    records: List[Dict] = []
    for row in rows:
        natural_key = "|".join(
            row.get(field, "")
            for field in ("region", "start", "end", "start_time", "end_time", "oblast")
        ) or str(row)
        payload = {"source_url": csv_url, "row": row}
        event_id = stable_event_id(SOURCE_NAME, natural_key)
        records.append(build_record(SOURCE_NAME, author, payload, event_id=event_id))
    return records
