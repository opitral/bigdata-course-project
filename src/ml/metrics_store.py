import json
import os
from typing import Any


def load_metrics(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def save_metrics_section(path: str, section: str, payload: Any) -> None:
    current = load_metrics(path)
    current[section] = payload
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(current, fh, indent=2, ensure_ascii=False, default=str)
