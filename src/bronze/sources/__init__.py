from typing import Callable, Dict, List

from . import alerts, roads, traffic, weather

SourceCollector = Callable[[str], List[Dict]]

STATIC_DOMAINS = frozenset({"alerts", "roads"})

COLLECTORS: Dict[str, Dict[str, object]] = {
    "weather": {"source": weather.SOURCE_NAME, "collect": weather.collect},
    "traffic": {"source": traffic.SOURCE_NAME, "collect": traffic.collect},
    "alerts": {"source": alerts.SOURCE_NAME, "collect": alerts.collect},
    "roads": {"source": roads.SOURCE_NAME, "collect": roads.collect},
}
