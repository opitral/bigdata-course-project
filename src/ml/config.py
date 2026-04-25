from dataclasses import dataclass


KYIV_LAT_MIN = 50.2
KYIV_LAT_MAX = 50.6
KYIV_LON_MIN = 30.2
KYIV_LON_MAX = 30.9
KYIV_CENTER = (50.4501, 30.5234)

DEFAULT_TIME_FROM = "2026-02-01"
DEFAULT_TIME_TO = "2026-04-01"

KPT_CONGESTION_THRESHOLD_KMH = 20.0
TOMTOM_CONGESTION_RATIO = 0.7

KPT_SOCKETIO_SOURCE = "kpt_socketio"
TOMTOM_SOURCE = "tomtom_traffic_katerina_demianik"

WEATHER_SOURCES = (
    "open_meteo_current",
    "open_meteo_api",
    "openweathermap_api",
)

RANDOM_STATE = 42
TEST_SIZE = 0.2


@dataclass(frozen=True)
class DatasetPaths:
    kpt: str = "reports/dataset_kpt.parquet"
    tomtom: str = "reports/dataset_tomtom.parquet"
    metrics: str = "reports/metrics.json"
    kmeans_centers: str = "reports/kmeans_centers.json"
    heatmap: str = "reports/heatmap.html"
    predictions_kpt: str = "reports/predictions_kpt.parquet"
    predictions_tomtom: str = "reports/predictions_tomtom.parquet"


PATHS = DatasetPaths()
