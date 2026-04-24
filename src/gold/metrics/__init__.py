from gold.metrics.temperature_hourly import compute as compute_temperature_hourly
from gold.metrics.precipitation_daily import compute as compute_precipitation_daily
from gold.metrics.wind_daily_extremes import compute as compute_wind_daily_extremes
from gold.metrics.subzero_hours_daily import compute as compute_subzero_hours_daily
from gold.metrics.comfort_index_hourly import compute as compute_comfort_index_hourly


WEATHER_METRIC_REGISTRY = {
    "temperature_hourly": {
        "compute": compute_temperature_hourly,
        "partition_cols": ("window_date", "window_hour"),
    },
    "precipitation_daily": {
        "compute": compute_precipitation_daily,
        "partition_cols": ("window_date",),
    },
    "wind_daily_extremes": {
        "compute": compute_wind_daily_extremes,
        "partition_cols": ("window_date",),
    },
    "subzero_hours_daily": {
        "compute": compute_subzero_hours_daily,
        "partition_cols": ("window_date",),
    },
    "comfort_index_hourly": {
        "compute": compute_comfort_index_hourly,
        "partition_cols": ("window_date", "window_hour"),
    },
}
