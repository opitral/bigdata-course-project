from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType


REQUIRED_FIELDS = (
    "event_id",
    "author",
    "city",
    "event_ts",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
)


def flatten_weather_payload(df: DataFrame) -> DataFrame:
    return df.select(
        col("event_id"),
        col("author"),
        col("source"),
        col("schema_v").cast(IntegerType()).alias("schema_v"),
        col("ingest_ts"),
        to_timestamp(col("payload.observed_at"), "yyyy-MM-dd'T'HH:mm").alias("event_ts"),
        trim(col("payload.city")).alias("city"),
        col("payload.latitude").cast(DoubleType()).alias("latitude"),
        col("payload.longitude").cast(DoubleType()).alias("longitude"),
        col("payload.elevation_m").cast(DoubleType()).alias("elevation_m"),
        col("payload.metrics.temperature_2m").cast(DoubleType()).alias("temperature_2m"),
        col("payload.metrics.relative_humidity_2m").cast(DoubleType()).alias("relative_humidity_2m"),
        col("payload.metrics.precipitation").cast(DoubleType()).alias("precipitation"),
        col("payload.metrics.wind_speed_10m").cast(DoubleType()).alias("wind_speed_10m"),
        col("payload.metrics.wind_direction_10m").cast(DoubleType()).alias("wind_direction_10m"),
        col("payload.metrics.surface_pressure").cast(DoubleType()).alias("surface_pressure"),
    )


def reject_null_fields(df: DataFrame) -> DataFrame:
    condition = None
    for field in REQUIRED_FIELDS:
        clause = col(field).isNotNull()
        condition = clause if condition is None else condition & clause
    return df.filter(condition)


def apply_weather_domain_filters(df: DataFrame, author: str) -> DataFrame:
    return df.filter(
        (col("author") == author)
        & col("latitude").between(-90.0, 90.0)
        & col("longitude").between(-180.0, 180.0)
        & col("temperature_2m").between(-90.0, 100.0)
        & col("relative_humidity_2m").between(0.0, 100.0)
        & (col("precipitation") >= 0.0)
        & (col("wind_speed_10m") >= 0.0)
    )


def drop_duplicate_events(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["event_id"])
