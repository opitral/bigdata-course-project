import argparse
import io
import logging
import os
import sys

import boto3
import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.author import resolve_author
from common.paths import silver_read_uri
from common.spark import build_spark_session
from ml.config import (
    DEFAULT_TIME_FROM,
    DEFAULT_TIME_TO,
    KPT_CONGESTION_THRESHOLD_KMH,
    KPT_SOCKETIO_SOURCE,
    KYIV_LAT_MAX,
    KYIV_LAT_MIN,
    KYIV_LON_MAX,
    KYIV_LON_MIN,
    PATHS,
    TOMTOM_CONGESTION_RATIO,
    TOMTOM_SOURCE,
    WEATHER_SOURCES,
)


logger = logging.getLogger("ml.build_dataset")

CELL_DIGITS = 2


def kyiv_bbox_filter():
    return (
        (F.col("lat") >= KYIV_LAT_MIN)
        & (F.col("lat") <= KYIV_LAT_MAX)
        & (F.col("lon") >= KYIV_LON_MIN)
        & (F.col("lon") <= KYIV_LON_MAX)
    )


def within_time_window(ts_col: str, time_from: str, time_to: str):
    return (F.col(ts_col) >= F.lit(time_from).cast(T.TimestampType())) & (
        F.col(ts_col) < F.lit(time_to).cast(T.TimestampType())
    )


def attach_calendar_features(df: DataFrame, ts_col: str) -> DataFrame:
    return (
        df.withColumn("hour_of_day", F.hour(F.col(ts_col)))
        .withColumn("day_of_week", F.dayofweek(F.col(ts_col)) - F.lit(1))
        .withColumn("is_weekend", (F.dayofweek(F.col(ts_col)).isin(1, 7)).cast("int"))
    )


def direction_bucket_col(heading_deg_col):
    return (
        F.when((heading_deg_col >= 315) | (heading_deg_col < 45), F.lit("N"))
        .when((heading_deg_col >= 45) & (heading_deg_col < 135), F.lit("E"))
        .when((heading_deg_col >= 135) & (heading_deg_col < 225), F.lit("S"))
        .otherwise(F.lit("W"))
    )


def read_silver_parquet(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .parquet(path)
    )


def load_kpt_traffic(spark: SparkSession, time_from: str, time_to: str) -> DataFrame:
    path = silver_read_uri("traffic", KPT_SOCKETIO_SOURCE)
    df = read_silver_parquet(spark, path)
    df = df.filter(kyiv_bbox_filter())
    df = df.filter(within_time_window("event_ts", time_from, time_to))
    df = df.filter((F.col("speed") >= 0) & (F.col("speed") < 150))
    df = df.withColumn("lat_cell", F.round(F.col("lat"), CELL_DIGITS))
    df = df.withColumn("lon_cell", F.round(F.col("lon"), CELL_DIGITS))
    df = df.withColumn("event_hour", F.date_trunc("hour", F.col("event_ts")))
    df = df.withColumn("heading_rad", F.radians(F.col("heading").cast("double")))
    return df


def aggregate_kpt_cells(df: DataFrame) -> DataFrame:
    agg = df.groupBy("event_hour", "lat_cell", "lon_cell").agg(
        F.count("*").alias("events_count"),
        F.avg(F.col("speed").cast("double")).alias("avg_speed_kmh"),
        F.stddev(F.col("speed").cast("double")).alias("speed_std_kmh"),
        F.avg(F.sin(F.col("heading_rad"))).alias("heading_sin"),
        F.avg(F.cos(F.col("heading_rad"))).alias("heading_cos"),
    )
    agg = agg.withColumn("speed_std_kmh", F.coalesce(F.col("speed_std_kmh"), F.lit(0.0)))
    agg = agg.withColumn(
        "heading_deg_mean",
        (F.degrees(F.atan2(F.col("heading_sin"), F.col("heading_cos"))) + F.lit(360.0)) % F.lit(360.0),
    )
    agg = agg.withColumn("direction_bucket", direction_bucket_col(F.col("heading_deg_mean")))
    agg = agg.withColumn(
        "is_congested",
        (F.col("avg_speed_kmh") < F.lit(KPT_CONGESTION_THRESHOLD_KMH)).cast("int"),
    )
    agg = agg.filter(F.col("events_count") >= 3)
    agg = agg.withColumnRenamed("lat_cell", "lat").withColumnRenamed("lon_cell", "lon")
    agg = attach_calendar_features(agg, "event_hour")
    return agg


TOMTOM_S3_PREFIX = f"processed/silver/domain=traffic/source={TOMTOM_SOURCE}/"


def build_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    )


def load_tomtom_traffic_pandas(time_from: str, time_to: str) -> pd.DataFrame:
    s3 = build_r2_client()
    bucket = os.environ.get("R2_BUCKET", "data")
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=TOMTOM_S3_PREFIX):
        for obj in page.get("Contents", []) or []:
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    logger.info("TomTom parquet files to read: %d", len(keys))

    frames = []
    for key in keys:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        table = pq.read_table(io.BytesIO(body))
        frame = table.to_pandas()
        for col in ("current_speed_kmh", "free_flow_speed_kmh", "current_travel_time_s", "confidence"):
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce").astype(float)
        frames.append(frame)
    if not frames:
        raise RuntimeError("No tomtom parquet files were read")

    df = pd.concat(frames, ignore_index=True, sort=False)

    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True, errors="coerce").dt.tz_convert(None)
    time_from_ts = pd.Timestamp(time_from)
    time_to_ts = pd.Timestamp(time_to)

    mask = (
        (df["lat"] >= KYIV_LAT_MIN)
        & (df["lat"] <= KYIV_LAT_MAX)
        & (df["lon"] >= KYIV_LON_MIN)
        & (df["lon"] <= KYIV_LON_MAX)
        & (df["event_ts"] >= time_from_ts)
        & (df["event_ts"] < time_to_ts)
        & (df["current_speed_kmh"] > 0)
        & (df["free_flow_speed_kmh"] > 0)
        & (df["road_closure"] == False)  # noqa: E712
    )
    df = df.loc[mask].copy()

    df["event_hour"] = df["event_ts"].dt.floor("h")
    df["hour_of_day"] = df["event_hour"].dt.hour
    df["day_of_week"] = df["event_hour"].dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["speed_ratio"] = df["current_speed_kmh"] / df["free_flow_speed_kmh"]
    df["is_congested"] = (df["speed_ratio"] < TOMTOM_CONGESTION_RATIO).astype(int)

    keep = [
        "event_hour",
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "road_id",
        "road_name",
        "lat",
        "lon",
        "current_speed_kmh",
        "free_flow_speed_kmh",
        "current_travel_time_s",
        "confidence",
        "speed_ratio",
        "is_congested",
    ]
    return df[keep].reset_index(drop=True)


def normalize_open_meteo_current(spark: SparkSession) -> DataFrame:
    path = silver_read_uri("weather", "open_meteo_current")
    df = read_silver_parquet(spark, path)
    return df.select(
        F.col("event_ts").alias("event_ts"),
        F.col("latitude").alias("lat"),
        F.col("longitude").alias("lon"),
        F.col("temperature_2m").cast("double").alias("temp_c"),
        F.col("precipitation").cast("double").alias("precip_mm"),
        (F.col("wind_speed_10m").cast("double") / F.lit(3.6)).alias("wind_mps"),
        F.col("relative_humidity_2m").cast("double").alias("humidity_pct"),
    )


def normalize_open_meteo_api(spark: SparkSession) -> DataFrame:
    path = silver_read_uri("weather", "open_meteo_api")
    df = read_silver_parquet(spark, path)
    return df.select(
        F.col("obs_time").alias("event_ts"),
        F.col("latitude").alias("lat"),
        F.col("longitude").alias("lon"),
        F.col("temperature_2m").cast("double").alias("temp_c"),
        F.col("precipitation").cast("double").alias("precip_mm"),
        (F.col("wind_speed_10m").cast("double") / F.lit(3.6)).alias("wind_mps"),
        F.col("relative_humidity_2m").cast("double").alias("humidity_pct"),
    )


def normalize_openweathermap_api(spark: SparkSession) -> DataFrame:
    path = silver_read_uri("weather", "openweathermap_api")
    df = read_silver_parquet(spark, path)
    return df.select(
        F.col("event_time").alias("event_ts"),
        F.col("lat").alias("lat"),
        F.col("lon").alias("lon"),
        F.col("temp_c").cast("double").alias("temp_c"),
        F.col("rain_1h_mm").cast("double").alias("precip_mm"),
        F.col("wind_speed_mps").cast("double").alias("wind_mps"),
        F.col("humidity_pct").cast("double").alias("humidity_pct"),
    )


WEATHER_LOADERS = {
    "open_meteo_current": normalize_open_meteo_current,
    "open_meteo_api": normalize_open_meteo_api,
    "openweathermap_api": normalize_openweathermap_api,
}


def load_hourly_weather(spark: SparkSession, time_from: str, time_to: str) -> DataFrame:
    frames = []
    for src in WEATHER_SOURCES:
        try:
            frames.append(WEATHER_LOADERS[src](spark))
        except Exception as exc:
            logger.warning("Skipping weather source '%s' due to read error: %s", src, exc)
    if not frames:
        raise RuntimeError("No weather sources could be loaded")
    unified = frames[0]
    for other in frames[1:]:
        unified = unified.unionByName(other, allowMissingColumns=True)
    unified = unified.filter(kyiv_bbox_filter())
    unified = unified.filter(within_time_window("event_ts", time_from, time_to))
    unified = unified.withColumn("event_hour", F.date_trunc("hour", F.col("event_ts")))
    hourly = unified.groupBy("event_hour").agg(
        F.avg("temp_c").alias("temp_c"),
        F.avg("precip_mm").alias("precip_mm"),
        F.avg("wind_mps").alias("wind_mps"),
        F.avg("humidity_pct").alias("humidity_pct"),
    )
    hourly = hourly.fillna({"precip_mm": 0.0})
    return hourly


def join_weather(traffic: DataFrame, weather: DataFrame) -> DataFrame:
    return traffic.join(F.broadcast(weather), on="event_hour", how="inner")


def persist_parquet(df: DataFrame, path: str, author: str) -> int:
    df_with_author = df.withColumn("author", F.lit(author))
    pandas_df = df_with_author.toPandas()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    pandas_df.to_parquet(path, index=False)
    return len(pandas_df)


def build_datasets(time_from: str, time_to: str, force: bool = False) -> None:
    author = resolve_author()
    need_kpt = force or not os.path.exists(PATHS.kpt)
    need_tomtom = force or not os.path.exists(PATHS.tomtom)
    if not need_kpt and not need_tomtom:
        logger.info("Both datasets already exist (%s, %s) — nothing to do (use --force to rebuild)", PATHS.kpt, PATHS.tomtom)
        return

    spark = build_spark_session(f"ml-build-dataset-{author}")
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    try:
        logger.info("Loading hourly weather (sources=%s)", list(WEATHER_SOURCES))
        weather = load_hourly_weather(spark, time_from, time_to).cache()
        weather_rows = weather.count()
        logger.info("Weather hourly rows in Kyiv bbox: %d", weather_rows)

        if need_kpt:
            logger.info("Loading KPT traffic from source=%s", KPT_SOCKETIO_SOURCE)
            kpt_raw = load_kpt_traffic(spark, time_from, time_to)
            kpt_aggregated = aggregate_kpt_cells(kpt_raw).cache()
            logger.info("KPT aggregated cell-hour rows: %d", kpt_aggregated.count())

            kpt_joined = join_weather(kpt_aggregated, weather)
            kpt_rows = persist_parquet(kpt_joined, PATHS.kpt, author)
            logger.info("Wrote %s with %d rows", PATHS.kpt, kpt_rows)
        else:
            logger.info("Skipping KPT — %s already exists", PATHS.kpt)

        if need_tomtom:
            logger.info("Loading TomTom traffic via boto3+pyarrow from source=%s", TOMTOM_SOURCE)
            tomtom_pdf = load_tomtom_traffic_pandas(time_from, time_to)
            logger.info("TomTom rows after filter: %d", len(tomtom_pdf))
            weather_pdf = weather.toPandas()
            joined = tomtom_pdf.merge(weather_pdf, on="event_hour", how="inner")
            joined["author"] = author
            os.makedirs(os.path.dirname(PATHS.tomtom) or ".", exist_ok=True)
            joined.to_parquet(PATHS.tomtom, index=False)
            logger.info("Wrote %s with %d rows", PATHS.tomtom, len(joined))
        else:
            logger.info("Skipping TomTom — %s already exists", PATHS.tomtom)
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build local ML dataset from R2 silver layer")
    parser.add_argument("--time-from", default=DEFAULT_TIME_FROM)
    parser.add_argument("--time-to", default=DEFAULT_TIME_TO)
    parser.add_argument("--force", action="store_true", help="Rebuild even if outputs exist")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    args = parse_args()
    try:
        build_datasets(args.time_from, args.time_to, args.force)
    except Exception as exc:
        logger.exception("Dataset build failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
