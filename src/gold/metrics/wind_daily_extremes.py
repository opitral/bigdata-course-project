import math

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    atan2,
    avg,
    col,
    cos,
    date_format,
    lit,
    max as spark_max,
    radians,
    sin,
    sqrt,
    when,
)


def compute(silver_df: DataFrame) -> DataFrame:
    with_window = (
        silver_df
        .withColumn("window_date", date_format(col("event_ts"), "yyyy-MM-dd"))
        .withColumn("wind_dir_rad", radians(col("wind_direction_10m")))
        .withColumn("wind_u", -col("wind_speed_10m") * sin(col("wind_dir_rad")))
        .withColumn("wind_v", -col("wind_speed_10m") * cos(col("wind_dir_rad")))
    )

    daily = (
        with_window
        .groupBy("window_date", "city")
        .agg(
            spark_max("wind_speed_10m").alias("wind_speed_max_ms"),
            avg("wind_speed_10m").alias("wind_speed_avg_ms"),
            avg("wind_u").alias("mean_u"),
            avg("wind_v").alias("mean_v"),
        )
    )

    deg_per_rad = lit(180.0 / math.pi)
    prevailing_raw = (atan2(-col("mean_u"), -col("mean_v")) * deg_per_rad)
    prevailing_dir = when(prevailing_raw < 0, prevailing_raw + lit(360.0)).otherwise(prevailing_raw)

    result = (
        daily
        .withColumn("prevailing_wind_deg", prevailing_dir)
        .withColumn(
            "mean_wind_vector_ms",
            sqrt(col("mean_u") * col("mean_u") + col("mean_v") * col("mean_v")),
        )
        .drop("mean_u", "mean_v")
        .filter(col("city").isNotNull())
    )

    return result
