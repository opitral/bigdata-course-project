from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    lit,
)


def compute(silver_df: DataFrame) -> DataFrame:
    apparent_temp = (
        col("temperature_2m")
        + lit(0.33) * (col("relative_humidity_2m") / lit(100.0)) * lit(6.105)
        - lit(0.7) * col("wind_speed_10m")
        - lit(4.0)
    )

    enriched = (
        silver_df
        .withColumn("window_date", date_format(col("event_ts"), "yyyy-MM-dd"))
        .withColumn("window_hour", date_format(col("event_ts"), "HH"))
        .withColumn("apparent_temperature_c", apparent_temp)
    )

    aggregated = (
        enriched
        .groupBy("window_date", "window_hour", "city")
        .agg(
            count("*").alias("samples_cnt"),
            avg("apparent_temperature_c").alias("comfort_index_c"),
            avg("temperature_2m").alias("temperature_avg_c"),
            avg("relative_humidity_2m").alias("humidity_avg_pct"),
            avg("wind_speed_10m").alias("wind_speed_avg_ms"),
        )
        .filter(col("city").isNotNull())
    )

    return aggregated
