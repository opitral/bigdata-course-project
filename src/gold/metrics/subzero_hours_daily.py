from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    date_format,
    hour,
    min as spark_min,
    when,
)


def compute(silver_df: DataFrame) -> DataFrame:
    with_window = (
        silver_df
        .withColumn("window_date", date_format(col("event_ts"), "yyyy-MM-dd"))
        .withColumn("sample_hour", hour(col("event_ts")))
    )

    hourly_min = (
        with_window
        .groupBy("window_date", "city", "sample_hour")
        .agg(spark_min("temperature_2m").alias("hour_min_temp_c"))
    )

    daily = (
        hourly_min
        .groupBy("window_date", "city")
        .agg(
            count("*").alias("hours_observed"),
            spark_min("hour_min_temp_c").alias("coldest_hour_temp_c"),
            avg("hour_min_temp_c").alias("avg_hourly_min_temp_c"),
            count(when(col("hour_min_temp_c") < 0.0, True)).alias("subzero_hours"),
        )
        .filter(col("city").isNotNull())
    )

    return daily
