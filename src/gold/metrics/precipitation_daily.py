from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    date_format,
    hour,
    sum as spark_sum,
    when,
)


RAIN_THRESHOLD_MM = 0.1


def compute(silver_df: DataFrame) -> DataFrame:
    with_window = (
        silver_df
        .withColumn("window_date", date_format(col("event_ts"), "yyyy-MM-dd"))
        .withColumn("sample_hour", hour(col("event_ts")))
    )

    hourly_precip = (
        with_window
        .groupBy("window_date", "city", "sample_hour")
        .agg(spark_sum("precipitation").alias("hour_precip_mm"))
    )

    daily = (
        hourly_precip
        .groupBy("window_date", "city")
        .agg(
            spark_sum("hour_precip_mm").alias("total_precip_mm"),
            count("*").alias("hours_observed"),
            spark_sum(when(col("hour_precip_mm") >= RAIN_THRESHOLD_MM, 1).otherwise(0)).alias("rainy_hours"),
        )
        .filter(col("city").isNotNull())
    )

    return daily
