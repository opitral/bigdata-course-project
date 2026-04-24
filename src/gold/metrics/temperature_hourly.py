from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    hour,
    max as spark_max,
    min as spark_min,
    stddev_pop,
    to_date,
)


def compute(silver_df: DataFrame) -> DataFrame:
    with_window = (
        silver_df
        .withColumn("window_date", date_format(col("event_ts"), "yyyy-MM-dd"))
        .withColumn("window_hour", date_format(col("event_ts"), "HH"))
    )

    aggregated = (
        with_window
        .groupBy("window_date", "window_hour", "city")
        .agg(
            count("*").alias("samples_cnt"),
            avg("temperature_2m").alias("temperature_avg_c"),
            spark_min("temperature_2m").alias("temperature_min_c"),
            spark_max("temperature_2m").alias("temperature_max_c"),
            stddev_pop("temperature_2m").alias("temperature_stddev_c"),
        )
        .filter(col("city").isNotNull())
    )

    return aggregated
