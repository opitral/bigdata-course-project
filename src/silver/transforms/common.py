from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp


EPOCH_LOWER_BOUND = "1971-01-01 00:00:00"


def reject_invalid_timestamps(df: DataFrame, ts_col: str) -> DataFrame:
    lower = to_timestamp(lit(EPOCH_LOWER_BOUND))
    return df.filter(
        col(ts_col).isNotNull()
        & (col(ts_col) > lower)
        & (col(ts_col) <= current_timestamp())
    )


def enrich_with_author(df: DataFrame, author: str) -> DataFrame:
    return df.withColumn("author", lit(author))


def apply_repartition(df: DataFrame, partitions: int = 8) -> DataFrame:
    return df.repartition(partitions)
