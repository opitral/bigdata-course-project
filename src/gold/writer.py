import logging
from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from common.constants import SCHEMA_VERSION
from common.paths import gold_write_uri


logger = logging.getLogger("gold.writer")


def finalize_metric_frame(df: DataFrame, author: str, run_id: str, metric_name: str) -> DataFrame:
    return (
        df
        .withColumn("author", lit(author))
        .withColumn("schema_v", lit(SCHEMA_VERSION))
        .withColumn("run_id", lit(run_id))
        .withColumn("metric_name", lit(metric_name))
    )


def persist_metric(
    df: DataFrame,
    domain: str,
    metric_name: str,
    run_id: str,
    partition_cols: Sequence[str],
    output_format: str,
    repartition_size: int = 4,
) -> str:
    write_path = gold_write_uri(domain, metric_name, run_id)
    partitioned = df.repartition(repartition_size, *partition_cols)

    writer = partitioned.write.mode("append").partitionBy(*partition_cols)
    if output_format == "parquet":
        writer.parquet(write_path)
    else:
        writer.json(write_path)

    logger.info("Metric '%s' written to %s (partitioned by %s)", metric_name, write_path, ", ".join(partition_cols))
    return write_path
