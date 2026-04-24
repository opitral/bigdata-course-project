import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp


EPOCH_LOWER_BOUND = "1971-01-01 00:00:00"


def build_spark_session(app_name: str) -> SparkSession:
    account_id = os.environ["R2_ACCOUNT_ID"]
    access_key = os.environ["R2_ACCESS_KEY_ID"]
    secret_key = os.environ["R2_SECRET_ACCESS_KEY"]
    endpoint = f"https://{account_id}.r2.cloudflarestorage.com"

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint.region", "auto")
        .config("spark.hadoop.fs.s3a.multiregion.enabled", "false")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


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
