from common.constants import (
    BRONZE_SEGMENT,
    BUCKET,
    GOLD_SEGMENT,
    PROCESSED_PREFIX,
    RAW_PREFIX,
    SCHEMA_VERSION,
    SILVER_SEGMENT,
)


def bronze_partition_key(domain: str, source: str, ingest_date: str, hour: str) -> str:
    return (
        f"{BRONZE_SEGMENT}/domain={domain}"
        f"/source={source}"
        f"/schema_v={SCHEMA_VERSION}"
        f"/ingest_date={ingest_date}"
        f"/hour={hour}"
    )


def bronze_read_uri(domain: str, source: str) -> str:
    return (
        f"s3a://{BUCKET}/{RAW_PREFIX}/{BRONZE_SEGMENT}"
        f"/domain={domain}"
        f"/source={source}"
        f"/schema_v={SCHEMA_VERSION}"
        "/ingest_date=*"
        "/hour=*"
    )


def silver_write_uri(domain: str, source: str, run_id: str) -> str:
    return (
        f"s3a://{BUCKET}/{PROCESSED_PREFIX}/{SILVER_SEGMENT}"
        f"/domain={domain}"
        f"/source={source}"
        f"/schema_v={SCHEMA_VERSION}"
        f"/run_id={run_id}"
    )


def silver_read_uri(domain: str, source: str) -> str:
    return (
        f"s3a://{BUCKET}/{PROCESSED_PREFIX}/{SILVER_SEGMENT}"
        f"/domain={domain}"
        f"/source={source}"
        f"/schema_v={SCHEMA_VERSION}"
    )


def gold_write_uri(domain: str, metric_name: str, run_id: str) -> str:
    return (
        f"s3a://{BUCKET}/{PROCESSED_PREFIX}/{GOLD_SEGMENT}"
        f"/domain={domain}"
        f"/metric={metric_name}"
        f"/schema_v={SCHEMA_VERSION}"
        f"/run_id={run_id}"
    )
