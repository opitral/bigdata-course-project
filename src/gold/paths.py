SCHEMA_VERSION = 1
BUCKET = "data"


def silver_input_path(domain: str, source: str) -> str:
    return (
        f"s3a://{BUCKET}/processed/silver"
        f"/domain={domain}"
        f"/source={source}"
        f"/schema_v={SCHEMA_VERSION}"
    )


def gold_output_path(domain: str, metric_name: str, run_id: str) -> str:
    return (
        f"s3a://{BUCKET}/processed/gold"
        f"/domain={domain}"
        f"/metric={metric_name}"
        f"/schema_v={SCHEMA_VERSION}"
        f"/run_id={run_id}"
    )
