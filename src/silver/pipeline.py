import argparse
import logging
import sys
import uuid

from dotenv import load_dotenv

from common.author import resolve_author
from common.paths import bronze_read_uri, silver_write_uri
from common.spark import build_spark_session
from silver.transforms.common import (
    apply_repartition,
    enrich_with_author,
    reject_invalid_timestamps,
)
from silver.transforms.weather import (
    apply_weather_domain_filters,
    drop_duplicate_events,
    flatten_weather_payload,
    reject_null_fields,
)

logger = logging.getLogger("silver")


def run_weather_pipeline(domain: str, source: str, output_format: str) -> None:
    author = resolve_author()
    run_id = uuid.uuid4().hex[:12]

    logger.info(
        "Starting silver pipeline: domain=%s source=%s run_id=%s author=%s",
        domain,
        source,
        run_id,
        author,
    )

    spark = build_spark_session(f"silver-{domain}-{source}")

    try:
        read_path = bronze_read_uri(domain, source)
        logger.info("Reading bronze from: %s", read_path)

        raw_df = spark.read.option("mode", "PERMISSIVE").json(read_path)
        total_raw = raw_df.count()
        logger.info("Bronze records read: %d", total_raw)

        enriched_df = enrich_with_author(raw_df, author)
        flattened_df = flatten_weather_payload(enriched_df)
        non_null_df = reject_null_fields(flattened_df)
        domain_filtered_df = apply_weather_domain_filters(non_null_df, author)
        time_filtered_df = reject_invalid_timestamps(domain_filtered_df, "event_ts")
        deduped_df = drop_duplicate_events(time_filtered_df)

        valid_count = deduped_df.count()
        rejected_count = total_raw - valid_count
        logger.info("Valid records: %d | Rejected: %d", valid_count, rejected_count)

        if valid_count == 0:
            logger.warning("No valid records after filtering — skipping write for run_id=%s", run_id)
            return

        write_path = silver_write_uri(domain, source, run_id)
        output_df = apply_repartition(deduped_df, 8)

        if output_format == "parquet":
            output_df.write.mode("append").parquet(write_path)
        else:
            output_df.write.mode("append").json(write_path)

        logger.info("Written %d records to: %s", valid_count, write_path)

    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Silver layer pipeline")
    parser.add_argument("--domain", required=True, help="Data domain (e.g. weather)")
    parser.add_argument("--source", required=True, help="Data source (e.g. open_meteo_current)")
    parser.add_argument(
        "--output-format",
        choices=("parquet", "json"),
        default="parquet",
        help="Output file format (default: parquet)",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    args = parse_args()

    if args.domain != "weather":
        logger.error("This pipeline only supports domain=weather, got: %s", args.domain)
        sys.exit(1)

    run_weather_pipeline(args.domain, args.source, args.output_format)


if __name__ == "__main__":
    main()
