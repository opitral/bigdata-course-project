import argparse
import logging
import os
import sys
import uuid
from typing import Iterable

from dotenv import load_dotenv

from gold.metrics import WEATHER_METRIC_REGISTRY
from gold.paths import silver_input_path
from gold.spark_context import create_gold_spark_session
from gold.writer import finalize_metric_frame, persist_metric


DOMAIN_METRIC_MAP = {
    "weather": WEATHER_METRIC_REGISTRY,
}

logger = logging.getLogger("gold.pipeline")


def select_metrics(domain: str, requested: Iterable[str] | None) -> dict:
    if domain not in DOMAIN_METRIC_MAP:
        raise ValueError(f"No gold metrics registered for domain '{domain}'")

    registry = DOMAIN_METRIC_MAP[domain]
    if not requested:
        return registry

    unknown = [name for name in requested if name not in registry]
    if unknown:
        raise ValueError(f"Unknown metrics requested: {unknown}")
    return {name: registry[name] for name in requested}


def run_gold_pipeline(domain: str, source: str, output_format: str, metric_names: Iterable[str] | None) -> None:
    author = os.environ.get("AUTHOR_SURNAME", "slobodian")
    run_id = uuid.uuid4().hex[:12]

    logger.info("Gold pipeline started | domain=%s source=%s run_id=%s author=%s", domain, source, run_id, author)

    metric_plan = select_metrics(domain, metric_names)
    logger.info("Metrics to build: %s", list(metric_plan.keys()))

    spark = create_gold_spark_session(f"gold-{domain}-{source}-{run_id}")

    try:
        read_path = silver_input_path(domain, source)
        logger.info("Reading Silver from: %s", read_path)
        silver_df = spark.read.parquet(read_path)
        silver_df = silver_df.filter(silver_df["author"] == author)
        silver_df.cache()
        row_count = silver_df.count()
        logger.info("Silver rows (author=%s): %d", author, row_count)

        if row_count == 0:
            logger.warning("No silver rows for author=%s — aborting without writes", author)
            return

        for metric_name, definition in metric_plan.items():
            logger.info("Computing metric '%s'", metric_name)
            computed = definition["compute"](silver_df)
            finalized = finalize_metric_frame(computed, author, run_id, metric_name)
            persist_metric(
                df=finalized,
                domain=domain,
                metric_name=metric_name,
                run_id=run_id,
                partition_cols=definition["partition_cols"],
                output_format=output_format,
            )

        logger.info("Gold pipeline complete | run_id=%s | metrics=%d", run_id, len(metric_plan))
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gold layer metrics pipeline")
    parser.add_argument("--domain", required=True, help="Data domain (e.g. weather)")
    parser.add_argument("--source", required=True, help="Data source (e.g. open_meteo_current)")
    parser.add_argument(
        "--output-format",
        choices=("parquet", "json"),
        default="parquet",
        help="Output file format (default: parquet)",
    )
    parser.add_argument(
        "--metrics",
        nargs="*",
        help="Subset of metrics to compute. If omitted, all domain metrics are built.",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    args = parse_args()

    if args.domain not in DOMAIN_METRIC_MAP:
        logger.error("Unsupported domain '%s' — known: %s", args.domain, list(DOMAIN_METRIC_MAP))
        sys.exit(1)

    try:
        run_gold_pipeline(args.domain, args.source, args.output_format, args.metrics)
    except Exception as exc:
        logger.exception("Gold pipeline failed: %s", exc)
        sys.exit(2)


if __name__ == "__main__":
    main()
