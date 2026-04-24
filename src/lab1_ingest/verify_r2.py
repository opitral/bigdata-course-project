import json
import logging
import sys
from collections import defaultdict

from dotenv import load_dotenv

from common.logging_setup import configure_logging
from common.r2_storage import R2Settings, R2Storage

logger = logging.getLogger("lab1_verify")


def main() -> None:
    load_dotenv()
    configure_logging()
    storage = R2Storage(R2Settings.from_env())
    prefix = storage.qualified_key("bronze/")
    logger.info("Listing s3://%s/%s", storage.bucket, prefix)

    hours_by_domain: dict = defaultdict(set)
    total_bytes = 0
    total_objects = 0
    sample_key = None
    page_no = 0

    for batch in storage.iter_pages("bronze/"):
        page_no += 1
        logger.info("Page %d: %d objects", page_no, len(batch))
        for obj in batch:
            key = obj["Key"]
            size = obj["Size"]
            total_objects += 1
            total_bytes += size
            if sample_key is None and key.endswith(".jsonl"):
                sample_key = key
            parts = {
                piece.split("=", 1)[0]: piece.split("=", 1)[1]
                for piece in key.split("/")
                if "=" in piece
            }
            if "domain" in parts and "ingest_date" in parts and "hour" in parts:
                hours_by_domain[parts["domain"]].add(
                    f"{parts['ingest_date']} {parts['hour']}:00"
                )

    logger.info("Listing complete: pages=%d total_objects=%d total_bytes=%s", page_no, total_objects, f"{total_bytes:,}")

    if total_objects == 0:
        logger.warning("Bucket prefix is empty — nothing to verify yet")
        sys.exit(1)

    for domain in sorted(hours_by_domain):
        hours = sorted(hours_by_domain[domain])
        logger.info("Domain=%s hours=%d partitions=%s", domain, len(hours), hours)

    if sample_key:
        logger.info("Sample object: s3://%s/%s", storage.bucket, sample_key)
        head_bytes = storage.fetch_range(sample_key, "bytes=0-4095")
        raw = head_bytes.decode("utf-8", errors="replace")
        first_line = raw.splitlines()[0] if raw else ""
        try:
            parsed = json.loads(first_line)
            logger.info("First record top-level keys: %s", sorted(parsed.keys()))
            logger.info("author=%s schema_v=%s event_id=%s", parsed.get("author"), parsed.get("schema_v"), parsed.get("event_id"))
        except json.JSONDecodeError as exc:
            logger.warning("Could not parse first line as JSON: %s", exc)

    two_hour_domains = [d for d, hours in hours_by_domain.items() if len(hours) >= 2]
    if two_hour_domains:
        logger.info("OK: data across >=2 hours for domains=%s", two_hour_domains)
    else:
        logger.warning("No domain has data across 2+ distinct hours yet — keep the ingest running")
        sys.exit(1)


if __name__ == "__main__":
    main()
