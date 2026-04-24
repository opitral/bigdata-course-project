import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

from common.author import resolve_author
from common.logging_setup import configure_logging
from common.r2_storage import R2Settings, R2Storage
from bronze.partition import PartitionSlot, write_jsonl_to_disk
from bronze.record import utc_now
from bronze.sources import COLLECTORS, STATIC_DOMAINS


DEFAULT_INTERVAL_SECONDS = 600
DEFAULT_MAX_ITERATIONS = 0
DEFAULT_LOCAL_ROOT = "data_local"
DEFAULT_DOMAINS = ("weather", "traffic", "alerts", "roads")

logger = logging.getLogger("bronze")


@dataclass(frozen=True)
class IngestConfig:
    author: str
    interval_seconds: int
    max_iterations: int
    local_root: Path
    domains: List[str]
    upload_to_r2: bool

    @classmethod
    def from_env(cls) -> "IngestConfig":
        domains_env = os.environ.get("INGEST_DOMAINS", "")
        selected = [d.strip() for d in domains_env.split(",") if d.strip()] or list(DEFAULT_DOMAINS)
        unknown = [d for d in selected if d not in COLLECTORS]
        if unknown:
            raise RuntimeError(f"Unknown domain(s) in INGEST_DOMAINS: {', '.join(unknown)}")
        return cls(
            author=resolve_author(),
            interval_seconds=int(os.environ.get("INGEST_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS)),
            max_iterations=int(os.environ.get("INGEST_MAX_ITERATIONS", DEFAULT_MAX_ITERATIONS)),
            local_root=Path(os.environ.get("LOCAL_DATA_ROOT", DEFAULT_LOCAL_ROOT)).expanduser(),
            domains=selected,
            upload_to_r2=os.environ.get("SKIP_R2_UPLOAD", "").lower() not in ("1", "true", "yes"),
        )


_stop_requested = False


def _request_stop(signum, _frame):
    global _stop_requested
    _stop_requested = True
    logger.warning("Received signal %s — will stop after current iteration", signum)


def _install_signal_handlers() -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _request_stop)
        except (ValueError, OSError):
            pass


def _should_collect(domain: str, iteration: int, last_hour_per_domain: dict, current_hour: str) -> bool:
    if domain not in STATIC_DOMAINS:
        return True
    if iteration == 1:
        return True
    return last_hour_per_domain.get(domain) != current_hour


def _process_domain(
    domain: str,
    config: IngestConfig,
    storage: Optional[R2Storage],
    moment,
) -> None:
    spec = COLLECTORS[domain]
    source = str(spec["source"])
    collect = spec["collect"]

    logger.info("Collecting domain=%s source=%s", domain, source)
    records = collect(config.author)
    if not records:
        logger.warning("No records returned for domain=%s — skipping", domain)
        return

    slot = PartitionSlot.for_moment(domain, source, moment)
    local_file = write_jsonl_to_disk(config.local_root, slot, records)
    logger.info("Wrote %d records to %s", len(records), local_file)

    if storage is None:
        logger.info("Skipping R2 upload (SKIP_R2_UPLOAD set)")
        return

    uploaded_key = storage.upload_file(local_file, slot.relative_key, content_type="application/x-ndjson")
    logger.info("Uploaded → s3://%s/%s", storage.bucket, uploaded_key)


def run(config: IngestConfig) -> None:
    _install_signal_handlers()
    config.local_root.mkdir(parents=True, exist_ok=True)

    storage: Optional[R2Storage] = None
    if config.upload_to_r2:
        storage = R2Storage(R2Settings.from_env())
        logger.info("Uploads → s3://%s/%s/bronze/...", storage.bucket, storage.base_prefix)
    else:
        logger.info("R2 upload disabled — writing only to %s", config.local_root)

    logger.info(
        "Start: author=%s domains=%s interval=%ss max_iterations=%s",
        config.author,
        ",".join(config.domains),
        config.interval_seconds,
        config.max_iterations or "unlimited",
    )

    last_hour_per_domain: dict = {}
    iteration = 1
    while not _stop_requested:
        moment = utc_now()
        current_hour = moment.strftime("%H")
        logger.info("Iteration %d at %s UTC", iteration, moment.isoformat(timespec="seconds"))

        for domain in config.domains:
            if not _should_collect(domain, iteration, last_hour_per_domain, current_hour):
                logger.info("Skip domain=%s (static, already captured for hour=%s)", domain, current_hour)
                continue
            try:
                _process_domain(domain, config, storage, moment)
                last_hour_per_domain[domain] = current_hour
            except Exception as exc:
                logger.exception("Domain=%s failed: %s", domain, exc)

        iteration += 1
        if config.max_iterations and iteration > config.max_iterations:
            logger.info("Reached max_iterations=%d — stopping", config.max_iterations)
            break
        if _stop_requested:
            break

        sleep_seconds = config.interval_seconds
        logger.info("Sleeping %ds before next iteration", sleep_seconds)
        slept = 0
        while slept < sleep_seconds and not _stop_requested:
            step = min(5, sleep_seconds - slept)
            time.sleep(step)
            slept += step


def main() -> None:
    load_dotenv()
    configure_logging()
    try:
        config = IngestConfig.from_env()
    except RuntimeError as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(2)
    run(config)


if __name__ == "__main__":
    main()
