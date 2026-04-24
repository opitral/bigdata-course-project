import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from common.paths import bronze_partition_key


@dataclass(frozen=True)
class PartitionSlot:
    domain: str
    source: str
    ingest_date: str
    hour: str
    part_stamp: str

    @classmethod
    def for_moment(cls, domain: str, source: str, moment: dt.datetime) -> "PartitionSlot":
        utc = moment.astimezone(dt.timezone.utc)
        return cls(
            domain=domain,
            source=source,
            ingest_date=utc.strftime("%Y-%m-%d"),
            hour=utc.strftime("%H"),
            part_stamp=utc.strftime("%Y%m%dT%H%M%SZ"),
        )

    @property
    def relative_dir(self) -> str:
        return bronze_partition_key(self.domain, self.source, self.ingest_date, self.hour)

    @property
    def file_name(self) -> str:
        return f"part-{self.part_stamp}.jsonl"

    @property
    def relative_key(self) -> str:
        return f"{self.relative_dir}/{self.file_name}"


def write_jsonl_to_disk(
    local_root: Path,
    slot: PartitionSlot,
    records: Iterable[Mapping],
) -> Path:
    target_dir = local_root / slot.relative_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / slot.file_name
    with target_file.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")
    return target_file
