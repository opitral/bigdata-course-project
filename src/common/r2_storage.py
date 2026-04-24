import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config


@dataclass(frozen=True)
class R2Settings:
    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket: str
    base_prefix: str

    @classmethod
    def from_env(cls) -> "R2Settings":
        missing = [
            key
            for key in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")
            if not os.environ.get(key)
        ]
        if missing:
            raise RuntimeError(f"Missing required R2 env vars: {', '.join(missing)}")
        return cls(
            account_id=os.environ["R2_ACCOUNT_ID"],
            access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            bucket=os.environ.get("R2_BUCKET", "data"),
            base_prefix=os.environ.get("R2_BASE_PREFIX", "raw").strip().strip("/"),
        )


class R2Storage:
    def __init__(self, settings: R2Settings):
        self._settings = settings
        self._client = boto3.client(
            "s3",
            endpoint_url=f"https://{settings.account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=settings.access_key_id,
            aws_secret_access_key=settings.secret_access_key,
            region_name="auto",
            config=Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"}),
        )

    @property
    def bucket(self) -> str:
        return self._settings.bucket

    @property
    def base_prefix(self) -> str:
        return self._settings.base_prefix

    def qualified_key(self, key: str) -> str:
        key = key.lstrip("/")
        if self._settings.base_prefix:
            return f"{self._settings.base_prefix}/{key}"
        return key

    def upload_file(self, local_path: Path, key: str, content_type: Optional[str] = None) -> str:
        full_key = self.qualified_key(key)
        extra = {"ContentType": content_type} if content_type else {}
        with local_path.open("rb") as handle:
            self._client.put_object(
                Bucket=self._settings.bucket,
                Key=full_key,
                Body=handle.read(),
                **extra,
            )
        return full_key

    def iter_pages(self, key_prefix: str):
        prefix = self.qualified_key(key_prefix)
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._settings.bucket, Prefix=prefix):
            yield page.get("Contents", [])

    def list_objects(self, key_prefix: str):
        for batch in self.iter_pages(key_prefix):
            for obj in batch:
                yield obj

    def fetch_range(self, key: str, byte_range: str) -> bytes:
        response = self._client.get_object(
            Bucket=self._settings.bucket,
            Key=key,
            Range=byte_range,
        )
        return response["Body"].read()
