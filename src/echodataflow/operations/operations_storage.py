"""Reusable operations for moving data between storage systems."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore import UNSIGNED
from botocore.config import Config


@dataclass(frozen=True)
class S3CopyWorkItem:
    """One remote S3 object and its exact local destination."""

    s3_path: str
    local_path: str


@dataclass(frozen=True)
class S3CopySettings:
    """S3 connection settings shared by a batch of file copies."""

    s3_bucket: str
    endpoint_url: str | None = None


@dataclass(frozen=True)
class S3CopyResult:
    """Locations associated with one successfully copied S3 object."""

    s3_path: str
    local_path: str


def copy_s3_file(
    item: S3CopyWorkItem,
    settings: S3CopySettings,
    *,
    s3_client: Any | None = None,
) -> S3CopyResult:
    """Download one S3 object to its specified local path."""
    local_path = Path(item.local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # OSN data is public, so the default client uses unsigned requests
    if s3_client is None:
        s3_client = boto3.client(
            "s3",
            endpoint_url=settings.endpoint_url,
            config=Config(signature_version=UNSIGNED),
        )

    # Download to the exact staging path supplied by the caller
    s3_client.download_file(
        settings.s3_bucket,
        item.s3_path,
        local_path,
    )

    return S3CopyResult(
        s3_path=item.s3_path,
        local_path=str(local_path),
    )
