"""Tasks that join reusable storage and acoustic operations."""

from prefect import task

from echodataflow.operations.operations_acoustics import (
    RawToSvResult,
    RawToSvSettings,
    RawToSvWorkItem,
    convert_raw_to_Sv,
)
from echodataflow.operations.operations_storage import (
    S3CopySettings,
    S3CopyWorkItem,
    copy_s3_file,
)


@task(log_prints=True)
def task_s3_raw2Sv(
    copy_item: S3CopyWorkItem,
    copy_settings: S3CopySettings,
    sv_settings: RawToSvSettings,
) -> RawToSvResult:
    """Download one raw object and immediately convert it to Sv."""
    # Keep staging and conversion in one task so each completed future is durable work
    copied = copy_s3_file(copy_item, copy_settings)
    return convert_raw_to_Sv(
        RawToSvWorkItem(raw_path=copied.local_path),
        sv_settings,
    )
