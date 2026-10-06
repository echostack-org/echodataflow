from pathlib import Path

from echodataflow.operations.operations_storage import (
    S3CopyResult,
    S3CopySettings,
    S3CopyWorkItem,
    copy_s3_file,
)


class FakeS3Client:
    def __init__(self, content: bytes = b"raw sonar data"):
        self.content = content
        self.calls = []

    def download_file(self, bucket, key, destination):
        self.calls.append((bucket, key, destination))
        Path(destination).write_bytes(self.content)


def test_copy_raw_file_downloads_to_configured_directory(tmp_path):
    s3_client = FakeS3Client()
    expected_path = tmp_path / "raw" / "example.raw"
    item = S3CopyWorkItem(
        s3_path="survey/leg1/example.raw",
        local_path=str(expected_path),
    )
    settings = S3CopySettings(s3_bucket="source-bucket")

    result = copy_s3_file(item, settings, s3_client=s3_client)

    assert result == S3CopyResult(
        s3_path="survey/leg1/example.raw",
        local_path=str(expected_path),
    )
    assert expected_path.read_bytes() == b"raw sonar data"
    assert s3_client.calls == [
        ("source-bucket", "survey/leg1/example.raw", expected_path)
    ]
