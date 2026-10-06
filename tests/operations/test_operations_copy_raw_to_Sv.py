from pathlib import Path
from types import SimpleNamespace

from echodataflow.operations import operations_acoustics
from echodataflow.operations.operations_storage import (
    S3CopySettings,
    S3CopyWorkItem,
    copy_s3_file,
)


class FakeS3Client:
    def download_file(self, _bucket, _key, destination):
        Path(destination).write_bytes(b"raw sonar data")


class FakeDataset:
    def __getitem__(self, key):
        assert key == "ping_time"
        return [
            SimpleNamespace(values="2026-01-01T00:00:00"),
            SimpleNamespace(values="2026-01-01T00:01:00"),
        ]

    def to_zarr(self, **kwargs):
        Path(kwargs["store"]).mkdir(parents=True)


def test_copied_raw_file_can_be_converted_to_Sv(monkeypatch, tmp_path):
    copy_result = copy_s3_file(
        S3CopyWorkItem(
            s3_path="survey/example.raw",
            local_path=str(tmp_path / "raw" / "example.raw"),
        ),
        S3CopySettings(s3_bucket="source-bucket"),
        s3_client=FakeS3Client(),
    )

    dataset = FakeDataset()
    echodata = {
        "Platform": SimpleNamespace(
            drop_duplicates=lambda _dimension: "deduplicated"
        )
    }
    monkeypatch.setattr(operations_acoustics.ep, "open_raw", lambda **_kwargs: echodata)
    monkeypatch.setattr(
        operations_acoustics.ep.calibrate,
        "compute_Sv",
        lambda **_kwargs: dataset,
    )
    monkeypatch.setattr(
        operations_acoustics.ep.consolidate,
        "add_depth",
        lambda **kwargs: kwargs["ds"],
    )
    monkeypatch.setattr(
        operations_acoustics.ep.consolidate,
        "add_location",
        lambda **kwargs: kwargs["ds"],
    )

    sv_result = operations_acoustics.convert_raw_to_Sv(
        operations_acoustics.RawToSvWorkItem(raw_path=copy_result.local_path),
        operations_acoustics.RawToSvSettings(
            output_directory=str(tmp_path / "Sv")
        ),
    )

    assert sv_result.filename_raw == "example.raw"
    assert sv_result.filename_Sv == "example_Sv.zarr"
    assert (tmp_path / "Sv" / "example_Sv.zarr").is_dir()
