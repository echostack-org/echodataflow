import datetime

import pandas as pd

from echodataflow.flows import flows_simulation
from echodataflow.operations.operations_storage import (
    S3CopyResult,
    S3CopySettings,
    S3CopyWorkItem,
)


class FakeTask:
    def __init__(self):
        self.calls = []

    def with_options(self, **_options):
        return self

    def __call__(self, item, settings):
        self.calls.append((item, settings))
        return S3CopyResult(
            s3_path=item.s3_path,
            local_path=item.local_path,
        )


class FakeVariable:
    stored = {}

    @classmethod
    def get(cls, key, default=None):
        return cls.stored.get(key, default)

    @classmethod
    def set(cls, key, value, overwrite=False):
        assert overwrite is True
        cls.stored[key] = value


class FakeDateTime(datetime.datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2024, 7, 7, 0, 30, tzinfo=datetime.timezone.utc)


def test_flow_copy_raw_simulates_new_file_arrivals(monkeypatch, tmp_path):
    FakeVariable.stored = {}
    fake_task = FakeTask()

    raw_list = tmp_path / "raw_files.csv"
    pd.DataFrame(
        {
            "timestamp": [
                "2024-07-07T00:10:00Z",
                "2024-07-07T00:20:00Z",
                "2024-07-07T00:40:00Z",
            ],
            "s3_path": [
                "survey/first.raw",
                "survey/second.raw",
                "survey/future.raw",
            ],
        }
    ).to_csv(raw_list, index=False)

    monkeypatch.setattr(flows_simulation, "Variable", FakeVariable)
    monkeypatch.setattr(flows_simulation, "task_copy_s3_file", fake_task)
    monkeypatch.setattr(flows_simulation.datetime, "datetime", FakeDateTime)

    output_directory = tmp_path / "raw"

    results = flows_simulation.flow_copy_raw.fn(
        path_raw_list=str(raw_list),
        path_copy=str(output_directory),
        s3_bucket="raw-bucket",
    )

    assert fake_task.calls == [
        (
            S3CopyWorkItem(
                s3_path="survey/first.raw",
                local_path=str(output_directory / "first.raw"),
            ),
            S3CopySettings(
                s3_bucket="raw-bucket",
                endpoint_url="https://sdsc.osn.xsede.org",
            ),
        ),
        (
            S3CopyWorkItem(
                s3_path="survey/second.raw",
                local_path=str(output_directory / "second.raw"),
            ),
            S3CopySettings(
                s3_bucket="raw-bucket",
                endpoint_url="https://sdsc.osn.xsede.org",
            ),
        ),
    ]

    assert results == [
        S3CopyResult(
            s3_path="survey/first.raw",
            local_path=str(output_directory / "first.raw"),
        ),
        S3CopyResult(
            s3_path="survey/second.raw",
            local_path=str(output_directory / "second.raw"),
        ),
    ]

    assert any(
        key.startswith("prev_start_time_")
        for key in FakeVariable.stored
    )


def test_flow_copy_raw_updates_watermark_when_no_files_selected(
    monkeypatch,
    tmp_path,
):
    FakeVariable.stored = {}

    raw_list = tmp_path / "raw_files.csv"
    pd.DataFrame(
        {
            "timestamp": [
                "2024-07-07T00:40:00Z",
            ],
            "s3_path": [
                "survey/future.raw",
            ],
        }
    ).to_csv(raw_list, index=False)

    monkeypatch.setattr(flows_simulation, "Variable", FakeVariable)
    monkeypatch.setattr(flows_simulation.datetime, "datetime", FakeDateTime)

    results = flows_simulation.flow_copy_raw.fn(
        path_raw_list=str(raw_list),
        path_copy=str(tmp_path / "raw"),
        s3_bucket="raw-bucket",
    )

    assert results == []

    watermark_keys = [
        key
        for key in FakeVariable.stored
        if key.startswith("prev_start_time_")
    ]

    assert len(watermark_keys) == 1
    assert FakeVariable.stored[watermark_keys[0]] == (
        "2024-07-07T00:30:00+00:00"
    )