
from echodataflow.flows import flows_simulation
from echodataflow.operations.operations_storage import (
    S3CopyResult,
    S3CopySettings,
    S3CopyWorkItem,
)


class FakePaginator:
    def paginate(self, *, Bucket, Prefix):
        assert Bucket == "trawl-bucket"
        return [
            {
                "Contents": [
                    {"Key": f"{Prefix}catch_001_data.xlsx"},
                    {"Key": f"{Prefix}catch_002_data.xlsx"},
                ]
            }
        ]


class FakeS3Client:
    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return FakePaginator()


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


def test_flow_copy_trawl_uses_shared_s3_copy_contract(monkeypatch, tmp_path):
    fake_task = FakeTask()
    monkeypatch.setattr(
        flows_simulation.boto3,
        "client",
        lambda *_args, **_kwargs: FakeS3Client(),
    )
    monkeypatch.setattr(flows_simulation, "task_copy_s3_file", fake_task)
    monkeypatch.setattr(flows_simulation, "Variable", FakeVariable)

    results = flows_simulation.flow_copy_trawl.fn(
        path_copy=str(tmp_path),
        s3_bucket="trawl-bucket",
        s3_prefix="survey",
        trawl_folders=["CatchPercentages"],
        start_trawl_num=1,
    )

    expected_path = tmp_path / "CatchPercentages" / "catch_001_data.xlsx"
    assert fake_task.calls == [
        (
            S3CopyWorkItem(
                s3_path="survey/CatchPercentages/catch_001_data.xlsx",
                local_path=str(expected_path),
            ),
            S3CopySettings(
                s3_bucket="trawl-bucket",
                endpoint_url="https://sdsc.osn.xsede.org",
            ),
        )
    ]
    assert results == [
        S3CopyResult(
            s3_path="survey/CatchPercentages/catch_001_data.xlsx",
            local_path=str(expected_path),
        )
    ]
