from types import SimpleNamespace

import pandas as pd

from echodataflow.operations import operations_acoustics


class FakeSvDataset:
    def __init__(self):
        self.chunks = None
        self.selection = None
        self.sizes = {"ping_time": 2}

    def chunk(self, chunks):
        self.chunks = chunks
        return self

    def sel(self, **kwargs):
        self.selection = kwargs
        return self


class FakeMVBSDataset:
    def __init__(self):
        self.chunks = None
        self.saved = None

    def __getitem__(self, key):
        assert key == "ping_time"
        return [
            SimpleNamespace(values="2026-01-01T00:00:05"),
            SimpleNamespace(values="2026-01-01T00:09:55"),
        ]

    def chunk(self, chunks):
        self.chunks = chunks
        return self

    def to_zarr(self, **kwargs):
        self.saved = kwargs


def test_create_MVBS_processes_one_time_slice(monkeypatch, tmp_path):
    sv_dataset = FakeSvDataset()
    mvbs_dataset = FakeMVBSDataset()
    open_call = {}
    compute_call = {}

    def fake_open_mfdataset(paths, **kwargs):
        open_call.update(paths=paths, kwargs=kwargs)
        return sv_dataset

    def fake_compute_MVBS(**kwargs):
        compute_call.update(kwargs)
        return mvbs_dataset

    monkeypatch.setattr(
        operations_acoustics.xr,
        "open_mfdataset",
        fake_open_mfdataset,
    )
    monkeypatch.setattr(
        operations_acoustics.ep.commongrid,
        "compute_MVBS",
        fake_compute_MVBS,
    )

    item = operations_acoustics.CreateMVBSWorkItem(
        start_time=pd.Timestamp("2026-01-01T00:00:00Z"),
        end_time=pd.Timestamp("2026-01-01T00:10:00Z"),
        sv_filenames=("first_Sv.zarr", "second_Sv.zarr"),
        mvbs_filename="MVBS_20260101T000000.zarr",
    )
    settings = operations_acoustics.CreateMVBSSettings(
        sv_directory=str(tmp_path / "Sv"),
        output_directory=str(tmp_path / "MVBS"),
        range_bin="1m",
        ping_time_bin="5s",
    )

    result = operations_acoustics.create_MVBS(item, settings)

    assert open_call == {
        "paths": [
            tmp_path / "Sv" / "first_Sv.zarr",
            tmp_path / "Sv" / "second_Sv.zarr",
        ],
        "kwargs": {
            "parallel": True,
            "coords": "minimal",
            "data_vars": "minimal",
            "compat": "override",
            "engine": "zarr",
            "preprocess": operations_acoustics._clean_reversed_ping_time,
        },
    }
    assert sv_dataset.chunks == {
        "channel": 1,
        "ping_time": 1000,
        "range_sample": -1,
    }
    assert sv_dataset.selection == {
        "ping_time": slice(
            pd.Timestamp("2026-01-01T00:00:00"),
            pd.Timestamp("2026-01-01T00:10:00") - pd.to_timedelta("1nanoseconds"),
        )
    }
    assert compute_call["ds_Sv"] is sv_dataset
    assert compute_call["range_bin"] == "1m"
    assert compute_call["ping_time_bin"] == "5s"
    assert mvbs_dataset.chunks == {"channel": -1, "ping_time": -1, "depth": -1}
    assert mvbs_dataset.saved == {
        "store": tmp_path / "MVBS" / "MVBS_20260101T000000.zarr",
        "mode": "w",
        "consolidated": True,
    }
    assert result == operations_acoustics.CreateMVBSResult(
        mvbs_filename="MVBS_20260101T000000.zarr",
        first_ping_time=pd.Timestamp("2026-01-01T00:00:05Z"),
        last_ping_time=pd.Timestamp("2026-01-01T00:09:55Z"),
    )


def test_create_MVBS_returns_no_data_before_computation(monkeypatch, tmp_path):
    sv_dataset = FakeSvDataset()
    sv_dataset.sizes = {"ping_time": 0}
    compute_called = False

    monkeypatch.setattr(
        operations_acoustics.xr,
        "open_mfdataset",
        lambda *args, **kwargs: sv_dataset,
    )

    def fake_compute_MVBS(**kwargs):
        nonlocal compute_called
        compute_called = True

    monkeypatch.setattr(
        operations_acoustics.ep.commongrid,
        "compute_MVBS",
        fake_compute_MVBS,
    )

    result = operations_acoustics.create_MVBS(
        operations_acoustics.CreateMVBSWorkItem(
            start_time=pd.Timestamp("2026-01-01T04:20:00Z"),
            end_time=pd.Timestamp("2026-01-01T04:40:00Z"),
            sv_filenames=("last_daytime_Sv.zarr",),
            mvbs_filename="MVBS_20260101T042000.zarr",
        ),
        operations_acoustics.CreateMVBSSettings(
            sv_directory=str(tmp_path / "Sv"),
            output_directory=str(tmp_path / "MVBS"),
            range_bin="1m",
            ping_time_bin="5s",
        ),
    )

    assert not compute_called
    assert result == operations_acoustics.CreateMVBSResult(
        mvbs_filename="MVBS_20260101T042000.zarr",
        first_ping_time=None,
        last_ping_time=None,
        has_data=False,
    )


def test_clean_reversed_ping_time_coerces_only_reversed_datasets(monkeypatch):
    reversed_dataset = object()
    increasing_dataset = object()
    coerced = []

    monkeypatch.setattr(
        operations_acoustics,
        "exist_reversed_time",
        lambda dataset, coordinate: dataset is reversed_dataset and coordinate == "ping_time",
    )
    monkeypatch.setattr(
        operations_acoustics,
        "coerce_increasing_time",
        lambda dataset: coerced.append(dataset),
    )

    assert operations_acoustics._clean_reversed_ping_time(reversed_dataset) is reversed_dataset
    assert operations_acoustics._clean_reversed_ping_time(increasing_dataset) is increasing_dataset
    assert coerced == [reversed_dataset]