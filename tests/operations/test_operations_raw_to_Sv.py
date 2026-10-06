from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from echodataflow.operations import operations_acoustics


class FakeDataset:
    def __init__(self):
        self.saved = None

    def __getitem__(self, key):
        assert key == "ping_time"
        return [
            SimpleNamespace(values="2026-01-01T00:00:00"),
            SimpleNamespace(values="2026-01-01T00:01:00"),
        ]

    def to_zarr(self, **kwargs):
        self.saved = kwargs


def test_convert_raw_to_Sv_returns_structured_result(monkeypatch, tmp_path):
    dataset = FakeDataset()
    platform = SimpleNamespace(drop_duplicates=lambda _dimension: "deduplicated")
    echodata = {"Platform": platform}

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

    result = operations_acoustics.convert_raw_to_Sv(
        operations_acoustics.RawToSvWorkItem(raw_path="/input/example.raw"),
        operations_acoustics.RawToSvSettings(output_directory=str(tmp_path)),
    )

    assert result == operations_acoustics.RawToSvResult(
        filename_raw="example.raw",
        filename_Sv="example_Sv.zarr",
        first_ping_time=pd.Timestamp("2026-01-01T00:00:00Z"),
        last_ping_time=pd.Timestamp("2026-01-01T00:01:00Z"),
    )
    assert dataset.saved == {
        "store": Path(tmp_path) / "example_Sv.zarr",
        "mode": "w",
        "consolidated": True,
    }
