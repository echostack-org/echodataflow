import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from echodataflow.tasks import tasks_cps as tasks
from echodataflow.utils.processing_ledger import initialize_ledger, register_sv_file


@pytest.mark.parametrize("remove_noise", [False, True])
@pytest.mark.parametrize("write_fails", [False, True])
def test_file_task_records_success_or_write_failure(
    monkeypatch, tmp_path, write_fails, remove_noise
):
    sv = tmp_path / "Sv" / "example_Sv.zarr"
    sv.mkdir(parents=True)
    (sv / ".zmetadata").write_text("{}")
    db = tmp_path / "processing.db"
    initialize_ledger(db)
    register_sv_file(db, sv)
    output = tmp_path / "CPS_Sv"
    bottom_dir = tmp_path / "CPS_Seafloor_CSVs"
    output.mkdir()
    bottom_dir.mkdir()
    times = pd.date_range("2024-08-05T13:20:00", periods=2, freq="s").values
    shape = (1, 2, 3)
    ds = xr.Dataset(
        {
            name: (("channel", "ping_time", "range_sample"), np.full(shape, value))
            for name, value in [
                ("Sv", -50.0),
                ("depth", 20.0),
                ("angle_alongship", 0.0),
                ("angle_athwartship", 0.0),
            ]
        },
        coords={"channel": ["70"], "ping_time": times, "range_sample": [0, 1, 2]},
    )
    monkeypatch.setattr(tasks.xr, "open_zarr", lambda *a, **kw: ds.copy(deep=True))
    monkeypatch.setattr(tasks, "align_cps_geometry", lambda ds, *a: (ds, "70"))
    monkeypatch.setattr(
        tasks,
        "detect_cps_seafloor",
        lambda *a: xr.DataArray([30.0, 30.0], dims="ping_time", coords={"ping_time": times}),
    )
    monkeypatch.setattr(
        tasks,
        "_mask_above_seafloor",
        lambda ds, *a: xr.ones_like(ds.depth.isel(channel=0, drop=True), dtype=bool),
    )
    corrections = []

    def correction(ds, *args):
        corrections.append(True)
        return ds.assign(Sv_corrected=ds.Sv)

    def classify(ds, *args, use_noise_residual):
        assert use_noise_residual is remove_noise
        return xr.ones_like(ds.Sv, dtype=bool)

    monkeypatch.setattr(tasks, "correct_background_noise", correction)
    monkeypatch.setattr(tasks, "classify_cps", classify)
    written = []

    def save(dataset, store, **kwargs):
        with sqlite3.connect(db) as con:
            assert con.execute("SELECT status FROM sv_cps").fetchone()[0] == "processing"
        if write_fails:
            raise OSError("injected write failure")
        assert kwargs == dict(mode="w", consolidated=True)
        assert "Sv_masked" in dataset
        assert dataset.attrs["cps_background_noise_removed"] == int(remove_noise)
        assert dataset.attrs["cps_noise_residual_criterion_applied"] == int(remove_noise)
        written.append(Path(store))
        Path(store).mkdir()

    monkeypatch.setattr(xr.Dataset, "to_zarr", save)
    kwargs = dict(
        sv_path=str(sv),
        sv_filename=sv.name,
        db_path=str(db),
        path_cps_sv=str(output),
        path_bottom=str(bottom_dir),
        target_frequency=70000,
        min_depth=10,
        seafloor_threshold=[-40, 2.4, 1.0],
        seafloor_offset=0.5,
        seafloor_r0=10,
        seafloor_r1=1000,
        seafloor_wtheta=28,
        seafloor_wphi=52,
        mask_mode="cps",
        fallback_sv_threshold=-70,
        remove_background_noise=remove_noise,
    )
    if write_fails:
        with pytest.raises(OSError, match="injected write failure"):
            tasks._process_one_sv_cps.fn(**kwargs)
    else:
        tasks._process_one_sv_cps.fn(**kwargs)
    assert len(corrections) == int(remove_noise)
    with sqlite3.connect(db) as con:
        status, filename, error, first, last = con.execute(
            "SELECT status, cps_filename, error, first_ping_time, last_ping_time FROM sv_cps"
        ).fetchone()
    if write_fails:
        assert status == "failed"
        assert "injected write failure" in error
        assert filename is None
    else:
        assert status == "completed"
        assert filename == "example_CPS.zarr"
        assert written == [output / filename]
        assert pd.Timestamp(first) == pd.Timestamp(times[0]).tz_localize("UTC")
        assert pd.Timestamp(last) == pd.Timestamp(times[-1]).tz_localize("UTC")
        frame = pd.read_csv(bottom_dir / "example_bottom_line.csv")
        assert frame["depth"].tolist() == [30.0, 30.0]
