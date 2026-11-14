"""Prepared displays preserve source values; unchanged revisions avoid array reads."""

import sqlite3

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from echodataflow.operations.cps_transect_cache import prepare_transect_caches


def setup_survey(root):
    source = root / "CPS_Sv"
    source.mkdir()
    times = pd.date_range("2021-08-12", periods=12, freq="s")
    sv = np.arange(3 * 12 * 18, dtype=float).reshape(3, 12, 18) - 100
    data = xr.Dataset(
        {
            "Sv": (("channel", "ping_time", "range_sample"), sv),
            "Sv_water_column": (("channel", "ping_time", "range_sample"), sv - 1),
            "Sv_masked": (("channel", "ping_time", "range_sample"), sv - 2),
            "depth": (("ping_time", "range_sample"), np.tile(np.arange(18), (12, 1))),
            "frequency_nominal": ("channel", [38000.0, 70000.0, 120000.0]),
            "longitude": ("ping_time", np.linspace(-122, -121, 12)),
            "latitude": ("ping_time", np.linspace(36, 35, 12)),
        },
        coords={
            "channel": ["38", "70", "120"],
            "ping_time": times,
            "range_sample": np.arange(18),
        },
    )
    for index in range(2):
        data.isel(ping_time=slice(index * 6, (index + 1) * 6)).chunk(
            {"channel": 1, "ping_time": 2, "range_sample": 3}
        ).to_zarr(source / f"{index}.zarr", zarr_format=2, consolidated=True)
    db = root / "processing.db"
    with sqlite3.connect(db) as con:
        con.execute(
            "CREATE TABLE transects (transect_part TEXT, start_time TEXT, end_time TEXT, status TEXT)"
        )
        con.execute(
            "CREATE TABLE sv_cps (cps_filename TEXT, first_ping_time TEXT, last_ping_time TEXT, status TEXT)"
        )
        for i in range(2):
            con.execute(
                "INSERT INTO sv_cps VALUES (?,?,?,?)",
                (
                    f"{i}.zarr",
                    times[i * 6].isoformat(),
                    times[i * 6 + 5].isoformat(),
                    "completed",
                ),
            )
        con.execute(
            "INSERT INTO transects VALUES (?,?,?,?)",
            ("001", times[0].isoformat(), times[-1].isoformat(), "completed"),
        )
        con.execute(
            "INSERT INTO transects VALUES (?,?,?,?)",
            ("002", times[0].isoformat(), times[5].isoformat(), "completed"),
        )
        con.execute(
            "INSERT INTO transects VALUES (?,?,?,?)",
            ("003", times[0].isoformat(), times[-1].isoformat(), "pending"),
        )
    return source, root / "viz_cache_CPS", db, data


def test_backfills_all_completed_and_preserves_full_resolution(tmp_path):
    source, cache, db, original = setup_survey(tmp_path)
    written = prepare_transect_caches(
        source, cache, db, preview_max_pings=4, preview_max_range_samples=5
    )
    assert set(written) == {"001", "002"}
    assert not (cache / "transects/transect_003.zarr").exists()
    with xr.open_zarr(cache / "transects/transect_001.zarr") as full:
        xr.testing.assert_equal(full.Sv.compute(), original.Sv)
        xr.testing.assert_equal(full.depth.compute(), original.depth)
        assert full.attrs["cache_kind"] == "full_resolution"
    assert not list((cache / "transects").glob("*_preview.zarr"))


def test_unchanged_cache_skips_all_acoustic_opens(tmp_path, monkeypatch):
    source, cache, db, _ = setup_survey(tmp_path)
    prepare_transect_caches(source, cache, db)

    def forbidden(*args, **kwargs):
        raise AssertionError("Unchanged cache must not reopen acoustic arrays")

    monkeypatch.setattr(xr, "open_zarr", forbidden)
    assert prepare_transect_caches(source, cache, db) == []


def test_legacy_preview_arguments_do_not_create_subsamples(tmp_path):
    source, cache, db, _ = setup_survey(tmp_path)
    prepare_transect_caches(source, cache, db)
    marker = cache / "transects/transect_001.zarr/.revision"
    before = marker.stat().st_mtime_ns
    prepare_transect_caches(source, cache, db, preview_max_pings=3)
    assert marker.stat().st_mtime_ns == before
    assert not list((cache / "transects").glob("*_preview.zarr"))


def test_missing_source_does_not_replace_existing_cache(tmp_path):
    import shutil

    source, cache, db, _ = setup_survey(tmp_path)
    prepare_transect_caches(source, cache, db)
    marker = cache / "transects/transect_001.zarr/.revision"
    before = marker.read_text()
    shutil.rmtree(source / "1.zarr")
    prepare_transect_caches(source, cache, db)
    assert marker.read_text() == before


def test_failed_write_retains_previous_published_store(tmp_path, monkeypatch):
    source, cache, db, _ = setup_survey(tmp_path)
    prepare_transect_caches(source, cache, db)
    target = cache / "transects/transect_001.zarr"
    marker = target / ".revision"
    before = marker.read_text()

    def fail(*args, **kwargs):
        raise OSError("Simulated disk failure")

    (source / "0.zarr" / ".revision").write_text("source changed")
    monkeypatch.setattr(xr.Dataset, "to_zarr", fail)
    with pytest.raises(OSError, match="disk failure"):
        prepare_transect_caches(source, cache, db, preview_max_pings=3)
    assert marker.read_text() == before
    with xr.open_zarr(target) as data:
        assert data.sizes["ping_time"] == 12
