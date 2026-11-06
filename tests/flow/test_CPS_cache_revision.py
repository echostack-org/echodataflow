import os
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr


def test_cps_cache_writer_skips_unchanged_sources(tmp_path, monkeypatch):
    import ast
    import hashlib
    import json
    from contextlib import ExitStack

    # Run the producer function without importing unrelated flow dependencies.
    relative = Path("echodataflow/src/echodataflow/flows/flows_viz_cloud.py")
    source = next(
        (
            base / relative
            for base in Path(__file__).resolve().parents
            if (base / relative).exists()
        ),
        None,
    )
    if source is None:
        import pytest

        pytest.skip("Producer integration check requires the companion flow file")
    fn = next(
        n
        for n in ast.parse(source.read_text()).body
        if isinstance(n, ast.FunctionDef) and n.name == "flow_update_cache_CPS"
    )
    fn.decorator_list = []
    raw = tmp_path / "CPS_Sv"
    raw.mkdir()
    ds = xr.Dataset(
        {"Sv_masked": (("channel", "ping_time", "echo_range"), np.ones((1, 3, 4)))},
        coords={
            "channel": ["70"],
            "ping_time": pd.date_range("2024-01-01", periods=3, freq="min").values,
            "echo_range": [1.0, 2.0, 3.0, 4.0],
        },
    )
    ds.to_zarr(raw / "a.zarr", zarr_format=3)
    calls = []
    original = xr.open_zarr

    def opened(*args, **kwargs):
        calls.append(args[0])
        return original(*args, **kwargs)

    monkeypatch.setattr(xr, "open_zarr", opened)
    namespace = {
        "Path": Path,
        "ExitStack": ExitStack,
        "hashlib": hashlib,
        "json": json,
        "pd": pd,
        "np": np,
        "xr": xr,
        "resolve_database": lambda *args: tmp_path / "processing.db",
        "get_latest_completed_transect": lambda db: {
            "transect_part": "001",
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-01T00:02:00Z",
        },
        "get_completed_cps_files": lambda *args, **kwargs: ["a.zarr"],
        "_prepare_sv_for_echogram": lambda data, **kwargs: data,
    }
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "<cache-producer>", "exec"), namespace)
    run = namespace["flow_update_cache_CPS"]
    run(str(raw), str(tmp_path / "viz"))
    assert len(calls) == 1
    marker = tmp_path / "viz/latest_CPS.zarr/.revision"
    assert marker.exists()
    run(str(raw), str(tmp_path / "viz"))
    assert len(calls) == 1
    metadata = raw / "a.zarr/zarr.json"
    stat = metadata.stat()
    os.utime(metadata, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1000000))
    run(str(raw), str(tmp_path / "viz"))
    assert len(calls) == 2
