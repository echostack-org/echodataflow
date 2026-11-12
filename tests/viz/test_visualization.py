
"""Smoke tests for Echodataflow visualization services."""

import importlib
from unittest.mock import patch

import holoviews as hv
import numpy as np
import pandas as pd
import xarray as xr


SHIMADA_CHANNELS = [
    "WBT 400141-15 ES18_ES",
    "WBT 400143-15 ES38B_ES",
    "WBT 400142-15 ES70-7C_ES",
    "WBT 400140-15 ES120-7C_ES",
    "WBT 400145-15 ES200-7C_ES",
]


def test_echograms(monkeypatch, tmp_path):
    import yaml

    channels = SHIMADA_CHANNELS
    config_path = tmp_path / "viz.yaml"
    config_path.write_text(yaml.safe_dump({"echogram": {
        "cache_dir": str(tmp_path),
        "channels": channels,
        "tricolor_channels": [channels[3], channels[1], channels[0]],
        "port": 1809,
    }}))
    monkeypatch.setenv("ECHODATAFLOW_VIZ_CONFIG", str(config_path))
    with patch("panel.serve") as serve:
        viz = importlib.import_module("echodataflow.viz.viz_echogram")
    assert serve.call_args.kwargs["port"] == 1809
    assert viz.config.cache_dir == tmp_path

    rng = np.random.default_rng(42)

    ds = xr.Dataset(
        {
            "Sv": (
                ("channel", "ping_time", "depth"),
                rng.uniform(-90, -30, (len(channels), 20, 30)),
            )
        },
        coords={
            "channel": channels,
            "ping_time": pd.date_range(
                "2026-01-01", periods=20, freq="s"
            ),
            "depth": np.arange(30, dtype=float),
        },
    )

    # Replace disk access with our synthetic MVBS dataset.
    monkeypatch.setattr(
        viz.xr,
        "open_zarr",
        lambda *_args, **_kwargs: ds,
    )

    multifreq = viz.update_cache_multi_freq()
    tricolor = viz.update_cache_tricolor()

    assert multifreq is not None
    assert tricolor is not None

    # Verify actual HoloViews/Bokeh plot construction.
    renderer = hv.renderer("bokeh")

    assert renderer.get_plot(multifreq).state is not None
    assert renderer.get_plot(tricolor).state is not None
