
"""Smoke tests for Echodataflow visualization services."""

import importlib
from unittest.mock import patch

import holoviews as hv
import numpy as np
import pandas as pd
import pytest
import xarray as xr


CHANNELS = [
    "WBT 987760-15 ES18_ES",
    "WBT 987763-15 ES38-7_ES",
    "WBT 987766-15 ES70-7C_ES",
    "WBT 987753-15 ES120-7C_ES",
    "WBT 987771-15 ES200-7C_ES",
    "WBT 987769-15 ES333-7C_ES",
]


SHIMADA_CHANNELS = [
    "WBT 400141-15 ES18_ES",
    "WBT 400143-15 ES38B_ES",
    "WBT 400142-15 ES70-7C_ES",
    "WBT 400140-15 ES120-7C_ES",
    "WBT 400145-15 ES200-7C_ES",
]


@pytest.mark.parametrize(
    "module_name, channels",
    [
        ("viz_echogram_track_lasker", CHANNELS),
        ("viz_echogram_track", SHIMADA_CHANNELS),
    ],
)
def test_echograms(monkeypatch, module_name, channels):
    # Import the real service without starting the Panel server.
    with patch("panel.serve"):
        viz = importlib.import_module(
            f"echodataflow.viz.{module_name}"
        )

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
