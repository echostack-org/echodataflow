from pathlib import Path

import echoshader
import matplotlib.pyplot as plt
import panel as pn
import xarray as xr
from holoviews import opts

pn.config.autoreload = False

path_MVBS = Path(r"PATH_TO_YOUR_MVBS_ZARR")
path_CPS = Path(r"PATH_TO_YOUR_CPS_ZARR")


def multi_freq_app():
    ds = xr.open_zarr(path_MVBS / "latest_MVBS.zarr")

    egram = ds.eshader.echogram(
        channel=list(ds.channel.values),
        vmin=-70,
        vmax=-36,
        cmap="viridis",
        opts=opts.Image(
            width=1000,
            height=400,
            tools=["pan", "box_zoom", "wheel_zoom", "reset"],
        ),
    )

    return pn.pane.HoloViews(egram)


def tricolor_app():
    ds = xr.open_zarr(path_MVBS / "latest_MVBS.zarr")
    channels = list(ds.channel.values)

    tricolor = ds.eshader.echogram(
        channel=[channels[3], channels[1], channels[0]],
        vmin=-70,
        vmax=-36,
        rgb_composite=True,
        opts=opts.RGB(
            width=1000,
            height=400,
            tools=["pan", "box_zoom", "wheel_zoom", "reset"],
        ),
    )

    return pn.pane.HoloViews(tricolor)


def cps_matplotlib_app():
    zarr_path = next(path_CPS.glob("*_mask_plot.zarr"))
    ds = xr.open_zarr(zarr_path)

    channel = "WBT 400142-15 ES70-7C_ES"
    da = ds["Sv"].sel(channel=channel).compute()

    x = da["ping_time"].values
    y = da["echo_range"].values
    z = da.values.T

    fig, ax = plt.subplots(figsize=(13, 5))
    im = ax.pcolormesh(x, y, z, shading="auto", vmin=-115, vmax=-36)
    ax.invert_yaxis()
    ax.set_xlabel("Ping time")
    ax.set_ylabel("Depth / range (m)")
    ax.set_title(f"CPS masked Sv - {channel}")
    fig.colorbar(im, ax=ax, label="Masked Sv (dB re 1 m$^{-1}$)")
    fig.tight_layout()

    return pn.Column(
        "# CPS masked Sv",
        f"File: `{zarr_path.name}` | Channel: `{channel}`",
        pn.pane.Matplotlib(fig, tight=True),
    )


test_server = pn.serve(
    {
        "multi_freq_echogram": multi_freq_app,
        "tricolor_echogram": tricolor_app,
        "cps_matplotlib_echogram": cps_matplotlib_app,
    },
    port=1802,
    websocket_origin="*",
    admin=True,
    show=False,
    autoreload=False,
    keep_alive=40000,
    check_unused_sessions_milliseconds=30000,
)