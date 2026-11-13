from echodataflow.viz.config import load_config, parse_config_file

import holoviews as hv
import numpy as np
import pandas as pd
import panel as pn
import xarray as xr
from echoshader.elements.echogram import echogram, tricolor_echogram

config_file = parse_config_file() if __name__ == "__main__" else None

hv.extension("bokeh")

# Configure Panel to prevent automatic refreshes
pn.config.autoreload = False

config = load_config("echogram", config_file)


def update_cache_multi_freq():
    """
    Load latest MVBS data and create multi-frequency echograms.
    """
    ds_MVBS = xr.open_zarr(config.cache_dir / config.mvbs_file)
    egram = echogram(
        ds_MVBS,
        channel=config.channels,
        vert_dim=config.vertical_dimension,
    ).opts(
        hv.opts.Image(
            cmap="viridis",
            clim=config.color_limits,
            colorbar=True,
            width=1000,
            height=400,
            tools=["pan", "box_zoom", "wheel_zoom", "reset"],
        ),
        hv.opts.QuadMesh(
            cmap="viridis",
            clim=config.color_limits,
            colorbar=True,
            width=1000,
            height=400,
            tools=["pan", "box_zoom", "wheel_zoom", "reset"],
        ),
    )
    return egram


def multi_freq_app():
    """
    Plot multi-frequency echograms with regular updates.
    """
    # Create initial plot
    egram = update_cache_multi_freq()
    plot_pane = pn.pane.HoloViews(egram)

    # Refresh at the configured interval
    def scheduled_update():
        try:
            new_egram = update_cache_multi_freq()
            plot_pane.object = new_egram
            print("Plot updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled update: {e}")

    # Register the periodic refresh callback
    pn.state.add_periodic_callback(scheduled_update, period=config.refresh_seconds * 1000)

    return plot_pane


def update_cache_tricolor():
    """
    Load latest MVBS data and create tricolor echogram.
    """
    ds_MVBS = xr.open_zarr(config.cache_dir / config.mvbs_file)

    tricolor = tricolor_echogram(
        ds_MVBS,
        channel=config.tricolor_channels,
        vmin=config.color_limits[0],
        vmax=config.color_limits[1],
        vert_dim=config.vertical_dimension,
    ).opts(
        width=1200,
        height=600,
        tools=["pan", "box_zoom", "wheel_zoom", "reset"],
    )
    return tricolor


def tricolor_app():
    """
    Plot tricolor echogram with regular updates.
    """
    # Create initial plot
    tricolor = update_cache_tricolor()
    plot_pane = pn.pane.HoloViews(tricolor)

    # Refresh at the configured interval
    def scheduled_update():
        try:
            new_tricolor = update_cache_tricolor()
            plot_pane.object = new_tricolor
            print("Plot updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled update: {e}")

    # Register the periodic refresh callback
    pn.state.add_periodic_callback(scheduled_update, period=config.refresh_seconds * 1000)

    return plot_pane


def create_contours_overlay():
    """
    Convert hake contours dataframe into HoloViews paths.
    """

    contours_df = pd.read_csv(config.cache_dir / config.contours_file)

    # Convert string representations back to arrays
    contours_df["depth"] = contours_df["depth"].apply(
        lambda x: np.fromstring(x.strip("[]"), sep=" ")
    )
    contours_df["time"] = contours_df["time"].apply(
        lambda x: np.array(x.strip("[]").replace("'", "").split())
    )

    # Set to datetime
    contours_df["time"] = contours_df["time"].apply(
        lambda x: pd.to_datetime(x).to_numpy(dtype="datetime64[ns]")
    )

    # Create holoviews path object
    hv_paths = []
    for _, row in contours_df.iterrows():
        time = list(row["time"])
        depth = list(row["depth"])

        # Close contour by appending first point to the end
        time.append(time[0])
        depth.append(depth[0])

        hv_paths.append(
            {
                "ping_time": time,
                config.vertical_dimension: depth,
            }
        )
    contours_hv = hv.Path(hv_paths, kdims=["ping_time", config.vertical_dimension],).opts(
        color="magenta",
        line_width=3,
    )

    return contours_hv


def tricolor_with_contour_app():
    """
    Plot tricolor echogram with regular updates.
    """
    # Create initial plot
    tricolor = update_cache_tricolor()
    contours_hv = create_contours_overlay()
    tricolor_with_contour = tricolor * contours_hv
    plot_pane = pn.pane.HoloViews(tricolor_with_contour)

    # Refresh at the configured interval
    def scheduled_update():
        try:
            new_tricolor = update_cache_tricolor()
            plot_pane.object = new_tricolor
            print("Plot updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled update: {e}")

    # Register the periodic refresh callback
    pn.state.add_periodic_callback(scheduled_update, period=config.refresh_seconds * 1000)

    return plot_pane


# Deploy the application with stable configuration
test_server = pn.serve(
    {
        "multi_freq_echogram": multi_freq_app,
        "tricolor_echogram": tricolor_app,
        "tricolor_contour_echogram": tricolor_with_contour_app,
    },
    port=config.port,
    websocket_origin="*",
    admin=True,
    show=False,
    # Additional settings to prevent auto-refresh issues
    autoreload=False,
    # Keep WebSocket connection stable
    keep_alive=40000,  # 40 seconds
    check_unused_sessions_milliseconds=30000,  # 30 seconds
)

# test_server.stop()
