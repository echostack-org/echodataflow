from pathlib import Path
import ast

import panel as pn
import xarray as xr
from holoviews import opts
import holoviews as hv
import echoshader
import pandas as pd
import numpy as np

# Configure Panel to prevent automatic refreshes
pn.config.autoreload = False

path_latest = Path("/media/volume/shimada_202506_volume/viz_data_cache_2026/iwcsp_2026")

def update_cache_multi_freq():
    """
    Load latest MVBS data and create multi-frequency echograms.
    """
    ds_MVBS = xr.open_zarr(path_latest / "latest_MVBS.zarr")
    egram = ds_MVBS.eshader.echogram(
        channel=[
            "WBT 987760-15 ES18_ES",
            "WBT 987763-15 ES38-7_ES",
            "WBT 987766-15 ES70-7C_ES",
            "WBT 987753-15 ES120-7C_ES",
            "WBT 987771-15 ES200-7C_ES",
            "WBT 987769-15 ES333-7C_ES"
        ],
        vmin=-70,
        vmax=-36,
        cmap="viridis",
        opts=opts.Image(
            width=1000, height=400,
            tools=["pan", "box_zoom", "wheel_zoom", "reset"],
        )
    )
    return egram


def multi_freq_app():
    """
    Plot multi-frequency echograms with regular updates.
    """
    # Create initial plot
    egram = update_cache_multi_freq()
    plot_pane = pn.pane.HoloViews(egram)
    
    # Simple update function that only runs every 10 minutes
    def scheduled_update():
        try:
            new_egram = update_cache_multi_freq()
            plot_pane.object = new_egram
            print("Plot updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled update: {e}")
    
    # Add ONLY the 10-minute callback - no other automatic updates
    pn.state.add_periodic_callback(
        scheduled_update,
        period=10*60*1000  # Update every 10 mins
    )
    
    return plot_pane


def update_cache_tricolor():
    """
    Load latest MVBS data and create tricolor echogram.
    """
    ds_MVBS = xr.open_zarr(path_latest / "latest_MVBS.zarr")

    tricolor = ds_MVBS.eshader.echogram(
        channel=[
            "WBT 987753-15 ES120-7C_ES",
            "WBT 987763-15 ES38-7_ES",
            "WBT 987760-15 ES18_ES",
        ],
        vmin=-70,
        vmax=-36,
        rgb_composite=True,
        opts=opts.RGB(
            width=1200, height=600,
            tools=["pan", "box_zoom", "wheel_zoom", "reset"],
        )
    )
    return tricolor


def tricolor_app():
    """
    Plot tricolor echogram with regular updates.
    """
    # Create initial plot
    tricolor = update_cache_tricolor()
    plot_pane = pn.pane.HoloViews(tricolor)
    
    # Simple update function that only runs every 10 minutes
    def scheduled_update():
        try:
            new_tricolor = update_cache_tricolor()
            plot_pane.object = new_tricolor
            print("Plot updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled update: {e}")
    
    # Add ONLY the 10-minute callback - no other automatic updates
    pn.state.add_periodic_callback(
        scheduled_update,
        period=10*60*1000  # Update every 10 mins
    )
    
    return plot_pane


def create_contours_overlay():
    """
    Convert hake contours dataframe into HoloViews paths.
    """

    contours_df = pd.read_csv(
        path_latest / "latest_contours.csv"
    )

    # Convert string representations back to arrays
    contours_df["depth"] = contours_df["depth"].apply(
        lambda x: np.fromstring(
            x.strip("[]"),
            sep=" "
        )
    )
    contours_df["time"] = contours_df["time"].apply(
        lambda x: np.array(
            x.strip("[]").replace("'", "").split()
        )
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
                "depth": depth,
            }
        )
    contours_hv = hv.Path(
        hv_paths,
        kdims=["ping_time", "depth"],
    ).opts(
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
    tricolor_with_contour = tricolor() * contours_hv
    plot_pane = pn.pane.HoloViews(tricolor_with_contour)
    
    # Simple update function that only runs every 10 minutes
    def scheduled_update():
        try:
            new_tricolor = update_cache_tricolor()
            plot_pane.object = new_tricolor
            print("Plot updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled update: {e}")
    
    # Add ONLY the 10-minute callback - no other automatic updates
    pn.state.add_periodic_callback(
        scheduled_update,
        period=10*60*1000  # Update every 10 mins
    )
    
    return plot_pane


# Deploy the application with stable configuration
test_server = pn.serve(
    {
        "multi_freq_echogram": multi_freq_app,
        "tricolor_echogram": tricolor_app,
        "tricolor_contour_echogram": tricolor_with_contour_app,
    },
    port=1802,
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
