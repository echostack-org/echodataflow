from pathlib import Path
import panel as pn
import xarray as xr
from holoviews import opts
import echoshader

# Configure Panel to prevent automatic refreshes
pn.config.autoreload = False

path_MVBS = Path("/media/volume/shimada_202506_volume/viz_data_cache_2026/drixo16")


def update_cache_multi_freq():
    """
    Load latest MVBS data and create multi-frequency echograms.
    """
    ds_MVBS = xr.open_zarr(path_MVBS / "latest_MVBS.zarr")
    ch_wanted = [ch for ch in ds_MVBS["channel"].values if ch.endswith("_1")]
    egram = ds_MVBS.eshader.echogram(
        channel=ch_wanted,
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
    ds_MVBS = xr.open_zarr(path_MVBS / "latest_MVBS.zarr")

    tricolor = ds_MVBS.eshader.echogram(
        channel=[
            "WBT 976714-15 ES120-7C_1",
            "WBT 978217-15 ES38-7_1",
            "WBT 998500-15 ES18-11mk2_1",
        ],
        vmin=-70,
        vmax=-36,
        rgb_composite=True,
        opts=opts.RGB(
            width=1000, height=400,
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


# Deploy the application with stable configuration
test_server = pn.serve(
    {
        "multi_freq_echogram": multi_freq_app,
        "tricolor_echogram": tricolor_app,
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
