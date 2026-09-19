"""Panel application for live SPASSO visualization."""

from __future__ import annotations

from pathlib import Path

import cartopy
import holoviews as hv
import pandas as pd
import panel as pn
from holoviews.streams import Pipe

from echodataflow.services.spasso.data import load_navigation
from echodataflow.services.spasso.layers import (
    add_navigation,
    geographic_bounds,
    navigation_layer,
)
from echodataflow.services.spasso.plotting import (
    create_currents_plot,
    create_product_plot,
)


def create_spasso_app(config: dict):
    """Create the live SPASSO dashboard from configuration."""

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    dashboard_config = config["dashboard"]
    products_config = config["products"]
    currents_config = config.get("currents")

    spasso_dir = Path(
        dashboard_config["spasso_path"]
    )

    navigation_dir = Path(
        dashboard_config["navigation_path"]
    )

    cartopy_dir = Path(
        dashboard_config["cartopy_path"]
    )

    cartopy_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    cartopy.config["data_dir"] = str(
        cartopy_dir
    )

    map_radius_km = dashboard_config.get(
        "map_radius_km",
        500,
    )

    navigation_history_hours = float(
        dashboard_config.get(
            "navigation_history_hours",
            6,
        )
    )

    refresh_seconds = dashboard_config.get(
        "refresh_seconds",
        60,
    )

    plot_config = dashboard_config.get(
        "plot",
        {},
    )

    plot_width = plot_config.get(
        "width",
        520,
    )

    plot_height = plot_config.get(
        "height",
        400,
    )

    # ------------------------------------------------------------------
    # Initial navigation
    #
    # Load the complete available track. navigation_history_hours is
    # used only to control which part is highlighted as "recent".
    # ------------------------------------------------------------------

    navigation = load_navigation(
        navigation_dir=navigation_dir,
    )

    latest = navigation.iloc[-1]

    ship_lat = float(
        latest["latitude"]
    )

    ship_lon = float(
        latest["longitude"]
    )

    bounds = geographic_bounds(
        latitude=ship_lat,
        longitude=ship_lon,
        radius_km=map_radius_km,
    )

    # ------------------------------------------------------------------
    # Shared dynamic navigation
    # ------------------------------------------------------------------

    navigation_pipe = Pipe(
        data=navigation
    )

    def make_navigation_layer(data):
        """Render navigation using the configured recent-track window."""

        return navigation_layer(
            data=data,
            history_hours=navigation_history_hours,
        )

    dynamic_navigation = hv.DynamicMap(
        make_navigation_layer,
        streams=[navigation_pipe],
    )

    # ------------------------------------------------------------------
    # SPASSO products
    #
    # These are deliberately created ONCE.
    #
    # The periodic callback below only updates navigation.
    # ------------------------------------------------------------------

    plots_by_tab: dict[str, list] = {}

    for product_name, product_config in products_config.items():

        plot = create_product_plot(
            product_name=product_name,
            product_config=product_config,
            spasso_dir=spasso_dir,
            bounds=bounds,
            width=plot_width,
            height=plot_height,
        )

        plot = add_navigation(
            plot,
            dynamic_navigation,
        )

        tab_name = product_config.get(
            "tab",
            "Products",
        )

        plots_by_tab.setdefault(
            tab_name,
            [],
        ).append(plot)

    # ------------------------------------------------------------------
    # Surface currents
    # ------------------------------------------------------------------

    if currents_config is not None:

        currents_plot = create_currents_plot(
            currents_config=currents_config,
            spasso_dir=spasso_dir,
            bounds=bounds,
            width=plot_width,
            height=plot_height,
        )

        currents_plot = add_navigation(
            currents_plot,
            dynamic_navigation,
        )

        tab_name = currents_config.get(
            "tab",
            "Currents / Diagnostics",
        )

        # Put currents first in its tab, matching the previous dashboard.
        plots_by_tab.setdefault(
            tab_name,
            [],
        ).insert(
            0,
            currents_plot,
        )

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    status = pn.pane.Markdown(
        (
            f"**Ship position:** "
            f"{latest['latitude']:.5f}, "
            f"{latest['longitude']:.5f}  \n"
            f"**Navigation time:** "
            f"{latest['timestamp_utc']:%Y-%m-%d %H:%M:%S UTC}"
        )
    )

    # ------------------------------------------------------------------
    # Tabs
    # ------------------------------------------------------------------

    tabs_content = []

    for tab_name, plots in plots_by_tab.items():

        grid = pn.GridBox(
            *plots,
            ncols=2,
            sizing_mode="stretch_width",
        )

        tabs_content.append(
            (
                tab_name,
                grid,
            )
        )

    tabs = pn.Tabs(
        *tabs_content,
        dynamic=True,
        sizing_mode="stretch_width",
    )

    # ------------------------------------------------------------------
    # Navigation refresh
    #
    # IMPORTANT:
    # Only navigation is reloaded here.
    #
    # SPASSO NetCDF files and rasterized products are NOT recreated.
    # This preserves the current zoom/pan behavior and avoids repeatedly
    # processing the heavy gridded products.
    # ------------------------------------------------------------------

    def scheduled_update():
        """Update only the ship-navigation layer."""

        try:
            new_navigation = load_navigation(
                navigation_dir=navigation_dir,
            )

            new_latest = new_navigation.iloc[-1]

            navigation_pipe.send(
                new_navigation
            )

            status.object = (
                f"**Ship position:** "
                f"{new_latest['latitude']:.5f}, "
                f"{new_latest['longitude']:.5f}  \n"
                f"**Navigation time:** "
                f"{new_latest['timestamp_utc']:%Y-%m-%d %H:%M:%S UTC}  \n"
                f"**Dashboard refresh:** "
                f"{pd.Timestamp.now(tz='UTC'):%H:%M:%S UTC}"
            )

        except Exception as exc:

            status.object = (
                f"**Navigation update error:** `{exc}`"
            )

            print(
                f"SPASSO navigation update error: {exc}"
            )

    pn.state.add_periodic_callback(
        scheduled_update,
        period=refresh_seconds * 1000,
    )

    # ------------------------------------------------------------------
    # Dashboard
    # ------------------------------------------------------------------

    title = dashboard_config.get(
        "title",
        "SPASSO / Ship Navigation",
    )

    return pn.Column(
        f"# {title}",
        status,
        tabs,
        sizing_mode="stretch_width",
    )