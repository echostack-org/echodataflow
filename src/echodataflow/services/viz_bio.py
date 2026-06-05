
from pathlib import Path
import datetime

import numpy as np
import pandas as pd
import geopandas as gpd
import shapely

from bokeh.models import HoverTool
import holoviews as hv
import geoviews as gv
import geoviews.tile_sources as gvts
import panel as pn


from viz_core import (
    THEME, BIO_VAR_NAME, COLORBAR_LABEL, BIO_VAR_UNIT, BIO_VAR_CLIM
)


# Initialize extensions
hv.extension("bokeh", enable_mathjax=True)
hv.renderer("bokeh").theme = THEME
pn.extension()


# Path to data files
path_vm_local = Path("/media/volume/shimada_202506_volume/integration")
file_grid = path_vm_local / "grid_cells.geojson"
file_NASC = path_vm_local / "NASC_all.csv"
file_length_count = path_vm_local / "length_count_all.csv"
file_specimen = path_vm_local / "specimen_all.csv"



# Define length count app widgets
update_text_length_count = pn.pane.Markdown(
    f"Length histograms last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
)

# Hidden widget for forcing plot refresh
refresh_button_length_count = pn.widgets.Button(name="Refresh", visible=False)


@pn.depends(refresh_button_length_count)
def plot_length_count(refresh) -> hv.Overlay:
    """
    Plot length count from all haul catches.
    """
    # Load data
    df_len_cnt_all = pd.read_csv(file_length_count)

    # Define stratum options and colors
    stratum_options = sorted(df_len_cnt_all["stratum"].dropna().unique())
    stratum_colors = hv.Cycle("Category10").values
    stratum_colors = {stratum: stratum_colors[i % len(stratum_colors)] for i, stratum in enumerate(stratum_options)}

    # Get all values in "sex"
    sex_options = df_len_cnt_all["sex"].unique().tolist()

    # Create overlay
    plots = []
    for sex in sex_options:
        overlay = None
        for stratum in stratum_options:
            df_sub = df_len_cnt_all[(df_len_cnt_all["sex"] == sex) & (df_len_cnt_all["stratum"] == stratum)]
            if not df_sub.empty:
                hist = hv.Histogram(np.histogram(df_sub["length"], bins=np.arange(0, 101, 5)), label=f"Stratum: {stratum} ({sex})").opts(
                    color=stratum_colors[stratum],
                    alpha=0.6,  # default transparency
                    muted_alpha=0.1,  # transparency when muted via legend
                    height=250, width=650,
                    xlabel="Fork Length (cm)",
                )
                overlay = hist if overlay is None else overlay * hist
        if overlay:
            overlay = overlay.opts(title=f"Sex: {sex}", legend_position="right")
            plots.append(overlay)
    if plots:
        return hv.Layout(plots).cols(1)
    else:
        return hv.Text(0, 0, "No data for available strata.")


def length_count_app():
    """
    Application to visualize length count from all haul catches.
    """
    layout = pn.Column(
        pn.pane.Markdown("# Length histograms"),
        update_text_length_count, plot_length_count,
        sizing_mode="stretch_width"
    )

    def scheduled_update():
        try:
            # Use hidden button to trigger plot_grid_map to run again
            refresh_button_length_count.param.trigger("value")  # This triggers plot_grid_map to run again
            update_text_length_count.object = (
                f"Length histograms last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            print("Length histograms updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled update: {e}")

    doc = pn.state.curdoc
    if not hasattr(doc, "length_count_callback"):
        doc.length_count_callback = pn.state.add_periodic_callback(
            scheduled_update,
            period=10*60*1000  # Update every 10 mins
        )
        def cleanup(session_context):
            try:
                doc.length_count_callback.stop()
            except ValueError:
                pass  # Ignore if callback already stopped or not in list
        pn.state.on_session_destroyed(cleanup)
    # pn.state.add_periodic_callback(
    #     scheduled_update,
    #     period=2*60*1000  # Update every 2 mins
    # )

    return layout



# Axis scale selector for length-weight app
log_selector_length_weight = pn.widgets.RadioButtonGroup(name="Axis Scale", options=["lin-lin", "log-log"], value="log-log")
update_text_length_weight = pn.pane.Markdown(
    f"Length-weight scatter last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
)
refresh_button_length_weight = pn.widgets.Button(name="Refresh", visible=False)

@pn.depends(refresh_button_length_weight, log_selector_length_weight)
def plot_length_weight(refresh, axis_scale) -> hv.Layout:
    """
    Plot length vs weight scatter for all strata and sex, with axis scale selector and separate legend for each plot.
    """
    df_specimen_all = pd.read_csv(file_specimen)
    stratum_options = sorted(df_specimen_all["stratum"].dropna().unique())
    stratum_colors = hv.Cycle("Category10").values
    stratum_colors = {stratum: stratum_colors[i % len(stratum_colors)] for i, stratum in enumerate(stratum_options)}
    sex_options = sorted(df_specimen_all["sex"].dropna().unique())

    plots = []
    for sex in sex_options:
        scatter_overlays = []
        for i, stratum in enumerate(stratum_options):
            df_sub = df_specimen_all[(df_specimen_all["stratum"] == stratum) & (df_specimen_all["sex"] == sex)]
            if not df_sub.empty:
                scatter = hv.Scatter(df_sub, "fork_length", "organism_weight", label=f"Stratum: {stratum} ({sex})")
                opts = dict(
                    color=stratum_colors[stratum],
                    size=6,
                    alpha=0.5,
                    muted_alpha=0.05,
                    legend_position="top_left",
                    height=500, width=500,
                    tools=["hover"],
                    show_legend=True,
                )
                if axis_scale == "log-log":
                    opts["logx"] = True
                    opts["logy"] = True
                scatter = scatter.opts(**opts)
                scatter_overlays.append(scatter)
        if scatter_overlays:
            overlay = hv.Overlay(scatter_overlays).opts(title=f"Sex: {sex}")
            plots.append(overlay)
    if plots:
        return hv.Layout(plots).cols(3)
    else:
        return hv.Text(0, 0, "No data for available strata.")

def length_weight_app():
    """
    Application to visualize length-weight scatter for all haul catches.
    """
    layout = pn.Column(
        pn.pane.Markdown("# Length vs weight by stratum"),
        update_text_length_weight, log_selector_length_weight, plot_length_weight,
        sizing_mode="stretch_width"
    )

    def scheduled_update():
        try:
            refresh_button_length_weight.param.trigger("value")
            update_text_length_weight.object = (
                f"Length-weight scatter last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            print("Length-weight scatter plot updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled update: {e}")

    doc = pn.state.curdoc
    if not hasattr(doc, "length_weight_callback"):
        doc.length_weight_callback = pn.state.add_periodic_callback(
            scheduled_update,
            period=10*60*1000  # Update every 10 mins
        )
        def cleanup(session_context):
            try:
                doc.length_weight_callback.stop()
            except ValueError:
                pass  # Ignore if callback already stopped or not in list
        pn.state.on_session_destroyed(cleanup)
    return layout


def clean_cells(
    gdf_grid_cells: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Clean POLYGON/MULTIPOLYGON objects
    """

    # Remove any POINT values
    gdf_polygon = gdf_grid_cells[
        gdf_grid_cells.geometry.apply(
            lambda x: isinstance(x, (shapely.geometry.Polygon, shapely.geometry.MultiPolygon))
        )
    ]

    return gdf_polygon


# Define widgets
update_text_grid_app = pn.pane.Markdown(
    f"Grid map last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
)

bio_var_selector_grid_map = pn.widgets.Select(
    name="Biological estimate to plot",
    options=list(BIO_VAR_NAME.keys()),
    value="NASC"
)

# Get all WMTS tile source names
tile_options = [name for name, obj in gvts.__dict__.items() if isinstance(obj, gvts.WMTS)]

tile_selector_grid_map = pn.widgets.Select(
    name="Basemap tile source",
    options=tile_options,
    value="EsriNatGeo"
)

# Hidden widget for forcing plot refresh
refresh_button_grid_map = pn.widgets.Button(name="Refresh", visible=False)


@pn.depends(bio_var_selector_grid_map, tile_selector_grid_map, refresh_button_grid_map)
def plot_grid_map(
    bio_var: str,
    map_tile: gv.element.geo.WMTS,
    refresh: bool,
) -> None:
    """
    Plot grid map according to the selected biological variable and tile source.
    """
    # Load grid file
    gdf_grids = gpd.read_file(file_grid)
    gdf_grids = clean_cells(gdf_grids)

    # Get variable attributes
    var = BIO_VAR_NAME.get(bio_var, "biomass")
    var_units = BIO_VAR_UNIT.get(var, "biomass")
    colorbar_label = COLORBAR_LABEL.get(var, "biomass")
    var_clim = BIO_VAR_CLIM.get(var, "biomass")

    # Get the base tilemap
    tile = getattr(gvts, map_tile)

    # Add label column
    gdf_grids.loc[:, f"{var}_label"] = gdf_grids.loc[:, var].apply(
        lambda v: "Unsampled" if pd.isna(v) else f"{v:.2e} {var_units}"
    )

    # Update the hover tooltipe
    hover_tool = HoverTool(
        tooltips=[
            (f"{bio_var}", f"@{var}_label"),
            ("Area", "@area nmi²"),
        ]
    )

    # Begin plotting
    cells = gv.Polygons(
        gdf_grids,
        vdims=[var, f"{var}_label", "area"]
    ).opts(
        width=900,
        height=800,
        colorbar=True,
        cmap="viridis",
        clim=var_clim,
        clipping_colors={"NaN": "white"},
        colorbar_opts={"title": colorbar_label},
        line_color="black",
        alpha=0.5,
        labelled=[bio_var],
        xlabel="Longitude (\u00B0E)",
        ylabel="Latitude (\u00B0N)",
        title=f"{bio_var}",
        tools=[hover_tool],
    )

    # Combine with tilemap
    overlay = tile * cells

    return overlay


def grid_app():
    """
    Application to visualize bio estimate grid cells.
    """
    layout = pn.Column(
        update_text_grid_app, bio_var_selector_grid_map, tile_selector_grid_map, plot_grid_map,
        sizing_mode="stretch_width"
    )

    def scheduled_update():
        try:
            # # Test: cycle through variables
            # current = bio_var_selector.value
            # options = list(BIO_VAR_NAME.keys())
            # idx = (options.index(current) + 1) % len(options)
            # bio_var_selector.value = options[idx]  # triggers plot_grid_map to run

            # Use hidden button to trigger plot_grid_map to run again
            refresh_button_grid_map.param.trigger("value")
            update_text_grid_app.object = (
                f"Grid map last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            print("Grid map updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled update: {e}")

    doc = pn.state.curdoc
    if not hasattr(doc, "grid_map_callback"):
        doc.grid_map_callback = pn.state.add_periodic_callback(
            scheduled_update,
            period=10*60*1000  # Update every 10 mins
        )
        def cleanup(session_context):
            try:
                doc.grid_map_callback.stop()
            except ValueError:
                pass  # Ignore if callback already stopped or not in list
        pn.state.on_session_destroyed(cleanup)
    # pn.state.add_periodic_callback(
    #     scheduled_update,
    #     period=2*60*1000  # Update every 2 mins
    # )

    return layout


# Define widgets
update_text_track_app = pn.pane.Markdown(
    f"Track map last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
)

bio_var_selector_track_map = pn.widgets.Select(
    name="Biological estimate to plot",
    options=["NASC", "Number density", "Biomass density"],
    value="NASC"
)

# Get all WMTS tile source names
tile_options = [name for name, obj in gvts.__dict__.items() if isinstance(obj, gvts.WMTS)]

tile_selector_track_map = pn.widgets.Select(
    name="Basemap tile source",
    options=tile_options,
    value="EsriOceanBase"
)

# Hidden widget for forcing plot refresh
refresh_button_track_map = pn.widgets.Button(name="Refresh", visible=False)


def scale_sizes(values, min_value, max_value, min_size=25, max_size=250):

    # Censor values if needed
    sizes = values.copy()
    sizes.loc[sizes < min_value] = min_value
    sizes.loc[sizes > max_value] = max_value

    return ((sizes - min_value) / (max_value - min_value)) * (max_size - min_size) + min_size


@pn.depends(bio_var_selector_track_map, tile_selector_track_map, refresh_button_track_map)
def plot_track_map(
    bio_var: str,
    map_tile: gv.element.geo.WMTS,
    refresh: bool,
) -> None:
    """
    Plot track map with NASC bubbles according to the selected biological variable and tile source.
    """
    # Load grid file
    df_NASC = pd.read_csv(file_NASC, index_col=0)

    # Filter points with nonzero NASC and format ping_time
    df_NASC_sel = df_NASC[
        (df_NASC["NASC"]>0) & (df_NASC["latitude"].isna() == False) & (df_NASC["longitude"].isna() == False)
    ]
    df_NASC_sel = df_NASC_sel.dropna(subset=["longitude", "latitude", "NASC"])
    df_NASC_sel['ping_time'] = pd.to_datetime(df_NASC_sel['ping_time'], format='ISO8601', errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Get variable attributes
    var = BIO_VAR_NAME.get(bio_var, "NASC")
    var_units = BIO_VAR_UNIT.get(var, "NASC")
    colorbar_label = COLORBAR_LABEL.get(var, "NASC")
    var_clim = BIO_VAR_CLIM.get(var, "NASC")

    # Get the base tilemap
    tile = getattr(gvts, map_tile)

    # Calculate marker size
    df_NASC_sel["size"] = scale_sizes(
        df_NASC_sel["NASC"],
        min_value=var_clim[0],
        max_value=var_clim[1],
        min_size=5,
        max_size=20,
    )

    # Begin plotting
    points = gv.Points(
        df_NASC_sel, ["longitude", "latitude"], [f"{var}", "size", "ping_time", "filename"]
    ).opts(
        color=f"{var}",
        cmap="Reds",
        clim=var_clim,
        colorbar=True,
        size="size",
        alpha=0.5,
        width=900,
        height=800,
        tools=["hover"],
        xlabel="Longitude",
        ylabel="Latitude",
        title=f"Ship track with {bio_var}",
        hover_tooltips=[
            ('Longitude', '@longitude'),
            ('Latitude', '@latitude'),
            (f"{var}", f"@{var}"),
            ('Ping Time', '@ping_time'),
            ('Filename', '@filename'),
        ]
    )

    track = gv.Path([df_NASC[['longitude', 'latitude']].values]).opts(
        color='gray',
        line_width=1
    )

    # Combine with tilemap
    overlay = tile * points * track

    return overlay


def track_app():
    """
    Application to visualize track with NASC bubbles.
    """
    layout = pn.Column(
        update_text_track_app, bio_var_selector_track_map, tile_selector_track_map, plot_track_map,
        sizing_mode="stretch_width"
    )

    def scheduled_update():
        try:
            # Use hidden button to trigger plot_grid_map to run again
            refresh_button_track_map.param.trigger("value")
            update_text_track_app.object = (
                f"Track map last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            print("Track map updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled update: {e}")

    doc = pn.state.curdoc
    if not hasattr(doc, "track_map_callback"):
        doc.track_map_callback = pn.state.add_periodic_callback(
            scheduled_update,
            period=10*60*1000  # Update every 10 mins
        )
        def cleanup(session_context):
            try:
                doc.track_map_callback.stop()
            except ValueError:
                pass  # Ignore if callback already stopped or not in list
        pn.state.on_session_destroyed(cleanup)
    # pn.state.add_periodic_callback(
    #     scheduled_update,
    #     period=2*60*1000  # Update every 2 mins
    # )

    return layout


# Deploy the application with stable configuration
test_server = pn.serve(
    {
        "biological_estimate_grid": grid_app,
        "length_histograms": length_count_app,
        "length_weight_scatter": length_weight_app,
        "track_map": track_app,
    },
    port=1804,
    websocket_origin="*",
    admin=True,
    show=False,
    # Additional settings to prevent auto-refresh issues
    autoreload=False,
    # Keep WebSocket connection stable
    keep_alive=40000,  # 40 seconds
    check_unused_sessions_milliseconds=10000,  # check every 10 seconds
    # unused_session_lifetime=30000,             # kill unused after 30 seconds
)
