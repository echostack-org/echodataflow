from pathlib import Path

import datetime
import os
from sqlalchemy import create_engine, inspect
from echodataflow.utils.processing_ledger import resolve_database

import holoviews as hv
import numpy as np
import pandas as pd
import panel as pn
import xarray as xr
from holoviews.operation.datashader import rasterize

hv.extension("bokeh")
pn.extension("tabulator")
pn.config.autoreload = False


# ---------------------------------------------------------------------
# Paths / settings
# ---------------------------------------------------------------------

ROOT_ENV = "ECHODATAFLOW_CPS_ROOT"

if ROOT_ENV not in os.environ:
    raise RuntimeError(
        f"{ROOT_ENV} is not set. "
        "Set it to the root directory containing the CPS workflow outputs."
    )

ROOT = Path(os.environ[ROOT_ENV]).expanduser().resolve()

PATH_CACHE = ROOT / "viz_cache_CPS"
PATH_CPS = ROOT / "CPS_Masks_Zarr"
PATH_NASC = ROOT / "CPS_NASC_Zarr"
PATH_BOTTOM = ROOT / "CPS_Seafloor_CSVs"
PROCESSING_DB = os.environ.get(
    "ECHODATAFLOW_CPS_PROCESSING_DB",
    "processing.db",
)

PATH_DB = resolve_database(
    ROOT,
    PROCESSING_DB,
)
TRANSECT_CSV_ENV = "ECHODATAFLOW_CPS_TRANSECT_CSV"

PATH_TRANSECTS = Path(
    os.environ.get(
        TRANSECT_CSV_ENV,
        ROOT / "plotSurvey_Survey_Data_Visualizer.csv",
    )
).expanduser().resolve()

TARGET_FREQUENCY = float(
    os.environ.get("ECHODATAFLOW_CPS_TARGET_FREQUENCY", "70000")
)


def pick_channel_by_frequency(
    ds: xr.Dataset,
    freq_hz: float,
) -> str:
    """Return the channel whose nominal frequency is closest to freq_hz."""

    if "channel" not in ds.coords:
        raise ValueError(
            "Dataset does not contain a channel coordinate."
        )

    channels = ds["channel"].values

    if "frequency_nominal" not in ds:
        return str(channels[0])

    frequencies = np.asarray(
        ds["frequency_nominal"].values
    ).squeeze()

    if (
        frequencies.ndim != 1
        or frequencies.size != len(channels)
    ):
        return str(channels[0])

    finite = np.isfinite(frequencies)

    if not finite.any():
        return str(channels[0])

    valid_indices = np.flatnonzero(finite)

    idx = valid_indices[
        np.argmin(
            np.abs(
                frequencies[finite] - freq_hz
            )
        )
    ]

    return str(channels[idx])


# ---------------------------------------------------------------------
# Sv plotting
# ---------------------------------------------------------------------

def plot_sv(
    ds: xr.Dataset,
    var_name: str,
    channel: str,
    title: str,
    vmin: float = -100,
    vmax: float = -30,
):
    """Plot Sv using the original irregular ping times."""

    da = ds[var_name].sel(
        channel=channel
    )

    quadmesh = hv.QuadMesh(
        (
            ds["ping_time"].values,
            ds["echo_range"].values,
            da.values.T,
        ),
        kdims=[
            "ping_time",
            "echo_range",
        ],
        vdims=["Sv"],
    )

    return rasterize(
        quadmesh,
        width=1000,
        height=400,
    ).opts(
        cmap="viridis",
        clim=(
            vmin,
            vmax,
        ),
        invert_yaxis=True,
        width=1000,
        height=400,
        tools=[
            "hover",
            "pan",
            "box_zoom",
            "wheel_zoom",
            "reset",
        ],
        title=title,
    )


def plot_seafloor(
    transect_number: str,
):
    """Load detected seafloor line for a transect."""

    bottom_path = (
        PATH_BOTTOM
        / f"transect_{transect_number}_bottom_line.csv"
    )

    if not bottom_path.exists():
        return None

    bottom = pd.read_csv(
        bottom_path
    )

    bottom["time"] = pd.to_datetime(
        bottom["time"]
    )

    return hv.Curve(
        (
            bottom["time"],
            bottom["depth"],
        ),
        kdims=[
            "ping_time",
        ],
        vdims=[
            "echo_range",
        ],
        label="Detected seafloor",
    ).opts(
        color="black",
        line_width=2,
    )


# ---------------------------------------------------------------------
# Processing database
# ---------------------------------------------------------------------

def load_database_tables():
    """Load every table currently present in the processing database."""

    db_value = str(PATH_DB)

    if "://" in db_value:
        database_url = db_value
    else:
        db_path = Path(db_value)

        if not db_path.exists():
            return {}

        database_url = (
            f"sqlite:///{db_path.resolve().as_posix()}"
        )

    engine = create_engine(database_url)

    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    tables = {}

    for table_name in table_names:
        tables[table_name] = pd.read_sql_table(
            table_name,
            con=engine,
        )

    return tables


# ---------------------------------------------------------------------
# CPS / NASC product summary
# ---------------------------------------------------------------------

def load_transect_products():
    """Return a table showing which CPS and NASC products exist."""

    cps = {
        p.name
        .replace(
            "transect_",
            "",
        )
        .replace(
            "_CPS.zarr",
            "",
        )
        for p in PATH_CPS.glob(
            "transect_*_CPS.zarr"
        )
    }

    nasc = {
        p.name
        .replace(
            "transect_",
            "",
        )
        .replace(
            "_nasc.zarr",
            "",
        )
        for p in PATH_NASC.glob(
            "transect_*_nasc.zarr"
        )
    }

    transects = sorted(
        cps | nasc
    )

    rows = []

    for transect in transects:
        rows.append(
            {
                "transect": transect,
                "CPS": (
                    "✓"
                    if transect in cps
                    else ""
                ),
                "NASC": (
                    "✓"
                    if transect in nasc
                    else ""
                ),
            }
        )

    return pd.DataFrame(
        rows
    )


# ---------------------------------------------------------------------
# NASC plotting
# ---------------------------------------------------------------------

def plot_nasc(
    ds_nasc: xr.Dataset,
    title: str,
):
    """Plot depth-integrated NASC along transect time."""

    if "NASC" not in ds_nasc:
        return pn.pane.Markdown(
            "### NASC variable not found in dataset"
        )

    nasc = ds_nasc["NASC"]

    # Select the channel closest to the configured target frequency.
    if "channel" in nasc.dims:
        target_channel = pick_channel_by_frequency(
            ds_nasc,
            TARGET_FREQUENCY,
        )

        nasc = nasc.sel(
            channel=target_channel
        )

    if "frequency_nominal" in nasc.dims:
        nasc = nasc.isel(
            frequency_nominal=0
        )

    nasc = nasc.squeeze(
        drop=True
    )

    print(
        "NASC dims before integration:",
        nasc.dims,
    )
    print(
        "NASC shape before integration:",
        nasc.shape,
    )

    # NASC is depth-resolved in the stored product:
    # distance x depth.
    #
    # For the dashboard, integrate explicitly over depth so that
    # one NASC value remains for each horizontal interval.
    if "depth" in nasc.dims:
        nasc = nasc.sum(
            dim="depth",
            skipna=True,
        )

    nasc = nasc.squeeze(
        drop=True
    )

    print(
        "NASC dims after integration:",
        nasc.dims,
    )
    print(
        "NASC shape after integration:",
        nasc.shape,
    )

    # Use the representative ping time associated with each
    # horizontal NASC interval so the plot aligns with the echogram.
    if "ping_time" in ds_nasc:
        x = pd.to_datetime(
            ds_nasc["ping_time"].values
        )
        xlabel = "Time"
        kdim = "ping_time"

    elif "distance" in nasc.coords:
        x = nasc["distance"].values
        xlabel = "Distance (nmi)"
        kdim = "distance"

    else:
        dim = nasc.dims[0]
        x = nasc[dim].values
        xlabel = dim
        kdim = dim

    curve = hv.Curve(
        (
            x,
            nasc.values,
        ),
        kdims=[
            kdim,
        ],
        vdims=[
            "NASC",
        ],
    )

    return curve.opts(
        width=1000,
        height=220,
        line_width=2,
        tools=[
            "hover",
            "pan",
            "box_zoom",
            "wheel_zoom",
            "reset",
        ],
        title=title,
        xlabel=xlabel,
        ylabel="NASC",
    )

# ---------------------------------------------------------------------
# Latest transect plotting
# ---------------------------------------------------------------------

def load_latest_transect():
    """Load latest cached CPS dataset."""

    cache_path = (
        PATH_CACHE
        / "latest_CPS.zarr"
    )

    if not cache_path.exists():
        raise FileNotFoundError(
            f"CPS cache does not exist yet: "
            f"{cache_path}"
        )

    ds = xr.open_zarr(
        cache_path
    )

    return (
        cache_path,
        ds,
    )


def build_latest_transect_panel(
    vmin: float = -100,
    vmax: float = -30,
):
    """Build Original Sv + water-column Sv + CPS masked Sv + NASC."""

    cache_path, ds = (
        load_latest_transect()
    )

    target_channel = (
        pick_channel_by_frequency(
            ds,
            TARGET_FREQUENCY,
        )
    )

    target_frequency_label = (
        f"{TARGET_FREQUENCY / 1000:g} kHz"
    )

    transect_number = str(
        ds.attrs.get(
            "transect_number",
            "unknown",
        )
    )

    if transect_number != "unknown":
        transect_number = (
            transect_number.zfill(
                3
            )
        )

    cache_time = (
        datetime.datetime
        .fromtimestamp(
            cache_path
            .stat()
            .st_mtime
        )
        .strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    # --------------------------------------------------------------
    # Transect / Sv time coverage
    # --------------------------------------------------------------

    transect_start = "unknown"
    transect_end = "unknown"

    path_transects = (
        PATH_TRANSECTS
    )

    if (
        path_transects.exists()
        and transect_number != "unknown"
    ):
        transect_df = (
            pd.read_csv(
                path_transects,
                dtype={
                    "transectPart": "string",
                    "transectNumber": "string",
                    "transectStart": "string",
                    "transectEnd": "string",
                },
            )
        )

        row = transect_df[
            transect_df[
                "transectNumber"
            ]
            .str.zfill(
                3
            )
            == transect_number
        ]

        if not row.empty:
            transect_start = (
                row.iloc[0][
                    "transectStart"
                ]
            )

            transect_end = (
                row.iloc[0][
                    "transectEnd"
                ]
            )

    ping_start = (
        pd.to_datetime(
            ds[
                "ping_time"
            ]
            .min()
            .values
        )
        .strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    ping_end = (
        pd.to_datetime(
            ds[
                "ping_time"
            ]
            .max()
            .values
        )
        .strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    # --------------------------------------------------------------
    # Metadata
    # --------------------------------------------------------------

    metadata = pn.pane.Markdown(
        f"""
## Latest completed CPS transect

**Transect:** {transect_number}

**Transect window:** {transect_start} → {transect_end}

**Sv coverage:** {ping_start} → {ping_end} UTC

**Cache updated:** {cache_time}

**Cache:** `{cache_path.name}`
"""
    )

    # --------------------------------------------------------------
    # Seafloor
    # --------------------------------------------------------------

    bottom_curve = (
        plot_seafloor(
            transect_number
        )
    )

    # --------------------------------------------------------------
    # Original Sv
    # --------------------------------------------------------------

    original = plot_sv(
        ds,
        var_name="Sv",
        channel=target_channel,
        title=(
            f"Original Sv - "
            f"{target_frequency_label} | "
            f"Transect {transect_number}"
        ),
        vmin=vmin,
        vmax=vmax,
    )

    if bottom_curve is not None:
        original = (
            original
            * bottom_curve
        )

    # --------------------------------------------------------------
    # Water-column masked Sv
    # --------------------------------------------------------------

    if "Sv_water_column" in ds:
        water_column = plot_sv(
            ds,
            var_name="Sv_water_column",
            channel=target_channel,
            title=(
                f"Water-column masked Sv - "
                f"{target_frequency_label} | "
                f"Transect {transect_number}"
            ),
            vmin=vmin,
            vmax=vmax,
        )

        if bottom_curve is not None:
            water_column = (
                water_column
                * bottom_curve
            )

    else:
        water_column = (
            pn.pane.Alert(
                "Sv_water_column is not available "
                "in this CPS product.",
                alert_type="warning",
            )
        )

    # --------------------------------------------------------------
    # CPS masked Sv
    # --------------------------------------------------------------

    masked = plot_sv(
        ds,
        var_name="Sv_masked",
        channel=target_channel,
        title=(
            f"CPS masked Sv - "
            f"{target_frequency_label} | "
            f"Transect {transect_number}"
        ),
        vmin=vmin,
        vmax=vmax,
    )

    if bottom_curve is not None:
        masked = (
            masked
            * bottom_curve
        )

    # --------------------------------------------------------------
    # NASC
    # --------------------------------------------------------------

    nasc_path = (
        PATH_NASC
        / f"transect_{transect_number}_nasc.zarr"
    )

    if nasc_path.exists():
        try:
            ds_nasc = xr.open_zarr(
                nasc_path
            )

            nasc_plot = plot_nasc(
                ds_nasc,
                title=(
                    f"NASC | "
                    f"Transect {transect_number}"
                ),
            )

        except Exception as e:
            nasc_plot = (
                pn.pane.Alert(
                    f"Could not plot NASC: {e}",
                    alert_type="warning",
                )
            )

    else:
        nasc_plot = (
            pn.pane.Alert(
                f"NASC is not available yet for "
                f"transect {transect_number}.",
                alert_type="info",
            )
        )

    return pn.Column(
        metadata,
        original,
        water_column,
        masked,
        nasc_plot,
        sizing_mode="stretch_width",
    )


# ---------------------------------------------------------------------
# Status dashboard
# ---------------------------------------------------------------------

def build_status_panel():
    """Build live processing status tables."""

    # --------------------------------------------------------------
    # Product status
    # --------------------------------------------------------------

    product_df = (
        load_transect_products()
    )

    product_table = (
        pn.widgets.Tabulator(
            product_df,
            pagination=None,
            show_index=False,
            disabled=True,
            sizing_mode="stretch_width",
            height=250,
        )
    )

    product_section = (
        pn.Column(
            "## Transect products",
            product_table,
        )
    )

    # --------------------------------------------------------------
    # processing.db
    # --------------------------------------------------------------

    db_tables = (
        load_database_tables()
    )

    db_panels = []

    if not db_tables:
        db_panels.append(
            pn.pane.Alert(
                "Processing database is unavailable "
                "or contains no tables.",
                alert_type="warning",
            )
        )

    else:
        for (
            table_name,
            df,
        ) in db_tables.items():
            table = (
                pn.widgets.Tabulator(
                    df,
                    pagination="local",
                    page_size=10,
                    show_index=False,
                    disabled=True,
                    sizing_mode="stretch_width",
                    height=300,
                )
            )

            db_panels.append(
                pn.Column(
                    f"### {table_name}",
                    table,
                )
            )

    database_section = (
        pn.Column(
            "## Processing database",
            *db_panels,
        )
    )

    update_time = (
        datetime.datetime.now()
        .strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    header = pn.pane.Markdown(
        f"""
# CPS processing monitor

**Dashboard refreshed:** {update_time}
"""
    )

    return pn.Column(
        header,
        product_section,
        pn.layout.Divider(),
        database_section,
        sizing_mode="stretch_width",
    )


# ---------------------------------------------------------------------
# Main application
# ---------------------------------------------------------------------

def cps_app():
    """Live CPS monitoring dashboard."""

    sv_clim = (
        pn.widgets.RangeSlider(
            name="Sv color range (dB)",
            start=-120,
            end=-20,
            value=(
                -100,
                -30,
            ),
            step=1,
            width=450,
        )
    )

    latest_container = (
        pn.Column(
            sizing_mode="stretch_width",
        )
    )

    status_container = (
        pn.Column(
            sizing_mode="stretch_width",
        )
    )

    def refresh_latest():
        try:
            vmin, vmax = (
                sv_clim.value
            )

            latest_container[:] = [
                build_latest_transect_panel(
                    vmin=vmin,
                    vmax=vmax,
                )
            ]

            print(
                "Latest transect panel refreshed at "
                f"{datetime.datetime.now():%H:%M:%S}"
            )

        except Exception as e:
            latest_container[:] = [
                pn.pane.Alert(
                    f"Could not load latest CPS "
                    f"transect: {e}",
                    alert_type="warning",
                )
            ]

    def refresh_status():
        try:
            status_container[:] = [
                build_status_panel()
            ]

            print(
                "Status panel refreshed at "
                f"{datetime.datetime.now():%H:%M:%S}"
            )

        except Exception as e:
            status_container[:] = [
                pn.pane.Alert(
                    f"Could not refresh processing "
                    f"status: {e}",
                    alert_type="danger",
                )
            ]

    # Initial load
    refresh_latest()
    refresh_status()

    # Rebuild Sv plots when color range changes
    sv_clim.param.watch(
        lambda event: refresh_latest(),
        "value",
    )

    # Refresh echograms / NASC every 30 seconds
    pn.state.add_periodic_callback(
        refresh_latest,
        period=30 * 1000,
    )

    # Refresh processing status every 10 seconds
    pn.state.add_periodic_callback(
        refresh_status,
        period=10 * 1000,
    )

    tabs = pn.Tabs(
        (
            "Latest transect",
            latest_container,
        ),
        (
            "Processing status",
            status_container,
        ),
        dynamic=False,
        sizing_mode="stretch_width",
    )

    template = (
        pn.template.FastListTemplate(
            title="CPS Near-Real-Time Monitor",
            main=[
                sv_clim,
                tabs,
            ],
        )
    )

    return template


# ---------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------

test_server = pn.serve(
    {
        "cps_echogram": cps_app,
    },
    port=1803,
    websocket_origin="*",
    admin=True,
    show=False,
    autoreload=False,
    keep_alive=40000,
    check_unused_sessions_milliseconds=30000,
)