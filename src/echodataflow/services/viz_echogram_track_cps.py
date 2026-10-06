from pathlib import Path

import datetime
import os
from sqlalchemy import create_engine, inspect
from echodataflow.utils.processing_ledger import resolve_database

import holoviews as hv
import numpy as np
import pandas as pd
import panel as pn
import matplotlib.pyplot as plt
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
PATH_NASC = ROOT / "CPS_NASC_Zarr"
PATH_NASC_CSV = ROOT / "CPS_NASC_CSV"
PATH_BOTTOM = ROOT / "CPS_Seafloor_CSVs"
PATH_CPS_SV = ROOT / "CPS_Sv"
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

PLOT_WIDTH = 1000
PLOT_HEIGHT = 400


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

def prepare_echogram_dataset(
    ds: xr.Dataset,
    channel: str,
) -> xr.Dataset:
    """Load only the display-resolution subset needed by the dashboard.

    The cached transect can be much larger than the rendered 1000 x 400
    echogram.  Slice the Dask-backed Zarr lazily first, then load the
    reduced subset once for all three Sv panels.
    """

    variables = [
        name
        for name in (
            "Sv",
            "Sv_water_column",
            "Sv_masked",
            "echo_range",
        )
        if name in ds
    ]

    plot_ds = ds[variables]

    if "channel" in plot_ds.dims:
        plot_ds = plot_ds.sel(channel=channel)

    indexers = {}

    if "ping_time" in plot_ds.dims:
        ping_step = max(
            1,
            int(np.ceil(plot_ds.sizes["ping_time"] / PLOT_WIDTH)),
        )
        indexers["ping_time"] = slice(None, None, ping_step)

    if "range_sample" in plot_ds.dims:
        range_step = max(
            1,
            int(np.ceil(plot_ds.sizes["range_sample"] / PLOT_HEIGHT)),
        )
        indexers["range_sample"] = slice(None, None, range_step)

    if indexers:
        plot_ds = plot_ds.isel(indexers)

    return plot_ds.load()


def plot_sv(
    ds: xr.Dataset,
    var_name: str,
    channel: str,
    title: str,
    vmin: float = -100,
    vmax: float = -30,
):
    """Plot Sv using the original irregular ping times."""

    da = ds[var_name]
    echo_range = ds["echo_range"]

    if "channel" in da.dims:
        da = da.sel(channel=channel)

    if "channel" in echo_range.dims:
        echo_range = echo_range.sel(channel=channel)

    quadmesh = hv.QuadMesh(
        (
            ds["ping_time"].values,
            echo_range.values,
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
        width=PLOT_WIDTH,
        height=PLOT_HEIGHT,
    ).opts(
        cmap="viridis",
        clim=(
            vmin,
            vmax,
        ),
        invert_yaxis=True,
        width=PLOT_WIDTH,
        height=PLOT_HEIGHT,
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

    try:
        inspector = inspect(engine)
        table_names = inspector.get_table_names()

        tables = {}

        for table_name in table_names:
            tables[table_name] = pd.read_sql_table(
                table_name,
                con=engine,
            )

        return tables
    finally:
        engine.dispose()


# ---------------------------------------------------------------------
# CPS / NASC product summary
# ---------------------------------------------------------------------

def _format_coverage(start, end) -> str:
    """Format CPS coverage stored in the transect ledger."""

    if pd.isna(start) and pd.isna(end):
        return ""
    if pd.isna(start):
        return f"→ {end}"
    if pd.isna(end):
        return f"{start} →"
    return f"{start} → {end}"


def load_transect_products():
    """Return transect status from the ledger plus finalized NASC products.

    CPS processing is performed per Sv file, so the processing ledger is the
    source of truth for transect CPS coverage/status. NASC Zarr/CSV columns
    report finalized transect products present on disk.
    """

    tables = load_database_tables()
    transects_df = tables.get("transects")

    columns = [
        "transect",
        "status",
        "CPS coverage",
        "NASC Zarr",
        "NASC CSV",
    ]

    if transects_df is None or transects_df.empty:
        return pd.DataFrame(columns=columns)

    rows = []

    for _, row in transects_df.iterrows():
        transect = str(row.get("transect_part", "")).zfill(3)

        nasc_zarr = PATH_NASC / f"transect_{transect}_nasc.zarr"
        nasc_csv = PATH_NASC_CSV / f"transect_{transect}_nasc.csv"

        rows.append(
            {
                "transect": transect,
                "status": row.get("status", ""),
                "CPS coverage": _format_coverage(
                    row.get("coverage_start"),
                    row.get("coverage_end"),
                ),
                "NASC Zarr": "✓" if nasc_zarr.exists() else "",
                "NASC CSV": "✓" if nasc_csv.exists() else "",
            }
        )

    product_df = pd.DataFrame(rows, columns=columns)
    return product_df.sort_values("transect", kind="stable").reset_index(drop=True)


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

    plot_ds = prepare_echogram_dataset(
        ds,
        target_channel,
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
        plot_ds,
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

    if "Sv_water_column" in plot_ds:
        water_column = plot_sv(
            plot_ds,
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
        plot_ds,
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
            height=180,
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
# Multiple-transect RGB overview
# ---------------------------------------------------------------------

RGB_FREQUENCIES = (18000.0, 38000.0, 70000.0)
MULTI_PLOT_WIDTH = 850
MULTI_PLOT_HEIGHT = 180


def _transect_dataframe() -> pd.DataFrame:
    """Return normalized transect metadata from processing.db."""
    tables = load_database_tables()
    df = tables.get("transects")
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    for col in ("start_time", "end_time", "coverage_start", "coverage_end"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    if "transect_part" in df.columns:
        df["transect"] = df["transect_part"].astype(str).str.zfill(3)
    elif "transect_number" in df.columns:
        df["transect"] = df["transect_number"].astype(str).str.zfill(3)
    else:
        df["transect"] = np.arange(1, len(df) + 1).astype(str)
        df["transect"] = df["transect"].str.zfill(3)

    return df.sort_values("start_time", kind="stable").reset_index(drop=True)


def _completed_cps_dataframe() -> pd.DataFrame:
    """Return completed per-file CPS products with normalized times."""
    tables = load_database_tables()
    df = tables.get("sv_cps")
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    if "status" in df.columns:
        df = df[df["status"].astype(str).str.lower() == "completed"]

    for col in ("first_ping_time", "last_ping_time"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    return df.sort_values("first_ping_time", kind="stable").reset_index(drop=True)


def _cps_paths_for_window(
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> list[Path]:
    """Return completed CPS stores that overlap [start, end]."""
    cps = _completed_cps_dataframe()
    if cps.empty:
        return []

    overlap = cps[
        (cps["last_ping_time"] >= start)
        & (cps["first_ping_time"] <= end)
    ]

    paths = []
    for _, row in overlap.iterrows():
        filename = row.get("cps_filename")
        if pd.isna(filename) or not filename:
            continue
        path = PATH_CPS_SV / str(filename)
        if path.exists():
            paths.append(path)
    return paths


def _open_transect_cps(
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[xr.Dataset | None, int]:
    """Open the available per-file CPS data overlapping one transect."""
    paths = _cps_paths_for_window(start, end)
    if not paths:
        return None, 0

    datasets = []
    for path in paths:
        try:
            ds = xr.open_zarr(path, consolidated=True)
            ds = ds.sel(
                ping_time=slice(
                    start.tz_convert(None).to_datetime64(),
                    end.tz_convert(None).to_datetime64(),
                )
            )
            if ds.sizes.get("ping_time", 0):
                datasets.append(ds)
        except Exception as exc:
            print(f"Could not open {path.name} for RGB overview: {exc}")

    if not datasets:
        return None, 0

    ds = xr.concat(
        datasets,
        dim="ping_time",
        data_vars="minimal",
        coords="minimal",
        compat="override",
        join="override",
    ).sortby("ping_time")

    _, unique_idx = np.unique(ds["ping_time"].values, return_index=True)
    ds = ds.isel(ping_time=np.sort(unique_idx))
    return ds, len(paths)


def _downsample_for_rgb(ds: xr.Dataset) -> xr.Dataset:
    """Reduce a transect to dashboard display resolution before loading."""
    keep = [
        name
        for name in (
            "Sv_masked",
            "Sv_water_column",
            "Sv",
            "depth",
            "echo_range",
            "frequency_nominal",
            "latitude",
            "longitude",
        )
        if name in ds
    ]
    out = ds[keep]

    indexers = {}
    if "ping_time" in out.dims:
        step = max(1, int(np.ceil(out.sizes["ping_time"] / MULTI_PLOT_WIDTH)))
        indexers["ping_time"] = slice(None, None, step)
    if "range_sample" in out.dims:
        step = max(1, int(np.ceil(out.sizes["range_sample"] / MULTI_PLOT_HEIGHT)))
        indexers["range_sample"] = slice(None, None, step)

    if indexers:
        out = out.isel(indexers)
    return out.load()


def _rgb_echogram(
    ds: xr.Dataset,
    transect: str,
    vmin: float,
    vmax: float,
):
    """Build R/G/B echogram using 18/38/70 kHz respectively."""
    if "frequency_nominal" not in ds or "channel" not in ds.coords:
        return pn.pane.Alert(
            f"Transect {transect}: frequency_nominal/channel is unavailable.",
            alert_type="warning",
        )

    source_var = (
        "Sv_masked"
        if "Sv_masked" in ds
        else "Sv_water_column"
        if "Sv_water_column" in ds
        else "Sv"
    )

    channels = [pick_channel_by_frequency(ds, f) for f in RGB_FREQUENCIES]
    plot_ds = _downsample_for_rgb(ds)

    planes = []
    for channel in channels:
        da = plot_ds[source_var]
        if "channel" in da.dims:
            da = da.sel(channel=channel)
        values = np.asarray(da.values, dtype=float)
        values = np.clip((values - vmin) / (vmax - vmin), 0.0, 1.0)
        planes.append(np.nan_to_num(values, nan=0.0, posinf=1.0, neginf=0.0))

    rgb = np.stack(planes, axis=-1)

    # HoloViews image convention is vertical x horizontal x RGB.
    if rgb.ndim != 3:
        raise ValueError(f"Unexpected RGB array shape: {rgb.shape}")
    rgb = np.transpose(rgb, (1, 0, 2))

    vertical = None
    vertical_name = "Depth (m)"
    for name in ("depth", "echo_range"):
        if name in plot_ds:
            vertical = plot_ds[name]
            vertical_name = "Depth (m)" if name == "depth" else "Range (m)"
            break

    if vertical is None:
        y = np.arange(rgb.shape[0])
        vertical_name = "Range sample"
    else:
        if "channel" in vertical.dims:
            vertical = vertical.sel(channel=channels[-1])
        reduce_dims = [d for d in vertical.dims if d != "range_sample"]
        if reduce_dims:
            vertical = vertical.median(dim=reduce_dims, skipna=True)
        y = np.asarray(vertical.values)

    x = pd.to_datetime(plot_ds["ping_time"].values)

    # Trim any mismatch caused by variable-specific trailing invalid samples.
    ny = min(len(y), rgb.shape[0])
    nx = min(len(x), rgb.shape[1])
    y = y[:ny]
    x = x[:nx]
    rgb = rgb[:ny, :nx, :]

    finite_y = np.isfinite(y)
    if finite_y.any():
        y = y[finite_y]
        rgb = rgb[finite_y, :, :]

    image = hv.RGB(
        (x, y, rgb),
        kdims=["ping_time", "depth"],
    )

    return image.opts(
        width=MULTI_PLOT_WIDTH,
        height=MULTI_PLOT_HEIGHT,
        invert_yaxis=True,
        tools=["hover", "pan", "box_zoom", "wheel_zoom", "reset"],
        xlabel="Time",
        ylabel=vertical_name,
        title=f"RGB 18 / 38 / 70 kHz | Transect {transect}",
    )


def _format_timestamp(value) -> str:
    if pd.isna(value):
        return "—"
    return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M")


def _transect_info_card(row: pd.Series, n_files: int):
    status = str(row.get("status", "unknown"))
    coverage_start = row.get("coverage_start")
    coverage_end = row.get("coverage_end")
    coverage = "—"
    if not pd.isna(coverage_start) or not pd.isna(coverage_end):
        coverage = f"{_format_timestamp(coverage_start)} → {_format_timestamp(coverage_end)}"

    return pn.pane.Markdown(
        f"""
### {row['transect']}
**Status:** {status}

**Start:** {_format_timestamp(row.get('start_time'))}

**End:** {_format_timestamp(row.get('end_time'))}

**CPS coverage:**  
{coverage}

**CPS files:** {n_files}
""",
        width=190,
        margin=(5, 10, 5, 5),
    )


def _latest_cps_time() -> pd.Timestamp | None:
    cps = _completed_cps_dataframe()
    if cps.empty or "last_ping_time" not in cps:
        return None
    value = cps["last_ping_time"].max()
    return None if pd.isna(value) else pd.Timestamp(value)


def _current_transect_number(transects: pd.DataFrame) -> str | None:
    """Find transect containing the most recent completed CPS ping."""
    latest = _latest_cps_time()
    if latest is None or transects.empty:
        return None

    active = transects[
        (transects["start_time"] <= latest)
        & (transects["end_time"] >= latest)
    ]
    if not active.empty:
        return str(active.iloc[-1]["transect"])

    started = transects[transects["start_time"] <= latest]
    if not started.empty:
        return str(started.iloc[-1]["transect"])
    return None


def build_multiple_transects_panel(
    n_transects: int = 5,
    vmin: float = -90,
    vmax: float = -30,
):
    """Stack recent RGB transects."""
    transects = _transect_dataframe()
    if transects.empty:
        return pn.pane.Alert(
            "No transects are registered yet.",
            alert_type="info",
        )

    current = _current_transect_number(transects)

    # Show the current/recent section of the cruise, oldest-to-newest vertically.
    if current is not None:
        current_idx = transects.index[transects["transect"] == current]
        if len(current_idx):
            stop = int(current_idx[-1]) + 1
            start = max(0, stop - n_transects)
            shown = transects.iloc[start:stop]
        else:
            shown = transects.tail(n_transects)
    else:
        shown = transects.tail(n_transects)

    rows = []
    for _, row in shown.iterrows():
        start = row.get("start_time")
        end = row.get("end_time")
        if pd.isna(start) or pd.isna(end):
            rows.append(
                pn.Row(
                    _transect_info_card(row, 0),
                    pn.pane.Alert("Transect window is unavailable.", alert_type="warning"),
                )
            )
            continue

        ds, n_files = _open_transect_cps(start, end)
        info = _transect_info_card(row, n_files)
        if ds is None:
            plot = pn.pane.Markdown(
                f"### Transect {row['transect']}\nNo completed CPS Sv coverage yet.",
                width=MULTI_PLOT_WIDTH,
                height=MULTI_PLOT_HEIGHT,
            )
        else:
            try:
                plot = _rgb_echogram(ds, str(row["transect"]), vmin, vmax)
            except Exception as exc:
                plot = pn.pane.Alert(
                    f"Could not build RGB echogram for transect {row['transect']}: {exc}",
                    alert_type="warning",
                    width=MULTI_PLOT_WIDTH,
                )
        rows.append(pn.Row(info, plot, sizing_mode="stretch_width"))

    left = pn.Column(
        f"## Recent RGB transects (R=18 kHz, G=38 kHz, B=70 kHz)",
        *rows,
        sizing_mode="stretch_width",
    )

    return left


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

    multiple_container = pn.Column(
        sizing_mode="stretch_width",
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

    def refresh_multiple():
        try:
            vmin, vmax = sv_clim.value
            multiple_container[:] = [
                build_multiple_transects_panel(
                    n_transects=5,
                    vmin=vmin,
                    vmax=vmax,
                )
            ]
            print(
                "Multiple transects panel refreshed at "
                f"{datetime.datetime.now():%H:%M:%S}"
            )
        except Exception as e:
            multiple_container[:] = [
                pn.pane.Alert(
                    f"Could not refresh multiple-transect RGB overview: {e}",
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
    refresh_multiple()
    refresh_status()

    # Rebuild Sv plots when color range changes
    sv_clim.param.watch(
        lambda event: (refresh_latest(), refresh_multiple()),
        "value",
    )

    # Refresh echograms / NASC every 30 seconds
    pn.state.add_periodic_callback(
        refresh_latest,
        period=30 * 1000,
    )

    # Refresh multi-transect RGB overview every 30 seconds
    pn.state.add_periodic_callback(
        refresh_multiple,
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
            "Multiple transects",
            multiple_container,
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