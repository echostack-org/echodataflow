from pathlib import Path

import dask_image.ndfilters
import echoregions as er
import numpy as np
import pandas as pd
import xarray as xr


def _pick_channel_by_frequency(
    ds: xr.Dataset,
    freq_hz: float,
) -> str:
    idx = int(
        np.abs(
            ds["frequency_nominal"].values - freq_hz
        ).argmin()
    )
    return str(ds["channel"].values[idx])


def _dilate_7x7(
    da: xr.DataArray,
) -> xr.DataArray:
    return xr.DataArray(
        dask_image.ndfilters.maximum_filter(
            da.data,
            size=(1, 7, 7),
        ),
        dims=da.dims,
        coords=da.coords,
    )


def _mask_above_seafloor(
    ds: xr.Dataset,
    bottom_line_path: Path,
    channel: str,
) -> xr.DataArray:
    lines = er.read_lines_csv(
        str(bottom_line_path)
    )

    depth_da = ds["depth"].sel(
        channel=channel
    )

    deepest_ping_idx = int(
        depth_da.max(
            dim="range_sample",
            skipna=True,
        )
        .argmax(dim="ping_time")
        .values
    )

    depth = (
        depth_da
        .isel(ping_time=deepest_ping_idx)
        .dropna(
            dim="range_sample",
            how="all",
        )
    )

    valid_length = depth.sizes[
        "range_sample"
    ]

    ds_trimmed = ds.isel(
        range_sample=slice(
            0,
            valid_length,
        )
    )

    sv_target = ds_trimmed["Sv"].sel(
        channel=[channel]
    )

    sv_for_regions = xr.DataArray(
        sv_target.values,
        dims=[
            "channel",
            "ping_time",
            "depth",
        ],
        coords={
            "channel": [channel],
            "ping_time": sv_target["ping_time"],
            "depth": depth.values,
        },
        name="Sv",
    )

    bottom_mask, _ = lines.seafloor_mask(
        sv_for_regions,
        operation="above_below",
        method="slinear",
        limit_area=None,
        limit_direction="both",
    )

    above_mask = ~bottom_mask.astype(bool)

    if "channel" in above_mask.dims:
        above_mask = above_mask.squeeze(
            "channel",
            drop=True,
        )

    above_mask = above_mask.rename(
        {"depth": "range_sample"}
    )

    above_mask = above_mask.assign_coords(
        range_sample=ds_trimmed[
            "range_sample"
        ]
    )

    return above_mask.reindex(
        range_sample=ds["range_sample"],
        fill_value=False,
    )


def _export_nasc_to_echoview_csv(
    ds_nasc: xr.Dataset,
    output_filepath: Path,
    process_id: int = 1928,
) -> None:
    df = (
        ds_nasc
        .to_dataframe()
        .reset_index()
    )

    depth_step = (
        float(
            np.nanmedian(
                np.diff(
                    ds_nasc["depth"].values
                )
            )
        )
        if ds_nasc.sizes.get(
            "depth",
            0,
        ) > 1
        else 5.0
    )

    ping_time = pd.to_datetime(
        df["ping_time"]
    )

    out = pd.DataFrame(
        {
            "Process_ID": process_id,
            "Interval": (
                pd.factorize(
                    df["distance"]
                )[0]
                + 1
            ),
            "Layer": (
                pd.factorize(
                    df["depth"]
                )[0]
                + 1
            ),
            "Sv_mean": -999.0,
            "NASC": df["NASC"].fillna(0.0),
            "Height_mean": depth_step,
            "Depth_mean": df["depth"],
            "Layer_depth_min": (
                df["depth"]
                - depth_step / 2
            ),
            "Layer_depth_max": (
                df["depth"]
                + depth_step / 2
            ),
            "Ping_S": 0,
            "Ping_E": 0,
            "Dist_M": (
                df["distance"] * 1852
            ),
            "Date_M": ping_time.dt.strftime(
                "%Y%m%d"
            ),
            "Time_M": (
                ping_time
                .dt.strftime(
                    "%H:%M:%S.%f"
                )
                .str[:-2]
            ),
            "Lat_M": df["latitude"],
            "Lon_M": df["longitude"],
            "Noise_Sv_1m": -999.0,
            "Minimum_Sv_threshold_applied": 1,
            "Maximum_Sv_threshold_applied": 0,
            "Standard_deviation": 0.0,
            "Thickness_mean": depth_step,
            "Range_mean": df["depth"],
            "Exclude_below_line_range_mean": 999.0,
            "Exclude_above_line_range_mean": 0.0,
        }
    )

    output_filepath.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    out.to_csv(
        output_filepath,
        index=False,
        encoding="utf-8-sig",
    )
