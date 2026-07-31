import asyncio
import datetime
from pathlib import Path

import dask_image.ndfilters
import echopype as ep
import numpy as np
import pandas as pd
import xarray as xr
from prefect import flow, get_client, get_run_logger, runtime, task
from prefect.states import Cancelled, Failed
from prefect_dask import DaskTaskRunner

from echodataflow.flows.flows_helper import deployment_already_running
from echodataflow.utils.utils import extract_datetime_from_filename


@task(name="dilate_7x7")
def dilate_7x7(da: xr.DataArray) -> xr.DataArray:
    dilated = dask_image.ndfilters.maximum_filter(da.data, size=(1, 7, 7))
    return xr.DataArray(dilated, dims=da.dims, coords=da.coords)


def _pick_channel_by_frequency(ds: xr.Dataset, freq_hz: float) -> str:
    if "frequency_nominal" in ds:
        idx = int(np.abs(ds["frequency_nominal"].values - freq_hz).argmin())
        return str(ds["channel"].values[idx])
    return str(ds["channel"].values[0])


def _make_plot_ready_dataset(ds: xr.Dataset, var_name: str = "Sv_masked") -> xr.Dataset:
    da = ds[var_name].rename("Sv")
    plot_ds = xr.Dataset({"Sv": da})

    if "frequency_nominal" in ds:
        plot_ds["frequency_nominal"] = ds["frequency_nominal"]

    if "depth" in ds:
        y_src = ds["depth"]
    elif "echo_range" in ds:
        y_src = ds["echo_range"]
    else:
        y_src = None

    if y_src is not None:
        reduce_dims = [d for d in ["channel", "ping_time"] if d in y_src.dims]
        if reduce_dims:
            y_src = y_src.median(dim=reduce_dims, skipna=True)

        y = y_src.values
    else:
        y = plot_ds["range_sample"].values

    plot_ds = plot_ds.assign_coords(echo_range=("range_sample", y))
    plot_ds = plot_ds.swap_dims({"range_sample": "echo_range"})

    vmin = float(plot_ds["Sv"].min(skipna=True).compute())
    vmax = float(plot_ds["Sv"].max(skipna=True).compute())

    if not np.isfinite(vmin) or not np.isfinite(vmax):
        vmin, vmax = -100.0, -20.0

    plot_ds["Sv"] = plot_ds["Sv"].assign_attrs(actual_range=(vmin, vmax))

    for var in plot_ds.data_vars:
        plot_ds[var].encoding.pop("chunks", None)
        plot_ds[var].encoding.pop("preferred_chunks", None)

    return plot_ds


@task(log_prints=True)
def task_compute_NASC(
    NASC_filename: str,
    ds_Sv_masked: xr.Dataset,
    path_NASC_zarr: str,
):
    logger = get_run_logger()

    try:
        ds_for_nasc = ds_Sv_masked.assign(Sv=ds_Sv_masked["Sv_masked"])
        ds_NASC = ep.commongrid.compute_NASC(
            ds_Sv=ds_for_nasc,
            range_bin="10m",
            dist_bin="0.5nmi",
        )

        logger.info(f"Saving NASC to zarr: {NASC_filename}")
        ds_NASC.to_zarr(
            store=Path(path_NASC_zarr) / NASC_filename,
            mode="w",
            consolidated=True,
        )
    except Exception as e:
        logger.warning(f"NASC skipped: {e}")


@task(log_prints=True, tags=["acoustic_processing"])
def task_process_acoustic(
    raw_path: str,
    encode_mode: str = "power",
    waveform_mode: str = "CW",
    sonar_model: str = "EK80",
    path_output_zarr: str = "",
    path_output_csv: str = "",
    target_frequency: float = 70000,
    seafloor_threshold: list = [-40, 2.4, 1.0],
    seafloor_offset: float = 0.5,
    seafloor_r0: float = 10,
    seafloor_r1: float = 1000,
    seafloor_wtheta: int = 28,
    seafloor_wphi: int = 52,
    mask_mode: str = "threshold",
    fallback_sv_threshold: float = -70,
):
    logger = get_run_logger()
    print(f"Loading and processing {raw_path}...")

    zarr_chunks = {"channel": 1, "ping_time": 1000, "range_sample": -1}

    ed = ep.open_raw(
        raw_file=str(raw_path),
        sonar_model=sonar_model,
        use_swap=True,
    )

    ds_Sv = ep.calibrate.compute_Sv(
        echodata=ed,
        waveform_mode=waveform_mode,
        encode_mode=encode_mode,
    )

    try:
        ds_Sv = ep.consolidate.add_splitbeam_angle(
            ds_Sv,
            ed,
            waveform_mode=waveform_mode,
            encode_mode=encode_mode,
            to_disk=False,
        )
    except Exception as e:
        logger.warning(f"Split-beam angles skipped: {e}")

    ds_Sv = ep.consolidate.add_depth(ds=ds_Sv, echodata=ed)

    channels = [str(ch) for ch in ds_Sv["channel"].values]
    logger.info(f"Available channels: {channels}")

    target_channel = _pick_channel_by_frequency(ds_Sv, target_frequency)
    logger.info(f"Using target channel: {target_channel}")

    chunked_ds = ds_Sv.chunk(zarr_chunks)

    try:
        aligned_ds_Sv = ep.commongrid.resample_to_geometry(
            chunked_ds,
            target_variable="Sv",
            target_channel=target_channel,
        )
        aligned_ds_Sv["sound_absorption"] = ds_Sv["sound_absorption"]
        aligned_ds_Sv = ep.consolidate.add_depth(aligned_ds_Sv)

        ds_Sv[["Sv", "echo_range"]] = aligned_ds_Sv[["Sv", "echo_range"]]

        for angle_var in ["angle_athwartship", "angle_alongship"]:
            if angle_var in ds_Sv:
                ds_Sv[angle_var] = ep.commongrid.resample_to_geometry(
                    chunked_ds,
                    target_variable=angle_var,
                    target_channel=target_channel,
                )[angle_var]

        ds_Sv = ep.consolidate.add_depth(ds_Sv)
    except Exception as e:
        logger.warning(f"Geometry resampling skipped: {e}")

    try:
        ds_Sv = ep.clean.remove_background_noise(
            ds_Sv,
            ping_num=20,
            range_sample_num=5,
            SNR_threshold="5.0dB",
        )
    except Exception as e:
        logger.warning(f"Background noise removal skipped: {e}")
        ds_Sv["Sv_corrected"] = ds_Sv["Sv"]

    try:
        seafloor_params = {
            "channel": target_channel,
            "var_name": "Sv",
            "threshold": seafloor_threshold,
            "offset": seafloor_offset,
            "r0": seafloor_r0,
            "r1": seafloor_r1,
            "wtheta": seafloor_wtheta,
            "wphi": seafloor_wphi,
        }

        blackwell_depth = ep.mask.detect_seafloor(
            ds=ds_Sv,
            method="blackwell",
            params=seafloor_params,
        )

        df_bottom = pd.DataFrame(
            {
                "time": blackwell_depth["ping_time"].values,
                "depth": blackwell_depth.values,
            }
        )
        df_bottom = df_bottom[df_bottom["depth"] > -0.2]

        out_csv = Path(path_output_csv) / f"{Path(raw_path).stem}_bottom_line.csv"
        df_bottom.to_csv(out_csv, index=False)
        logger.info(f"Saved bottom line to {out_csv}")
    except Exception as e:
        logger.warning(f"Seafloor detection skipped: {e}")

    sv_var = "Sv_corrected" if "Sv_corrected" in ds_Sv else "Sv"

    try:
        ds_Sv["Sv_smoothed"] = ds_Sv[sv_var].rolling(
            ping_time=3,
            range_sample=11,
        ).mean()

        ds_Sv["variance"] = (
            10 ** (ds_Sv[sv_var] / 10)
            - 10 ** (ds_Sv["Sv_smoothed"] / 10)
        ) ** 2

        ds_Sv["variance_smoothed"] = ds_Sv["variance"].rolling(
            ping_time=3,
            range_sample=11,
        ).mean()

        ds_Sv["variance_smoothed"] = 10 * np.log10(
            ds_Sv["variance_smoothed"] ** 0.5
        )

        ds_Sv["variance_smoothed"] = dilate_7x7.fn(ds_Sv["variance_smoothed"])

        if mask_mode == "cps" and len(channels) >= 4 and "frequency_nominal" in ds_Sv:
            ch_38 = _pick_channel_by_frequency(ds_Sv, 38000)
            ch_70 = _pick_channel_by_frequency(ds_Sv, 70000)
            ch_120 = _pick_channel_by_frequency(ds_Sv, 120000)
            ch_200 = _pick_channel_by_frequency(ds_Sv, 200000)

            sd_200 = ds_Sv["variance_smoothed"].sel(channel=ch_200)
            sd_120 = ds_Sv["variance_smoothed"].sel(channel=ch_120)
            mask_sd = (sd_200 > -65) & (sd_120 > -65)

            ds_Sv["Sv_dilated"] = dilate_7x7.fn(ds_Sv["Sv_smoothed"])
            differencing = ds_Sv["Sv_dilated"] - ds_Sv["Sv_dilated"].sel(channel=ch_38)

            diff_200 = differencing.sel(channel=ch_200)
            diff_120 = differencing.sel(channel=ch_120)
            diff_70 = differencing.sel(channel=ch_70)

            mask_frequency_response = (
                ((diff_200 > -13.51) & (diff_200 < 12.53))
                & ((diff_120 > -13.50) & (diff_120 < 9.37))
                & ((diff_70 > -13.85) & (diff_70 < 9.89))
            )

            final_mask = mask_frequency_response & mask_sd
        else:
            logger.warning("Using simple Sv threshold mask.")
            final_mask = ds_Sv[sv_var] > fallback_sv_threshold

        valid_mask = int(final_mask.sum().compute())
        logger.info(f"Final mask valid pixels: {valid_mask}")

        ds_Sv["Sv_masked"] = ds_Sv["Sv"].where(final_mask)

    except Exception as e:
        logger.warning(f"CPS mask failed, using fallback Sv threshold mask: {e}")
        ds_Sv["Sv_masked"] = ds_Sv["Sv"].where(ds_Sv[sv_var] > fallback_sv_threshold)

    try:
        ds_Sv = ep.consolidate.add_location(
            ds=ds_Sv,
            echodata=ed,
            datagram_type="MRU1",
        )
    except Exception as e:
        logger.warning(f"Location add skipped: {e}")

    ds_Sv = ds_Sv.chunk(zarr_chunks)

    task_compute_NASC.fn(
        NASC_filename=f"{Path(raw_path).stem}_nasc.zarr",
        ds_Sv_masked=ds_Sv,
        path_NASC_zarr=path_output_zarr,
    )

    out_zarr = Path(path_output_zarr) / f"{Path(raw_path).stem}_mask.zarr"
    ds_Sv.to_zarr(store=out_zarr, mode="w", consolidated=True)

    plot_ds = _make_plot_ready_dataset(ds_Sv, var_name="Sv_masked")
    out_plot_zarr = Path(path_output_zarr) / f"{Path(raw_path).stem}_mask_plot.zarr"
    plot_ds.chunk({"channel": 1, "ping_time": 213, "echo_range": 4000}).to_zarr(
        store=out_plot_zarr,
        mode="w",
        consolidated=True,
    )

    return (
        out_zarr.name,
        pd.to_datetime(ds_Sv["ping_time"][0].values),
        pd.to_datetime(ds_Sv["ping_time"][-1].values),
    )


@flow(log_prints=True, task_runner=DaskTaskRunner())
def flow_process_acoustic_data(
    exclude_before: str | None = None,
    exclude_raw_file: list[str] = [],
    parallel: bool = False,
    encode_mode: str = "power",
    waveform_mode: str = "CW",
    sonar_model: str = "EK80",
    filename_pattern: str = "*.raw",
    path_main: str = "processed_data",
    path_raw: str = "raw_data",
    file_Sv_csv: str = "processed_files_registry.csv",
    new_file_num_limit: int = 50,
    target_frequency: float = 70000,
    seafloor_threshold: list = [-40, 2.4, 1.0],
    seafloor_offset: float = 0.5,
    seafloor_r0: float = 10,
    seafloor_r1: float = 1000,
    seafloor_wtheta: int = 28,
    seafloor_wphi: int = 52,
    mask_mode: str = "threshold",
    fallback_sv_threshold: float = -70,
):
    errors = []

    already_running = asyncio.run(deployment_already_running())
    if already_running:
        print("Cancelling because deployment_already_running() returned True")
        async def cancel_run():
            async with get_client() as client:
                await client.set_flow_run_state(
                    flow_run_id=runtime.flow_run.id,
                    state=Cancelled(message="Another instance of this flow is already running"),
                )

        asyncio.run(cancel_run())
        return

    path_main_obj = Path(path_main)
    path_Sv_zarr = path_main_obj / "Sv_Masks_Zarr"
    path_csv_outputs = path_main_obj / "Seafloor_CSVs"
    file_Sv_csv_path = path_main_obj / file_Sv_csv
    path_raw_obj = Path(path_raw)

    path_Sv_zarr.mkdir(parents=True, exist_ok=True)
    path_csv_outputs.mkdir(parents=True, exist_ok=True)

    if not file_Sv_csv_path.exists():
        df_Sv = pd.DataFrame(
            columns=["raw_filename", "zarr_mask_filename", "first_ping_time", "last_ping_time"]
        )
        df_Sv.to_csv(file_Sv_csv_path)
    else:
        df_Sv = pd.read_csv(
            file_Sv_csv_path,
            index_col=0,
            date_format="ISO8601",
            parse_dates=["first_ping_time", "last_ping_time"],
        )
        df_Sv.sort_values(by="first_ping_time", inplace=True, ignore_index=True)

    if exclude_before is None:
        raw_files_in_folder = set(filename.name for filename in path_raw_obj.glob(filename_pattern))
    else:
        raw_files_in_folder = set(
            filename.name
            for filename in path_raw_obj.glob(filename_pattern)
            if extract_datetime_from_filename(filename.name)
            >= datetime.datetime.fromisoformat(exclude_before)
        )

    raw_files_in_df = set() if df_Sv.empty else set(df_Sv["raw_filename"].tolist())

    last_raw_filename = df_Sv.iloc[-1]["raw_filename"] if not df_Sv.empty else None
    if last_raw_filename:
        df_Sv = df_Sv[:-1]

    ######
    # we could change to fetch only latest files in the folder
    new_files = set(raw_files_in_folder)
    # new_files = raw_files_in_folder.difference(raw_files_in_df)
    ######

    if last_raw_filename:
        new_files.add(last_raw_filename)

    if len(exclude_raw_file) > 0:
        new_files.difference_update(set(exclude_raw_file))

    new_files = sorted(list(new_files))

    if new_file_num_limit != -1 and len(new_files) > new_file_num_limit:
        new_files = new_files[:new_file_num_limit]

    task_kwargs = dict(
        encode_mode=encode_mode,
        waveform_mode=waveform_mode,
        sonar_model=sonar_model,
        path_output_zarr=str(path_Sv_zarr),
        path_output_csv=str(path_csv_outputs),
        target_frequency=target_frequency,
        seafloor_threshold=seafloor_threshold,
        seafloor_offset=seafloor_offset,
        seafloor_r0=seafloor_r0,
        seafloor_r1=seafloor_r1,
        seafloor_wtheta=seafloor_wtheta,
        seafloor_wphi=seafloor_wphi,
        mask_mode=mask_mode,
        fallback_sv_threshold=fallback_sv_threshold,
    )

    results = []

    for nf in new_files:
        try:
            zarr_filename, first_ping_time, last_ping_time = task_process_acoustic.with_options(
                task_run_name=nf,
                name=nf,
                retries=2,
            )(
                raw_path=path_raw_obj / nf,
                **task_kwargs,
            )
            results.append([nf, zarr_filename, first_ping_time, last_ping_time])
        except Exception as e:
            errors.append(e)
            print(f"Error processing {nf}: {e}")

    if len(results) > 0:
        df_new = pd.DataFrame(
            results,
            columns=["raw_filename", "zarr_mask_filename", "first_ping_time", "last_ping_time"],
        )
        df_Sv = pd.concat([df_Sv, df_new], ignore_index=True)
        df_Sv.sort_values(by=["first_ping_time"], inplace=True, ignore_index=True)
        df_Sv.to_csv(file_Sv_csv_path, date_format="%Y-%m-%dT%H:%M:%S.%f")

    if len(errors) > 0:
        error_msg = f"{len(errors)} errors during acoustic processing out of {len(new_files)} files"
        raise Exception(error_msg)