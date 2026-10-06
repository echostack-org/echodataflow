"""Hake-prediction and downstream NASC operations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import echopype as ep
import echoregions as er
import numpy as np
import pandas as pd
import torch
import xarray as xr
from segmentation_inference.model import binary_hake_model
from segmentation_inference.utils import get_MVBS_tensor


def get_hake_model(model_path: str) -> binary_hake_model:
    """Load a binary hake model and its trained weights."""
    model = binary_hake_model().eval()
    model.load_state_dict(torch.load(model_path, map_location=torch.device("cpu"))["state_dict"])
    return model


@dataclass(frozen=True)
class PredictHakeWorkItem:
    """One prediction time slice and its contributing MVBS files."""

    start_time: pd.Timestamp
    end_time: pd.Timestamp
    filenames_MVBS: tuple[str, ...]
    filename_postfix: str


@dataclass(frozen=True)
class PredictHakeSettings:
    """Processing settings shared by a batch of prediction slices."""

    model: binary_hake_model
    directory_MVBS: str
    directory_prediction: str
    directory_evr: str
    temperature: float = 0.5
    softmax_threshold: float = 0.5
    max_depth: float = 590.0


@dataclass(frozen=True)
class PredictHakeResult:
    """Prediction arrays plus metadata needed by downstream processing."""

    mvbs_dataset: xr.Dataset
    hake_prediction: xr.DataArray
    filename_postfix: str
    score_filename: str
    softmax_filename: str
    prediction_filename: str
    evr_filename: str
    first_ping_time: pd.Timestamp
    last_ping_time: pd.Timestamp


def predict_hake(
    item: PredictHakeWorkItem,
    settings: PredictHakeSettings,
) -> PredictHakeResult:
    """
    Predict on a single MVBS time slice.

    This function combines multiple MVBS files into a single dataset,
    converts it to the input tensor, and feeds it into the model for prediction.
    """
    # Remove timezone info for slicing
    start_time = item.start_time.replace(tzinfo=None)
    end_time = item.end_time.replace(tzinfo=None)

    # Combine MVBS files into a single dataset
    ds_MVBS_combine = xr.open_mfdataset(
        [Path(settings.directory_MVBS) / mvbsf for mvbsf in item.filenames_MVBS],
        parallel=True,
        coords="minimal",
        data_vars="minimal",
        compat="override",
        chunks={"channel": -1, "ping_time": -1, "depth": -1},  # load everything into 1 chunk
        engine="zarr",  # use zarr engine for reading
    ).sel(
        # slice start/end, end exclusive
        ping_time=slice(start_time, end_time - pd.to_timedelta("10milliseconds")),
        depth=slice(None, settings.max_depth),  # slice to what the model expects
    )

    # Prepare input tensor: slice depth and ensure order of coordinates
    input_tensor = get_MVBS_tensor(ds_MVBS_combine)

    # Predict using the model
    output_dict = settings.model.forward(
        input_tensor,
        softmax_temperature=settings.temperature,
    )
    score_tensor = output_dict["interpolated_output"].detach()
    score_tensor_softmax = output_dict["softmax_output"].detach()

    # Assemble output DataArrays
    da_score = xr.DataArray(
        score_tensor,
        coords={
            "scatterer_class": ["background", "hake"],
            "depth": ds_MVBS_combine["depth"],
            "ping_time": ds_MVBS_combine["ping_time"],
        },
        name="score",
    )
    da_score_softmax = xr.DataArray(
        score_tensor_softmax,
        coords={
            "scatterer_class": ["background", "hake"],
            "depth": ds_MVBS_combine["depth"],
            "ping_time": ds_MVBS_combine["ping_time"],
        },
        name="softmax_score",
    )
    da_predict_hake = (
        da_score_softmax.sel(scatterer_class="hake")  # only need hake class
        .transpose("ping_time", "depth")  # TODO: remove once update echopype to 0.10.2
        .drop_vars("scatterer_class")
    ) > settings.softmax_threshold
    da_predict_hake.name = "hake_prediction"

    # Save to zarr
    score_filename = f"score_{item.filename_postfix}.zarr"
    softmax_filename = f"softmax_{item.filename_postfix}.zarr"
    prediction_filename = f"prediction_{item.filename_postfix}.zarr"
    da_score.chunk({"scatterer_class": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=Path(settings.directory_prediction) / score_filename,
        mode="w",
        consolidated=True,
    )
    da_score_softmax.chunk({"scatterer_class": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=Path(settings.directory_prediction) / softmax_filename,
        mode="w",
        consolidated=True,
    )
    da_predict_hake.chunk({"ping_time": -1, "depth": -1}).to_zarr(
        store=Path(settings.directory_prediction) / prediction_filename,
        mode="w",
        consolidated=True,
    )

    # Save to evr
    evr_filename = f"prediction_{item.filename_postfix}.evr"
    er.write_evr(
        Path(settings.directory_evr) / evr_filename,
        da_predict_hake,
        region_classification="hake",
    )

    return PredictHakeResult(
        mvbs_dataset=ds_MVBS_combine,
        hake_prediction=da_predict_hake,
        filename_postfix=item.filename_postfix,
        score_filename=score_filename,
        softmax_filename=softmax_filename,
        prediction_filename=prediction_filename,
        evr_filename=evr_filename,
        # Need to enforce UTC as df_prediction for which these values will be
        # put into is already read in as UTC (if it already exists before this flow)
        first_ping_time=pd.to_datetime(
            ds_MVBS_combine["ping_time"][0].values,
            utc=True,
        ),
        last_ping_time=pd.to_datetime(
            ds_MVBS_combine["ping_time"][-1].values,
            utc=True,
        ),
    )


@dataclass(frozen=True)
class ComputeNASCWorkItem:
    """One prediction output to convert into a NASC dataset."""

    nasc_filename: str
    prediction: PredictHakeResult


@dataclass(frozen=True)
class ComputeNASCSettings:
    """Settings shared by a batch of NASC computations."""

    output_directory: str
    range_bin: str = "10m"
    dist_bin: str = "0.5nmi"


@dataclass(frozen=True)
class ComputeNASCResult:
    """Metadata describing one successfully created NASC store."""

    nasc_filename: str


def compute_NASC(
    item: ComputeNASCWorkItem,
    settings: ComputeNASCSettings,
) -> ComputeNASCResult:
    """Compute and save NASC from one hake-prediction result."""
    # Apply mask based on threshold
    ds_MVBS_combine_masked = ep.mask.apply_mask(
        source_ds=item.prediction.mvbs_dataset,
        mask=item.prediction.hake_prediction,
        var_name="Sv",
        fill_value=np.nan,
    )

    # Compute NASC from MVBS and hake prediction
    ds_NASC = ep.commongrid.compute_NASC(
        ds_Sv=ds_MVBS_combine_masked,
        range_bin=settings.range_bin,
        dist_bin=settings.dist_bin,
    )

    # Save to zarr
    ds_NASC.to_zarr(
        store=Path(settings.output_directory) / item.nasc_filename,
        mode="w",
        consolidated=True,
    )
    return ComputeNASCResult(nasc_filename=item.nasc_filename)
