"""Reusable Prefect tasks for hake prediction and NASC processing."""

from prefect import get_run_logger, task

from echodataflow.operations.operations_predict_hake import (
    ComputeNASCResult,
    ComputeNASCSettings,
    ComputeNASCWorkItem,
    PredictHakeResult,
    PredictHakeSettings,
    PredictHakeWorkItem,
    compute_NASC,
    predict_hake,
)


@task(log_prints=True)
def task_predict_hake(
    item: PredictHakeWorkItem,
    settings: PredictHakeSettings,
) -> PredictHakeResult:
    """Predict hake for one MVBS time slice as a Prefect task."""
    return predict_hake(item, settings)


@task(log_prints=True)
def task_compute_NASC(
    item: ComputeNASCWorkItem,
    settings: ComputeNASCSettings,
) -> ComputeNASCResult:
    """Compute NASC for one prediction result as a Prefect task."""
    logger = get_run_logger()
    logger.info(f"Saving NASC to zarr: {item.nasc_filename}")
    return compute_NASC(item, settings)
