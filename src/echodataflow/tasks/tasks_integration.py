"""Reusable Prefect tasks for acoustic-biological integration."""

import pandas as pd
from prefect import task

from echodataflow.operations.operations_integration import (
    ReadNASCSettings,
    ReadNASCWorkItem,
    read_NASC,
)


@task(log_prints=True)
def task_read_NASC(
    item: ReadNASCWorkItem,
    settings: ReadNASCSettings,
) -> pd.DataFrame:
    """Read one NASC store within a Prefect task."""
    return read_NASC(item, settings)
