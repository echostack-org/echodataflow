"""Runtime task-runner configuration supplied by deployment job variables."""

from __future__ import annotations

import json
import os

from prefect_dask import DaskTaskRunner
from prefect.task_runners import ThreadPoolTaskRunner

from echodataflow.deployment.core import TASK_RUNNER_ENV_VAR


def task_runner_from_environment() -> DaskTaskRunner | ThreadPoolTaskRunner:
    """Build the configured runner, or use Prefect's default thread pool."""
    serialized = os.getenv(TASK_RUNNER_ENV_VAR)
    if serialized is None:
        return ThreadPoolTaskRunner()

    config = json.loads(serialized)
    if config.get("type") != "dask":
        raise ValueError(f"{TASK_RUNNER_ENV_VAR}.type must be 'dask'")
    return DaskTaskRunner(cluster_kwargs=dict(config.get("cluster_kwargs", {})))
