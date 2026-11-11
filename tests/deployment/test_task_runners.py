import json

from prefect.task_runners import ThreadPoolTaskRunner

from echodataflow.deployment.task_runners import (
    TASK_RUNNER_ENV_VAR,
    task_runner_from_environment,
)


def test_dask_task_runner_uses_runtime_environment(monkeypatch):
    monkeypatch.setenv(
        TASK_RUNNER_ENV_VAR,
        json.dumps(
            {
                "type": "dask",
                "cluster_kwargs": {
                    "n_workers": 4,
                    "threads_per_worker": 1,
                    "processes": True,
                },
            }
        ),
    )

    runner = task_runner_from_environment()

    assert runner.cluster_kwargs == {
        "n_workers": 4,
        "threads_per_worker": 1,
        "processes": True,
    }


def test_task_runner_uses_prefect_default_without_runtime_config(monkeypatch):
    monkeypatch.delenv(TASK_RUNNER_ENV_VAR, raising=False)

    runner = task_runner_from_environment()

    assert isinstance(runner, ThreadPoolTaskRunner)
