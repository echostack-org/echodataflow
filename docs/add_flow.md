# How to add a flow

To run a new flow through Echodataflow's orchestration mechanisms, write the flow, register its entrypoint, and include it in paired parameter and deployment recipes. This guide walks through those steps with a small example that appends the current UTC time for two labels to one CSV file every five minutes. See [Development](development.md) for setting up your environment and running tests.





## Writing a minimum flow

A flow is a Python function decorated with `@flow`; a task is a function decorated with `@task`. A flow can contain ordinary computational steps and run on a schedule or in response to a trigger without defining any tasks.

Wrapping computational steps in tasks gives observability of their execution states and logs in the Prefect dashboard. Splitting independent work into multiple tasks also allows concurrent or parallel execution through a [task runner](#recipe-spec_task-runner). A task can combine several operations when they belong in one execution unit. See [Prefect terminology](prefect_terminology.md) for the orchestration concepts.

The example below separates an operation, its task wrapper, and a flow that submits two independent tasks. Each task returns a labeled timestamp. The flow collects the results from the tasks and writes them together to one CSV file.




## 1. Add the computational steps in `operations/`

Create `src/echodataflow/operations/operations_example.py`:

```python
import csv
from datetime import datetime, timezone
from pathlib import Path


def record_timestamp(label: str) -> dict[str, str]:
    """Return a label and its current UTC timestamp."""
    return {
        "label": label,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }


def append_timestamps(csv_path: str, rows: list[dict[str, str]]) -> None:
    """Append collected timestamp rows to a CSV file."""
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_header = not path.exists() or path.stat().st_size == 0

    with path.open("a", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["label", "timestamp_utc"])
        if needs_header:
            writer.writeheader()
        writer.writerows(rows)
```

These ordinary Python functions can run and be tested without Prefect.
- `record_timestamp` produces one result
- `append_timestamps` creates the output directory if necessary, writes the header once, and appends a batch of rows




## 2. Add a task that wraps the operation

Create `src/echodataflow/tasks/tasks_example.py`:

```python
from prefect import task

from echodataflow.operations.operations_example import record_timestamp


@task(log_prints=True)
def task_record_timestamp(label: str) -> dict[str, str]:
    """Record one labeled timestamp as a Prefect task."""
    return record_timestamp(label)
```

The wrapper exposes the operation as an observable Prefect task without duplicating its implementation.





## 3. Add a flow that runs the task

Create `src/echodataflow/flows/flows_example.py`:

```python
from prefect import flow

from echodataflow.deployment.task_runners import task_runner_from_environment
from echodataflow.operations.operations_example import append_timestamps
from echodataflow.tasks.tasks_example import task_record_timestamp


@flow(log_prints=True, task_runner=task_runner_from_environment())
def flow_record_timestamps(csv_path: str, labels: list[str]) -> list[dict[str, str]]:
    """Record timestamps in parallel, then append the collected rows once."""
    if not labels or len(labels) != len(set(labels)):
        raise ValueError("Provide at least one label, with no duplicates.")

    futures = [task_record_timestamp.submit(label) for label in labels]
    rows = [future.result() for future in futures]
    append_timestamps(csv_path, rows)
    return rows
```

`.submit()` sends each task to the flow's task runner. All tasks are submitted before the flow waits for their results, allowing independent tasks to execute concurrently. Calling `task_record_timestamp(label)` directly would execute each call synchronously instead.

Once every task succeeds, the flow calls `append_timestamps` once to write all results. If a task fails, collecting its result raises an exception before the CSV write begins. Rows follow the input label order, regardless of task completion order.

`task_runner_from_environment()` reads the configuration supplied by the deployment recipe when the worker imports the module. Without an explicit runner configuration, it returns Prefect's default `ThreadPoolTaskRunner`. Using this helper is necessary for this flow to support the recipe's `task_runner` setting.


(add-flow_quick-check)=
For a quick local check, run this from the repository root after installing the package in editable mode:

```shell
python -c 'from echodataflow.flows.flows_example import flow_record_timestamps; flow_record_timestamps("/tmp/echodataflow-example/timestamps.csv", ["clock_a", "clock_b"])'
```

This executes the flow once using the default runner; the next step configures scheduled execution with Dask.

:::{note}
In the above, a flow is called directly as a regular Python function and will start immediately:
- If PREFECT_API_URL is configured, the flow connects to that server.
- If no API URL is configured and ephemeral mode is enabled, Prefect starts a temporary local server automatically. See [ephemeral server settings](https://www.prefect.io/docs/v3/api-ref/settings-ref#serverephemeralsettings).
:::





## 4. Configure recipes

First, add the following entry **inside the existing `FLOW_REGISTRY` dictionary** in `src/echodataflow/deployment/flow_registry.py`. The registry connects a recipe's flow key to its importable entrypoint. Only deployable flows needs to be added to the registry.

```python
"record_timestamps": FlowRegistration(
    entrypoint="echodataflow/flows/flows_example.py:flow_record_timestamps",
    description="Record timestamps in parallel and append them to one CSV file.",
),
```


Save the flow parameters as `params_example.yaml`:

```yaml
flows:
  record_timestamps:
    csv_path: /tmp/echodataflow-example/timestamps.csv
    labels: [clock_a, clock_b]
```

Save the deployment settings as `deploy_example.yaml`:

```yaml
source:
  mode: local

default_work_pool_name: local

flows:
  record_timestamps:  # recipe key
    deployment_name: record-timestamps
    interval: 5
    deployment_concurrency:
      limit: 1
      collision_strategy: ENQUEUE
    task_runner:
      type: dask
      cluster_kwargs:
        n_workers: 2
        threads_per_worker: 1
        processes: true
        memory_limit: 1GiB
```

The `record_timestamps` key must match in both recipes and resolves to the registry entry with the same name. If you want to deploy this same flow function as different flows, set `flow: record_timestamps` in that deployment entry and use different recipe key (see [](recipe-spec_flow)).

`interval: 5` creates a cron schedule at minutes 0, 5, 10, and so on each hour. Each flow run creates a local Dask cluster with two workers, allowing the two timestamp tasks to run in separate processes. The per-deployment concurrency limit prevents runs of this deployment from overlapping; `ENQUEUE` keeps conflicting runs waiting. Within a run, the flow is the only CSV writer, with parallel tasks returning data without writing to files.





## 5. Deployment the workflow
To run this minimum example, spin up a server by running:
```shell
prefect server start
```
And start a worker for the pool in a separate terminal:

```shell
prefect worker start --pool local --type process
```

From your checkout, create the deployment with the paired recipes:

```shell
echodataflow-deploy run \
  --deploy-spec deploy_example.yaml \
  --param-config params_example.yaml
```

Keep the worker running to execute scheduled runs. See [Recipe specification](recipe_spec.md) for details on specifying recipes and [System services](system_services.md) for running the server and worker as ssytem services.






## 6. Check the expected CSV output

After three successful scheduled runs starting with a new file, `timestamps.csv` should contain a header and six rows: one row per label per run. For example:

```csv
label,timestamp_utc
clock_a,2026-10-27T18:00:02.137421+00:00
clock_b,2026-10-27T18:00:02.139852+00:00
clock_a,2026-10-27T18:05:01.845906+00:00
clock_b,2026-10-27T18:05:01.842513+00:00
clock_a,2026-10-27T18:10:02.019332+00:00
clock_b,2026-10-27T18:10:02.021147+00:00
```

Values reflect actual task execution times, so they need not fall exactly on the scheduled minute or be in timestamp order. If [a local check was run from the earlier step](add-flow_quick-check) you may have two more rows to this file.


