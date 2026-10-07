# Development

Echodataflow uses a `src` layout, Prefect for orchestration, Pytest for tests, and Numpydoc
docstrings. Follow [Installation](installation.md#install-for-development) to set up a
Conda environment and install the editable package with development and mission tools.

## Create the environment

Fork the repository if you intend to submit changes, then clone your fork:

```shell
git clone https://github.com/YOUR-USER/echodataflow.git
cd echodataflow
git remote add upstream https://github.com/echostack-org/echodataflow.git
```

Use the environment and editable installation commands in the
[installation guide](installation.md), applying them to your fork's checkout.

Install the pre-commit hooks:

```shell
pre-commit install
pre-commit run --all-files
```

## Run tests

Run the complete suite from the repository root:

```shell
pytest
```

During development, select a focused module first:

```shell
pytest tests/deployment/test_deploy_engine.py
pytest tests/operations/test_operations_raw_to_Sv.py
```

Tests should not require a live Prefect server unless they explicitly exercise integration
with one. Mock network and Prefect clients for unit tests, and keep external-data integration
tests clearly identified.

## Understand the package layers

```text
src/echodataflow/
├── operations/   scientific and file-processing functions
├── tasks/        Prefect task wrappers
├── flows/        workflow composition and state
├── deployment/   recipe validation and Prefect deployment creation
├── services/     operating-system service templates
└── utils/        shared utilities and constants
```

Keep scientific logic as low in this stack as practical. An operation is easier to test and
reuse than logic embedded directly in a Prefect flow.

## Add a workflow step

Use the following sequence when a new step performs a unit of processing and should be
deployable.

### 1. Add the operation

Place ordinary Python logic in the domain's module under `operations/`. Existing operations
use dataclasses to make the boundary explicit:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class ExampleWorkItem:
    input_path: str


@dataclass(frozen=True)
class ExampleSettings:
    output_dir: str


@dataclass(frozen=True)
class ExampleResult:
    output_path: str


def run_example(item: ExampleWorkItem, settings: ExampleSettings) -> ExampleResult:
    ...
```

The operation should not read Prefect runtime state, choose schedules, or depend on a work
pool. Validate inputs and return enough information for the caller to log or compose the
result.

### 2. Add the task wrapper

Put the Prefect wrapper in the matching module under `tasks/`:

```python
from prefect import task


@task
def task_example(item: ExampleWorkItem, settings: ExampleSettings) -> ExampleResult:
    return run_example(item, settings)
```

Configure retries at the layer that understands whether a failure is transient. Avoid
duplicating the processing implementation in the task.

### 3. Compose the flow

Add a flow to the appropriate domain module under `flows/`. The flow translates user-facing
parameters into work items and settings, invokes tasks, and coordinates workflow-level
decisions:

```python
from prefect import flow


@flow(log_prints=True)
def flow_example(input_path: str, output_dir: str) -> ExampleResult:
    return task_example(
        ExampleWorkItem(input_path=input_path),
        ExampleSettings(output_dir=output_dir),
    )
```

Use `time_offset_seconds` only if the flow participates in historical replay. Make repeated
runs idempotent where possible, and define how partial results are handled.

### 4. Register deployable flows

Add a stable key to `FLOW_REGISTRY` in
`src/echodataflow/deployment/flow_registry.py`:

```python
"example": FlowRegistration(
    entrypoint="echodataflow/flows/flows_example.py:flow_example",
    description="Process one example input.",
),
```

Only register public deployable flows. Helper flows and internal functions do not need recipe
names.

### 5. Add paired recipe entries

The parameter recipe key and deployment recipe key must match. The deployment key defaults
to the registry key; set `flow: example` when multiple recipe entries reuse the same flow.

### 6. Test each layer

Add:

- operation unit tests for successful, empty, and invalid inputs;
- task or flow tests for orchestration behavior that is not covered by the operation;
- deployment tests for registry resolution and recipe validation; and
- a small documentation example if users need new configuration.

Update the [Workflow reference](reference.md) and public Numpydoc docstrings in the same
change.

## Build the documentation

Jupyter Book 2 uses the MyST engine and `docs/myst.yml`; it does not use Sphinx
`_config.yml`, `_toc.yml`, or `conf.py` files.

Preview with live reload:

```shell
cd docs
jupyter book start
```

Run the strict production check before submitting documentation changes:

```shell
jupyter book build --html --strict
```

Keep narrative documentation in the small set of top-level Markdown files. Supporting recipe
and data fixtures belong in `docs/examples/`.
