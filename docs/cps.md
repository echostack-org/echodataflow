# Detect coastal pelagic species in acoustic data

<!-- CPS workflow: replay NCEI RAW data and visualize transects -->

NOAA's acoustic-trawl surveys monitor coastal pelagic species (CPS) and Pacific hake along the US West Coast. Since 2025, the surveys have been combined in the [Integrated West Coast Pelagics Survey](https://www.fisheries.noaa.gov/west-coast/science-data/integrated-west-coast-pelagics-survey). This example demonstrates a shipboard, **edge-only** Python workflow with processing objectives similar to NOAA's Echoview-based approach (see the [SWFSC processing procedures](https://nmfs-swfsc-ast.github.io/AST-Lab-Manual/content/processing.html)). Processing, scheduling, and visualization run locally, without cloud execution.

The configurations and resources needed to run this example are provided in the [CPS recipe directory](https://github.com/echostack-org/echodataflow-recipes/tree/main/recipes_nrt/cps) of the **echodataflow-recipes** repository.

> **Note:** This example assumes that `echodataflow` and `echodataflow-recipes` are cloned into sibling directories.

This example replays SH2407 EK80 recordings from the public NOAA archive (`noaa-wcsd-pds`). Scheduled flows download RAW data, convert them to Sv, prepare CPS products, integrate transects, and publish full-resolution Zarr datasets for [Echoshader](https://github.com/echostack-org/echoshader). [Prefect](https://docs.prefect.io/) orchestration and the `raw2Sv` flow are also used by the edge-to-cloud hake workflow; CPS processing, transect integration, and visualization are specific to this example.

> **Disk space:** Make sure you have sufficient disk space before starting the example. The dataset includes **93 RAW files totalling 9.69 GB**, and additional space is needed for intermediate Sv and CPS products, NASC outputs, and full-resolution transect Zarr stores.

We recommend using [Visual Studio Code (VS Code)](https://code.visualstudio.com/) with its integrated terminals to manage the workflow. The following steps explain how to run each component locally, from the Prefect server and worker to the transect watcher and Echoshader visualization.

### Before you start: four terminals

This workflow uses **four separate terminals** because several components need to keep running at the same time. Each terminal runs a different part of the local workflow:

| Terminal | What runs there | Why it stays open |
| --- | --- | --- |
| **1** | Prefect server | Provides the orchestration API and dashboard |
| **2** | Prefect worker | Executes scheduled processing flows |
| **3** | Transect watcher | Monitors and registers survey transect intervals |
| **4** | Deployment command, then Echoshader app | Registers the flows first, then runs the visualization app |

Keep Terminals **1-3** running throughout the example. The deployment command in Terminal **4** finishes after registering the flows, so you can reuse that terminal to launch Echoshader. **No background Windows/Linux services or Docker setup are required.**

For the commands below, open VS Code in the **parent directory containing both repositories**, for example:

```text
my_echopype/dev/
├── echodataflow/
└── echodataflow-recipes/
```

A new VS Code terminal should then start in that parent directory. Each step explicitly shows when to change folders using `cd`. If your terminal opens elsewhere, first navigate to the parent directory containing the two repositories.

## 1. Prepare the environment

Open **Terminal 1**, move into the `echodataflow` repository, and activate the `echodataflow` environment:

```sh
cd echodataflow
conda activate echodataflow
```

If the environment is not configured, follow the [Installation](installation.md) instructions before continuing. Once activated, check that the main packages are installed and inspect their versions:

```sh
python -m pip show echodataflow echopype echoshader prefect
```

| Package | Expected version |
| --- | --- |
| Python | >=3.13,<3.15 (3.13 or 3.14) |
| echodataflow | Local editable checkout (version may be a development build) |
| echopype | 0.12.0 |
| echoshader | 0.2.3 |
| echoregions | 0.2.4 |
| Prefect | 3.8.4 |

The installed versions should satisfy the dependency requirements in the [`echodataflow/pyproject.toml`](https://github.com/echostack-org/echodataflow/blob/main/pyproject.toml). You can also check for missing or incompatible dependencies:

```sh
python -m pip check
```

## 2. Start Prefect

[Prefect](https://docs.prefect.io/) schedules the processing flows: the server provides the API and dashboard, while a worker runs the flows locally. We'll start all components manually in separate terminals.

**Terminal 1 — Start the Prefect server**

In your first terminal (already inside `echodataflow`), run:

```sh
prefect server start
```

> **Troubleshooting:** If Prefect reports a database error, first check that only one local server is running. Prefect stores its local orchestration state in `~/.prefect/prefect.db`; do not remove it while the server is running. See the [Prefect documentation](https://docs.prefect.io/) for further troubleshooting.

**Terminal 2 — Start the Prefect worker**

Open a **second terminal**, move into `echodataflow`, and activate the environment again:

```sh
cd echodataflow
conda activate echodataflow
```

If the `local` work pool does not exist yet, create it after the server has started:

```sh
prefect work-pool create --type process local
```

Then start the worker:

```sh
prefect worker start --pool local --type process
```

Open the [Prefect dashboard](http://127.0.0.1:4200) and check that a worker is online under **Work Pools → local → Workers**.

Keep both terminals open. We'll use a third terminal for the transect watcher and a fourth one to deploy the flows and launch the app.

## 3. Review the supplied recipe

### Recipe files

The files in [`recipes_nrt/cps/`](https://github.com/echostack-org/echodataflow-recipes/tree/main/recipes_nrt/cps) within the `echodataflow-recipes` repository define the example:

| File | Purpose |
| --- | --- |
| [`resources/raw_files.csv`](https://github.com/echostack-org/echodataflow-recipes/blob/main/recipes_nrt/cps/resources/raw_files.csv) | Selected S3 keys, timestamps, and RAW sizes in bytes |
| [`resources/plotSurvey_Survey_Data_Visualizer.csv`](https://github.com/echostack-org/echodataflow-recipes/blob/main/recipes_nrt/cps/resources/plotSurvey_Survey_Data_Visualizer.csv) | Ten one-hour transect windows |
| [`params.yaml`](https://github.com/echostack-org/echodataflow-recipes/blob/main/recipes_nrt/cps/params.yaml) | Input paths and processing parameters |
| [`deploy.yaml`](https://github.com/echostack-org/echodataflow-recipes/blob/main/recipes_nrt/cps/deploy.yaml) | Deployment names and schedules |
| [`app.yaml`](https://github.com/echostack-org/echodataflow-recipes/blob/main/recipes_nrt/cps/app.yaml) | Echoshader views and output locations |

### Input data: acoustic recordings and survey transects

**RAW recordings.** The example uses EK80 files from NOAA's SH2407 survey, archived in the public `noaa-wcsd-pds` bucket. The supplied [`raw_files.csv`](https://github.com/echostack-org/echodataflow-recipes/blob/main/recipes_nrt/cps/resources/raw_files.csv) manifest identifies the 93 files to replay, with their archive keys, timestamps, and sizes. The `copy_raw` flow downloads them in batches to `recipes_nrt/cps/data/raw/`, simulating recordings arriving aboard a ship.

**Survey transects.** The supplied [`plotSurvey_Survey_Data_Visualizer.csv`](https://github.com/echostack-org/echodataflow-recipes/blob/main/recipes_nrt/cps/resources/plotSurvey_Survey_Data_Visualizer.csv) defines ten one-hour transect windows on August 11–14, 2024, using transect IDs and start/end timestamps. The transect watcher reads this file and registers the intervals in the local processing database so the pipeline knows which acoustic records belong to each transect.

### How the CPS processing works

The pipeline follows five main stages:

1. **`copy_raw`** downloads archived RAW files in batches to simulate new shipboard recordings.
2. **`raw2Sv`** converts the RAW recordings into calibrated volume backscattering strength (Sv).
3. **`process_Sv_CPS`** prepares CPS-specific acoustic products, including masking according to the configured processing settings.
4. **`process_transect_CPS`** combines processed data within the transect time windows and calculates the nautical area scattering coefficient (NASC).
5. **`update_cache_CPS`** publishes full-resolution Zarr stores for completed transects; Echoshader reads these directly, without acoustic subsampling.

The transect watcher runs independently of Prefect so changes to the survey transect file can be registered as the workflow runs. Echoshader is a separate visualization app: it reads completed products, rather than processing acoustic data itself. The processing code lives in the [echodataflow repository](https://github.com/echostack-org/echodataflow), while the [CPS parameters](https://github.com/echostack-org/echodataflow-recipes/blob/main/recipes_nrt/cps/params.yaml) control how the example is run. This is an overview of the processing stages, rather than a detailed description of the CPS detection algorithm.

## 4. Start the transect watcher

**Terminal 3 — Start the watcher**

Open a **third terminal**. Starting from the parent directory containing both repositories, move into `echodataflow-recipes` and activate the environment:

```sh
cd echodataflow-recipes
conda activate echodataflow
```

Then start the transect watcher:

::::{tab-set}

:::{tab-item} PowerShell
```powershell
python -m echodataflow.watchers.watch_transect_updates `
    ./recipes_nrt/cps/resources/plotSurvey_Survey_Data_Visualizer.csv `
    --db-path ./recipes_nrt/cps/data/processing.db
```
:::

:::{tab-item} Bash
```bash
python -m echodataflow.watchers.watch_transect_updates \
    ./recipes_nrt/cps/resources/plotSurvey_Survey_Data_Visualizer.csv \
    --db-path ./recipes_nrt/cps/data/processing.db
```
:::

::::

Keep this terminal open. The watcher monitors the transect CSV and registers its intervals in the local processing database. The CPS flows use these intervals to group incoming acoustic data into transects. **The watcher runs independently of Prefect.**

## 5. Apply the deployments

**Terminal 4 — Deploy the flows**

Open a **fourth terminal**. Starting from the parent directory containing both repositories, move into `echodataflow` and activate the environment:

```sh
cd echodataflow
conda activate echodataflow
```

The deployment command registers the five flows and their schedules with Prefect; the worker then executes them as inputs become available. Run:

::::{tab-set}

:::{tab-item} PowerShell
```powershell
python -m echodataflow.deployment.deploy_cli run `
    --default-work-pool-name local `
    --param-config ../echodataflow-recipes/recipes_nrt/cps/params.yaml `
    --deploy-spec ../echodataflow-recipes/recipes_nrt/cps/deploy.yaml
```
:::

:::{tab-item} Bash
```bash
python -m echodataflow.deployment.deploy_cli run \
    --default-work-pool-name local \
    --param-config ../echodataflow-recipes/recipes_nrt/cps/params.yaml \
    --deploy-spec ../echodataflow-recipes/recipes_nrt/cps/deploy.yaml
```
:::

::::

Deployments tell Prefect which flows to run, with which parameters and schedules. In the [Prefect dashboard](http://127.0.0.1:4200), check that the five `template-cps-*` deployments are **Ready** with active schedules (the worker must be polling the `local` pool):

| Flow | Purpose |
| --- | --- |
| `copy_raw` | Download the next batch of archived RAW files (simulating acquisition) |
| `raw2Sv` | Convert RAW files to calibrated Sv (shared with the hake workflow) |
| `process_Sv_CPS` | Prepare per-file CPS products |
| `process_transect_CPS` | Assemble transects and calculate NASC |
| `update_cache_CPS` | Prepare visualization products for Echoshader |

The workflow is now running! You can follow its runs and logs in the [Prefect dashboard](http://127.0.0.1:4200). Each stage picks up available inputs on its next scheduled run; runs without new work are normal.

## 6. Launch Echoshader

For this CPS example, visualization is handled by [Echoshader](https://github.com/echostack-org/echoshader), which displays acoustic data, transect views, and processing status as new products become available. Echoshader **visualizes the outputs; it does not run the processing flows**.

Reuse **Terminal 4**. The deployment command has finished, so switch from `echodataflow` to `echodataflow-recipes` (the environment is already activated):

```sh
cd ../echodataflow-recipes
python -m echoshader.app.launcher ./recipes_nrt/cps/app.yaml
```

Once the app has started, open the [CPS dashboard](http://127.0.0.1:1805/cps). Keep the terminal open: the app waits for data and refreshes as products arrive. The **Recent transects** tab automatically displays newly completed transects.

For real shipboard acquisition, disable `copy_raw`, point `raw2Sv.path_raw` at the incoming RAW directory, and use the live survey transect CSV with the watcher.
