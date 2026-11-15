# Visualize satellite data with ship track

<!-- SPASSO workflow: download products and launch the dashboard -->

This example shows how to use [Echodataflow](https://github.com/echostack-org/echodataflow) to retrieve oceanographic satellite products with a scheduled [Prefect](https://docs.prefect.io/) flow and display them alongside ship navigation in [Echoshader](https://github.com/echostack-org/echoshader). All components run locally, without cloud execution or background system services.

The configuration files are provided in the [SPASSO recipe directory](https://github.com/echostack-org/echodataflow-recipes/tree/main/recipes_nrt/spasso) of the **echodataflow-recipes** repository. This walkthrough uses a fixed example dataset; [Section 6](#6-switch-to-operational-sftp-downloads) explains how to connect to a live source.

> **Note:** This example assumes that `echodataflow` and `echodataflow-recipes` are cloned into sibling directories.

:::{note} About SPASSO [SPASSO](https://github.com/OceanCruises/SPASSO) (Software Package for an Adaptive Satellite-based Sampling for Oceanographic cruises) supports adaptive sampling at sea by providing satellite-derived environmental fields and Eulerian and Lagrangian diagnostics of ocean circulation. These maps help identify fronts, eddies, and filaments and guide in situ observations. See [Rousselet et al. (2025), *A Software Package for an Adaptive Satellite-Based Sampling for Oceanographic Cruises (SPASSOv2.0): Tracking Fine-Scale Features for Physical and Biogeochemical Studies*](https://doi.org/10.1175/JTECH-D-24-0071.1). **Echodataflow downloads existing SPASSO products; it does not calculate the diagnostics.**
:::

We recommend using [Visual Studio Code (VS Code)](https://code.visualstudio.com/) with its integrated terminals to run the example.

### Before you start: three terminals

The example uses **three separate terminals**, because the Prefect server, worker, and Echoshader app are long-running processes. The deployment command runs once and then finishes, so its terminal can be reused for the app.

| Terminal | What runs there | Why it stays open |
| --- | --- | --- |
| **1** | Prefect server | Provides the orchestration API and dashboard |
| **2** | Prefect worker | Executes the scheduled SPASSO download flow |
| **3** | Deployment command, then Echoshader app | Registers the flow first, then runs the visualization app |

Keep Terminals **1-2** open while running the example. For **live ship navigation**, an additional terminal can run a navigation watcher; the supplied synthetic track does not need one. No system services or Docker setup are required.

For the commands below, open VS Code in the **parent directory containing both repositories**, for example:

```text
my_echopype/dev/
├── echodataflow/
└── echodataflow-recipes/
```

Each step shows the required `cd` command. If your terminal opens somewhere else, first move to this parent directory.

> **Windows users:** Select the **PowerShell** tab for multiline commands, or **Bash** for Git Bash, Linux, or macOS. PowerShell uses a trailing backtick (`` ` ``), while Bash uses `\`. If VS Code automatically activates an unrelated Python environment, disable its automatic terminal activation or deactivate it before running `conda activate echodataflow`. Verify the interpreter with `python -c "import sys; print(sys.executable)"`.

## 1. Prepare the environment

Open **Terminal 1**, move into the `echodataflow` repository, and activate the environment:

```sh
cd echodataflow
conda activate echodataflow
```

If the environment is not configured, follow the [Installation](installation.md) guide first. Check that the main packages are installed:

```sh
python -m pip show echodataflow echopype echoshader prefect
python -m pip check
```

The expected package versions are documented in the [echodataflow dependency configuration](https://github.com/echostack-org/echodataflow/blob/main/pyproject.toml). In particular, use Python 3.13 or 3.14 and a compatible installation of Prefect and Echoshader.

## 2. Start Prefect

[Prefect](https://docs.prefect.io/) schedules the download flow: its server provides the API and dashboard, while a worker executes flows locally. We start them manually in separate terminals.

**Terminal 1 — Start the Prefect server**

In Terminal 1 (already inside `echodataflow`), run:

```sh
prefect server start
```

**Terminal 2 — Start the Prefect worker**

Open a **second terminal** at the parent directory, move into `echodataflow`, and activate the environment:

```sh
cd echodataflow
conda activate echodataflow
```

Connect Prefect to the local server:

```sh
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

If the `local` work pool does not already exist, create it:

```sh
prefect work-pool create --type process local
```

Start the worker:

```sh
prefect worker start --pool local --type process
```

Open the [Prefect dashboard](http://127.0.0.1:4200) and check that the worker appears under **Work Pools → local → Workers**. Keep both terminals open.

> **Troubleshooting:** If Prefect reports a database error, first check that only one local server is running. Prefect stores its local orchestration state in `~/.prefect/prefect.db`; do not remove it while the server is running. See the [Prefect documentation](https://docs.prefect.io/) for further troubleshooting.

## 3. Review the supplied recipe

### Recipe files

The files in [`recipes_nrt/spasso/`](https://github.com/echostack-org/echodataflow-recipes/tree/main/recipes_nrt/spasso) define this example:

| File | Purpose |
| --- | --- |
| [`params.yaml`](https://github.com/echostack-org/echodataflow-recipes/blob/main/recipes_nrt/spasso/params.yaml) | Download source, output directory, and retry settings |
| [`deploy.yaml`](https://github.com/echostack-org/echodataflow-recipes/blob/main/recipes_nrt/spasso/deploy.yaml) | Deployment name and schedule |
| [`app.yaml`](https://github.com/echostack-org/echodataflow-recipes/blob/main/recipes_nrt/spasso/app.yaml) | Echoshader products, navigation, and dashboard settings |

### Input data: satellite products and ship navigation

**Satellite products.** This walkthrough downloads a [fixed public ZIP archive](https://github.com/echostack-org/echodataflow-recipes/releases/download/data-v1/spasso-20260918.zip) containing seven NetCDF products dated **September 18, 2026**. These include physical and biogeochemical fields (such as SST, SSS, and chlorophyll) and circulation diagnostics provided by SPASSO (such as kinetic energy, Okubo–Weiss, and FTLE). The archive is a **snapshot**, not a live data feed: scheduled runs demonstrate how Prefect executes the flow, but they do not discover products from newer dates.

**Ship navigation.** Echoshader displays an illustrative ship position and route using the synthetic navigation data included in `recipes_nrt/spasso/resources/navigation/`. The supplied `app.yaml` already points to this directory, so no additional configuration or navigation watcher is required. The track is fixed, and navigation timestamps may differ from the satellite-product timestamps.

For a real cruise, SPASSO produces updated satellite products and diagnostics, and the ship provides a live navigation stream. The Echodataflow flow downloads existing SPASSO products; it does not compute their diagnostics. See [Section 6](#6-switch-to-operational-sftp-downloads) for the changes needed to access live SPASSO data and navigation.

### How the SPASSO workflow works

The `fetch_spasso` flow downloads the configured products into the local `data/` directory. Repeated runs reuse existing files when no new download is needed. Echoshader then reads those files, displays the selected maps, and overlays the ship track using the Parquet navigation files.

The supplied `params.yaml` configures the public example archive, local output path, and retries (see the [recipe file](https://github.com/echostack-org/echodataflow-recipes/blob/main/recipes_nrt/spasso/params.yaml)):

```yaml
flows:
  fetch_spasso:
    asset_url: https://github.com/echostack-org/echodataflow-recipes/releases/download/data-v1/spasso-20260918.zip
    path_main: ../../echodataflow-recipes/recipes_nrt/spasso/data
    task_retries: 3
    task_retry_delay_seconds: 30
```

The relative `path_main` is resolved from the deployment's working directory, `echodataflow/src`. The output directory is created automatically, and this public ZIP mode does not require SFTP credentials.

The supplied `deploy.yaml` defines the scheduled deployment:

```yaml
flows:
  fetch_spasso:
    deployment_name: template_spasso
    interval: 5
```

The schedule checks for the example products every five minutes; it **does not mean new satellite data become available every five minutes**.

## 4. Apply the deployment

**Terminal 3 — Deploy the flow**

Open a **third terminal** at the parent directory, move into `echodataflow`, and activate the environment:

```sh
cd echodataflow
conda activate echodataflow
```

Register the SPASSO deployment so Prefect knows which flow to run, on what schedule, and with which recipe parameters:

::::{tab-set}

:::{tab-item} PowerShell
```powershell
python -m echodataflow.deployment.deploy_cli run `
    --default-work-pool-name local `
    --param-config ../echodataflow-recipes/recipes_nrt/spasso/params.yaml `
    --deploy-spec ../echodataflow-recipes/recipes_nrt/spasso/deploy.yaml
```
:::

:::{tab-item} Bash
```bash
python -m echodataflow.deployment.deploy_cli run \
    --default-work-pool-name local \
    --param-config ../echodataflow-recipes/recipes_nrt/spasso/params.yaml \
    --deploy-spec ../echodataflow-recipes/recipes_nrt/spasso/deploy.yaml
```
:::

::::

Open [Prefect Deployments](http://127.0.0.1:4200/deployments) and select **`template_spasso`**. With the worker online, the deployment should become **Ready**, with an **Active** schedule. Follow the next run in **Runs** and inspect its logs. On successful completion, the seven downloaded products should be available under `echodataflow-recipes/recipes_nrt/spasso/data/`.

The deployment command registers the flow and exits; **Terminal 3 can now be reused** to launch Echoshader.

## 5. Launch Echoshader

The supplied [`app.yaml`](https://github.com/echostack-org/echodataflow-recipes/blob/main/recipes_nrt/spasso/app.yaml) configures the products, dashboard, and navigation-file location. Echoshader **visualizes the downloaded outputs; it does not run the download flow**.

**Terminal 3 — Launch the app**

Reuse Terminal 3 (currently inside `echodataflow`), and switch to the recipes repository:

```sh
cd ../echodataflow-recipes
python -m echoshader.app.launcher ./recipes_nrt/spasso/app.yaml
```

Open the [SPASSO dashboard](http://127.0.0.1:1804/spasso). Keep this terminal open while using the app. You should see environmental fields and the simulated ship track once the downloaded products are available.

> **First launch:** Cartopy may download geographic map assets. Allow these downloads before using the dashboard offline.

## 6. Switch to operational SFTP downloads

The fixed archive in [Section 3](#input-data-satellite-products-and-ship-navigation) is designed for repeatable testing and will never receive newer products. **This section changes the input source for a real cruise:** configure the SPASSO product server and provide live navigation data instead of the fixed example inputs. Contact the [SPASSO team](https://spasso.mio.osupytheas.fr/) to coordinate access and product delivery.

In `echodataflow-recipes/recipes_nrt/spasso/params.yaml`, remove `asset_url` and set the SFTP connection parameters under `flows.fetch_spasso`:

```yaml
flows:
  fetch_spasso:
    host: YOUR_SFTP_HOST
    port: YOUR_SFTP_PORT
    remote_path: path/to/cruise/products/
    path_main: ../../echodataflow-recipes/recipes_nrt/spasso/data
    product_patterns:
      - Copernicus_PHY
      - Copernicus_SST_L4
      - Copernicus_SSS_L4
      - Copernicus_CHL_L4
      - FTLE_Copernicus_PHY
      - KE_Copernicus_PHY
      - OW_Copernicus_PHY
    username_secret: spasso-username
    password_secret: spasso-password
    task_retries: 3
    task_retry_delay_seconds: 30
```

Use the host, numerical port, and remote directory supplied for your cruise. Create Prefect **Secret** blocks named `spasso-username` and `spasso-password` on the same server; do not put credentials in the recipe. The active download flow selects the latest `YYYYMMDD_<product>.nc` file for each requested product based on the filename date. Missing or size-changed files are downloaded; previously downloaded dates are retained. Older remote dates are not automatically backfilled by the latest-only selection.

For **live ship navigation**, replace the static Parquet track described in Section 3 with continuously recorded positions. Configure the navigation watcher to read valid `$GPRMC` NMEA 0183 sentences from the ship's serial stream and write `ship_navigation_*.parquet` files into the same `recipes_nrt/spasso/data/navigation/` folder used by Echoshader. The watcher records UTC time, position, speed, and course and can run in a fourth terminal. It is not needed for the synthetic example. See the [echodataflow watchers](https://github.com/echostack-org/echodataflow/tree/main/src/echodataflow/watchers) for the navigation implementation.

Finally, **reapply the deployment** using the command from [Section 4](#4-apply-the-deployment), from the `echodataflow` directory. The running Echoshader app can continue using the same local data root.
