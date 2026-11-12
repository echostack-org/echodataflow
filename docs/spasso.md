# Visualize satellite data with ship track

<!-- SPASSO workflow: download products and launch the dashboard -->

[SPASSO](https://github.com/OceanCruises/SPASSO) (Software Package for an Adaptive Satellite-based Sampling for Oceanographic cruises) uses satellite-derived ocean products to support adaptive sampling during oceanographic cruises. It provides maps of environmental conditions and computes Eulerian and Lagrangian diagnostics from near-real-time altimetric data to help identify ocean fronts, eddies, and filaments. See [Rousselet et al. (2025), *A Software Package for an Adaptive Satellite-Based Sampling for Oceanographic Cruises (SPASSOv2.0): Tracking Fine-Scale Features for Physical and Biogeochemical Studies*](https://doi.org/10.1175/JTECH-D-24-0071.1).

This walkthrough uses Echodataflow to retrieve SPASSO NetCDF products through a scheduled Prefect deployment, then displays them with [Echoshader](https://github.com/echostack-org/echoshader). The configuration files are provided in `recipes_nrt/spasso/` in the companion [echodataflow-recipes repository](https://github.com/echostack-org/echodataflow-recipes/tree/main/recipes_nrt/spasso):

| File | Purpose |
| --- | --- |
| `params.yaml` | Download source, output directory, and retry settings |
| `deploy.yaml` | Deployment name and schedule |
| `app.yaml` | Echoshader products, navigation, and dashboard settings |

## Live deployments and the example dataset

For a real cruise deployment, SPASSO provides the latest available environmental products and produces near-real-time (NRT) diagnostics from altimetric data. At the interval configured by the user, the Echodataflow flow checks the SPASSO server and retrieves the latest available file for each requested product. Previously downloaded files are retained, and unchanged files are skipped. Echoshader displays the downloaded products and diagnostics alongside ship navigation.

The download flow retrieves the diagnostics produced by SPASSO; it does not calculate those diagnostics itself.

This walkthrough uses a public GitHub release asset containing seven products for September 18, 2026, together with simulated ship navigation. The asset is a fixed snapshot: repeated flow runs reuse the cached dataset and will never discover a newer dataset through this example configuration. The schedule demonstrates deployment execution, rather than incoming data availability.

To arrange a real cruise deployment with SPASSO and onboard visualization, contact the authors to coordinate cruise setup, data access, and product delivery.

## 1. Prepare the environment

Follow the [installation guide](installation.md) to install Echodataflow and its dependencies, including Echoshader, and obtain the recipes. See [System services](system_services.md) to configure persistent Prefect services on macOS.

The example assumes sibling `echodataflow` and `echodataflow-recipes` repository checkouts. The relative download path below is resolved from the deployment's working directory, `echodataflow/src`.

Activate the environment:

```shell
conda activate echodataflow
```

## 2. Check the Prefect server and worker

If you completed the service setup, the server and worker should already be running.

Open the [Prefect dashboard](http://127.0.0.1:4200). If it loads, the server is running. Under **Work Pools → local → Workers**, check that a worker is online.

If both are running, proceed to [3. Example recipe](#3-example-recipe).
Otherwise, see [Start Prefect (optional)](#start-prefect-optional).

### Start Prefect (optional)

If you configured system services, start or restart those services. Otherwise, start the server in a separate terminal:

```shell
conda activate echodataflow
prefect server start
```

In another terminal, activate the environment and connect to the local server:

```shell
conda activate echodataflow
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

Check the available work pools:

```shell
prefect work-pool ls
```

If `local` does not exist, create it:

```shell
prefect work-pool create --type process local
```

Start a worker if none is already online:

```shell
prefect worker start --pool local
```

Keep the server and worker terminals open while the workflow runs.

## 3. Example recipe

The files in `recipes_nrt/spasso/` are already configured for this example. No changes are needed when using the sibling repository layout described above.

`params.yaml` specifies the public GitHub dataset, download directory, and retry settings:

```yaml
flows:
  fetch_spasso:
    asset_url: https://github.com/echostack-org/echodataflow-recipes/releases/download/data-v1/spasso-20260918.zip
    path_main: ../../echodataflow-recipes/recipes_nrt/spasso/data
    task_retries: 3
    task_retry_delay_seconds: 30
```

The output directory is created automatically. Public ZIP mode does not require SFTP credentials.

`deploy.yaml` schedules the `template_spasso` deployment every five minutes:

```yaml
flows:
  fetch_spasso:
    deployment_name: template_spasso
    interval: 5
```

The deployment defaults to local source mode and uses the work pool supplied by the deployment command below. The GitHub asset provides the example dataset.

## 4. Apply the deployment

From the root of your local `echodataflow` repository, run:

```powershell
python -m echodataflow.deployment.deploy_cli run `
    --default-work-pool-name local `
    --param-config ../echodataflow-recipes/recipes_nrt/spasso/params.yaml `
    --deploy-spec ../echodataflow-recipes/recipes_nrt/spasso/deploy.yaml
```

Open the [Prefect dashboard](http://127.0.0.1:4200), select **Deployments**, and click **template_spasso**. Check that the deployment is **Ready** and its five-minute schedule is **Active**.

The worker will automatically pick up the next scheduled run. Wait until its scheduled time, then open **Runs** and select the run associated with `template_spasso` to follow its progress.

Once it reaches **Completed**, check its **Logs** for `SPASSO: 7 files available`. The downloaded products are now in `echodataflow-recipes/recipes_nrt/spasso/data/`, ready for visualization.

### Ship navigation

Echoshader reads navigation from `ship_navigation_*.parquet` files in `data/navigation/`, relative to the SPASSO recipe directory. It overlays the ship’s latest position and track on the environmental maps and periodically reloads the navigation files.

For this example, place the supplied synthetic navigation file, `ship_navigation_20260918_0000.parquet`, in that directory before launching the app. It contains a fixed example track; no navigation watcher is required. This file is supplied separately from the environmental-data ZIP.

For a real cruise, `scripts/navigation/watch_ship_navigation.py` reads valid `$GPRMC` sentences from an NMEA 0183 serial stream. It records UTC timestamps, latitude, longitude, speed, and course, saving one Parquet file per completed minute. Configure its output directory to the same `data/navigation/` directory used by the dashboard.

The example and live deployment therefore use the same navigation-file reader.

## 5. Launch Echoshader

The supplied `app.yaml` already defines the products, dashboard, and navigation-file location.

In another PowerShell terminal, activate the environment and move to the root of your local `echodataflow-recipes` repository. Then run:

```powershell
conda activate echodataflow
python -m echoshader.app.launcher ./recipes_nrt/spasso/app.yaml
```

Open <http://127.0.0.1:1804/spasso> and keep the terminal open. The dashboard displays environmental products and current diagnostics alongside simulated navigation. Navigation timestamps can differ from the fixed September 2026 product date.

On first use, Cartopy may download geographic map assets. Allow internet access for these downloads before using the dashboard offline.

## 6. Switch to operational SFTP downloads

For a live source, remove `asset_url` and configure the SFTP parameters under `flows.fetch_spasso`:

```yaml
flows:
  fetch_spasso:
    host: YOUR_SFTP_HOST
    port: 0000
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

Use the host, port, and remote directory provided for your dataset. Create Prefect Secret blocks named `spasso-username` and `spasso-password` on the same Prefect server, with your SFTP credentials. Keep credentials out of recipe files.

Reapply the deployment after changing its parameters. The running Echoshader app can continue to use the same local data root.

In SFTP mode, each run lists the remote directory and selects the newest `YYYYMMDD_<product>.nc` file for each requested product. It selects by filename date, not by today's date. Missing or size-changed files are downloaded; previously downloaded dates remain locally. Older remote files are not backfilled by this latest-only selection.
