# Coastal pelagic species detection

<!-- CPS workflow: replay NCEI RAW data and visualize transects -->

This example replays SH2407 EK80 recordings from the public NCEI bucket `noaa-wcsd-pds`. The `copy_raw` flow downloads selected files into the recipe's `data/raw/` directory. Scheduled flows convert them to Sv, prepare CPS products, assemble transects, and update the Echoshader cache.

The supplied CSV defines ten one-hour transect windows on August 11-14, 2024. Original acquisition timestamps are preserved.

> **Disk space:** The manifest lists **93 RAW files totalling 9.69 GB**. Sv, CPS, NASC, and visualization products need additional space.

## 1. Prepare the environment

Follow [Installation](installation.md). Use sibling `echodataflow` and `echodataflow-recipes` checkouts and activate the environment:

```powershell
conda activate echodataflow
```

## 2. Check Prefect

This walkthrough assumes the Prefect server and a worker for the `local` work pool are running; otherwise, follow [System services](system_services.md) before continuing. Open the [Prefect dashboard](http://127.0.0.1:4200) and check that a worker is online under **Work Pools → local → Workers**.

If started manually, keep the server and worker terminals open. Use another terminal for the transect watcher, and one for deployment commands followed by the app.

## 3. Review the supplied recipe

The files in recipes_nrt/cps/ within the echodataflow-recipes repository define the example:

| File | Purpose |
| --- | --- |
| `resources/raw_files.csv` | Selected S3 keys, timestamps, and RAW sizes in bytes |
| `resources/plotSurvey_Survey_Data_Visualizer.csv` | Ten one-hour transect windows |
| `params.yaml` | Input paths and processing parameters |
| `deploy.yaml` | Deployment names and schedules |
| `app.yaml` | Echoshader views and output locations |

## 4. Start the transect watcher

From the root of `echodataflow-recipes`, in a separate terminal with the environment activated:

```powershell
python scripts/monitoring/watch_transect_updates.py `
    ./recipes_nrt/cps/resources/plotSurvey_Survey_Data_Visualizer.csv `
    --db-path ./recipes_nrt/cps/data/processing.db
```

Keep this terminal open. The watcher registers the supplied transect intervals; processing waits for their acoustic data.

## 5. Apply the deployments

From the root of `echodataflow`, in another activated terminal:

```powershell
python -m echodataflow.deployment.deploy_cli run `
    --default-work-pool-name local `
    --param-config ../echodataflow-recipes/recipes_nrt/cps/params.yaml `
    --deploy-spec ../echodataflow-recipes/recipes_nrt/cps/deploy.yaml
```

In Prefect, check that the five `template-cps-*` deployments are **Ready** with active schedules:

| Flow | Purpose |
| --- | --- |
| `copy_raw` | Download the next batch of RAW files |
| `raw2Sv` | Convert RAW files to calibrated Sv |
| `process_Sv_CPS` | Prepare per-file CPS products |
| `process_transect_CPS` | Assemble transects and calculate NASC |
| `update_cache_CPS` | Prepare visualization products |

Follow their runs and logs. Each stage picks up available inputs on its next scheduled run; runs with no new work are normal.

## 6. Launch Echoshader

Reuse the deployment terminal and switch to `echodataflow-recipes`:

```powershell
cd ../echodataflow-recipes
python -m echoshader.app.launcher ./recipes_nrt/cps/app.yaml
```

Open [the CPS dashboard](http://127.0.0.1:1805/cps-north). Keep the terminal open; the app waits for data and refreshes as products arrive.

For real acquisition, disable `copy_raw`, point `raw2Sv.path_raw` at the incoming RAW directory, and use your live transect CSV with the watcher.
