# Package structure

## Package structure

The importable package lives under `src/echodataflow/`. The package is structured with separate folders for the flows, tasks, and the underlying reusable processing logics. See [(prefect-terminology)] to know more about specific terminology used by Perfect, the orchestration library Echodataflow builds on.

```text
src/echodataflow/
├── deployment/    # Recipe parsing, flow registry, CLI, and deployment configuration
├── flows/         # Flow functions that are each high-level processing stages
├── operations/    # Reusable processing operations, settings, work items, and planning logic
├── services/      # System services and configuration templates
├── tasks/         # Task functions that wraps operations
└── utils/         # Helpers for files, ledgers, time handling, etc

tests/             # tests organized in a way that mirrors the package structure
├── deployment/
├── flow/
├── operations/
├── tasks/
└── utils/

docs/             # Documemtatopm
.github/          # GitHub workflows
```


Echodataflow uses Prefect to orchestrate the flows, and each flow can include multiple tasks to be executed underneath. The tasks can be executed sequentially or in parallel (see [](recipe-spec_task-runner)), and each task can include multiple operation steps. This design allows easily reusing existing computation either at the operation or the task level.





## Available flows

The table below lists currently available flows in Echodataflow package. Deployment recipes select from these flows and configure their schedules and execution infrastructure (via the [deployment recipe](recipe-spec_deployment)) and parameters (via the [parameters recipe](recipe-spec_parameters)). See [](overview) on how to deploy pipelines, and the [echodataflow-recipes repository]((https://github.com/echostack-org/echodataflow-recipes).) for example recipes.


| Module in `echodataflow.flows` | [Flow](https://docs.prefect.io/v3/concepts/flows/) | Purpose |
| --- | --- | --- |
| `flows_acoustics` | `flow_create_MVBS` | Combine Sv data into time slices and compute mean volume backscattering strength (MVBS) products, recording output metadata in a manifest. |
| `flows_acoustics` | `flow_raw2Sv` | Convert newly available local echosounder raw files to calibrated volume backscattering strength (Sv) Zarr stores and update the Sv manifest. |
| `flows_biology` | `flow_ingest_haul` | Ingest complete cloud trawl hauls and recompute biological estimates used in acoustic–biological integration. |
| `flows_classify_backscatter` | `flow_classify_backscatter` | Classify hake backscatter from MVBS time windows, export predictions and Echoview region (EVR) files, and compute NASC products. |
| `flows_helper` | `flow_file_upload` | Upload files with rclone, with options to exclude subdirectories and restrict file age. |
| `flows_integration` | `flow_ingest_NASC` | Ingest cloud NASC stores and integrate acoustic measurements with biological estimates. |
| `flows_integration` | `flow_update_grid` | Update survey grid-cell estimates from integrated NASC and haul biology. |
| `flows_simulation` | `flow_copy_raw` | Copy raw files according to their timestamps to simulate new real-time arrivals. |
| `flows_simulation` | `flow_copy_trawl` | Download files for the next trawl number from S3-compatible storage into local folders. |
| `flows_simulation` | `flow_simulate_transects` | Simulate the arrival of completed transect rows. |
| `flows_viz_cloud` | `flow_update_cache_contours` | Collect recent prediction EVR files into a local contour cache for visualization. |
| `flows_viz_cloud` | `flow_update_cache_MVBS` | Combine recent cloud MVBS stores into a local cache for echogram visualization. |



:::{note}
A subset of the above flows support the edge-to-cloud pipeline that was implemented for Integrated West Coast Pelagics Survey (IWCPS):
- edge workflows: `flow_raw2Sv`, `flow_create_MVBS`, `flows_classify_backscatter`, and `flow_file_upload`
- cloud workflows: `flow_ingest_haul`, `flow_ingest_NASC`, `flow_update_grid`, `flow_update_cache_MVBS`, and `flow_update_cache_contours`
:::