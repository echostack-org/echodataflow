# Package structure

## Package structure

The importable package lives under `src/echodataflow/`. The package is structured with separate folders for the flows, tasks, and the underlying reusable processing logics. See [(prefect-terminology)] to know more about specific terminology used by Perfect, the orchestration library Echodataflow builds on.

```text
src/echodataflow/
├── deployment/    # Recipe parsing, flow registry, CLI, and deployment configuration
├── flows/         # Flow functions that are each high-level processing stages
├── operations/    # Reusable processing operations, settings, work items, and planning logic
├── services/      # System services configuration templates
├── tasks/         # Reusable Prefect task wrappers around operations
└── utils/         # Shared helpers for files, manifests, ledgers, grids, and time handling

tests/
├── deployment/    # Recipe validation, registry, CLI, deployment, and runner tests
├── flow/          # Workflow coordination and flow-level behavior
├── operations/    # Individual processing operations and planning behavior
├── tasks/         # Prefect task wrappers
└── utils/         # Shared helpers, manifests, and processing ledgers

docs/             # User and contributor documentation, plus example YAML recipes
test_notebooks/   # Notebooks for exploratory and manual checks
.github/          # Repository automation and contribution configuration
.agents/          # Implementation context for coding agents
```

The `tasks/` directory provides a **Prefect orchestration layer** around the functions in `operations/`. Operations implement reusable processing behavior; task wrappers expose that behavior to Prefect for logging, retries, and task-run tracking. This separation also lets operations be tested directly without running a complete workflow.

Task boundaries do not have to match operation boundaries. A task can call several operations when they belong in one execution unit, and a flow can combine several tasks into a larger workflow. For example, `flow_classify_backscatter` coordinates separate hake-prediction and nautical area scattering coefficient (NASC) tasks. Some flows also call operations directly when a separate Prefect task is unnecessary.

Because task execution is configured at the flow level, reusable tasks can run under different compatible task runners without rewriting their processing logic. Prefect defaults to `ThreadPoolTaskRunner`; Echodataflow also supports recipe-configured `DaskTaskRunner` for flows that use its environment-based runner configuration. Submitted tasks must still meet the selected runner's requirements, such as serialization for execution in separate processes. See [Recipe specification](recipe_spec.md) for task-runner settings.

Equivalent real-time and post-processing flows share the same module, while reusable planning and manifest logic stays outside flow functions. The flow reference below focuses on the real-time hake pipeline.

Tests mirror the package layers in a separate top-level directory. Files use the `test_*.py` naming convention, and fixtures or stubs stay local unless shared by multiple tests. Run the full suite with `python -m pytest`, or select a layer with, for example, `python -m pytest tests/operations`. See [Tests layout](https://github.com/echostack-org/echodataflow/blob/main/tests/README.md) for more details.

## Available flows

The following subset supports the hake edge-to-cloud pipeline, including simulation of incoming survey data. Modules are listed alphabetically. Post-processing flows and flows specific to other pipelines are omitted.

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

Deployment recipes select these flows and configure their parameters, schedules, and execution infrastructure. See [Recipe specification](recipe_spec.md) and [System services](system_services.md) for deployment and service setup.
