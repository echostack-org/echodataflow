# How Echodataflow works

Echodataflow separates scientific processing from scheduling and infrastructure. This keeps
the computation testable while allowing operators to change paths, timing, and deployment
location through YAML recipes.

## The execution model

```{mermaid}
flowchart LR
    P[Parameter recipe] --> E[Deployment engine]
    D[Deployment recipe] --> E
    R[Flow registry] --> E
    E --> PF[Prefect deployment]
    PF --> F[Flow]
    F --> T[Task]
    T --> O[Operation]
    O --> FS[(Local or cloud data)]
```

The layers have distinct responsibilities:

- **Operations** are ordinary Python processing functions. They receive typed work items and
  settings and return typed results. They should not decide when a workflow runs.
- **Tasks** add Prefect behavior around operations, such as retries and task-run naming.
- **Flows** coordinate tasks, paths, processing windows, incremental state, and upstream or
  downstream steps.
- **The flow registry** maps stable recipe names such as `raw2Sv` to Python entrypoints.
- **Parameter recipes** provide the arguments for each deployed flow.
- **Deployment recipes** select source code, names, schedules, triggers, and work pools.

## Recipes are a pair

A parameter recipe and deployment recipe are validated together. If one declares
`file_upload_acoustics`, the other must declare the same key:

```yaml
# params_edge.yaml
flows:
  file_upload_acoustics:
    src_dir: /data/processed
    dest_dir: cloud:bucket/mission/acoustics
    exclude_subdirs: [Sv]
    max_age: 2
```

```yaml
# deploy_edge.yaml
default_work_pool_name: local
flows:
  file_upload_acoustics:
    flow: file_upload
    deployment_name: file-upload-acoustics
    interval: 10
```

The recipe key identifies this particular deployment. The optional `flow` field points it to
a registered implementation. This is how multiple deployments can reuse `file_upload` with
different source directories or destinations.

## Schedules and triggers

An `interval` is expressed in minutes and compiled to a cron expression. `cron_offset`
delays the first minute within the hour, which is useful when one flow should normally follow
another. For example, an interval of 10 with an offset of 3 runs at minutes 3, 13, 23, and so
on.

A flow may instead define `triggers`. An event-triggered flow starts when Prefect observes
the configured event from a related deployment. A single flow entry cannot use both an
interval and triggers.

Schedule offsets do not establish a data dependency. Flows should remain safe when an
upstream run is slow, empty, or unsuccessful.

## Historical simulation

Some flows calculate a processing window relative to the current time. A deployment recipe
can set `flow_start_time` and enable `inject_time_offset` for flows that accept
`time_offset_seconds`. This maps the current wall clock to a historical mission clock and is
used by the simulation examples.

Only enable injection for flows that declare `time_offset_seconds`; deployment validation
rejects incompatible flows.

## Edge and cloud roles

An edge deployment commonly:

1. observes or simulates files arriving on a shipboard filesystem;
2. converts raw sonar files to Sv;
3. creates time-binned MVBS products;
4. optionally runs a model;
5. uploads selected products and biological files when connectivity permits.

A cloud deployment can ingest uploaded products, integrate acoustic and biological data,
update survey grids, and prepare visualization caches. These are conventions rather than
hard-coded environments: work pools and recipe parameters determine where a flow runs.

## Data and state

Several workflows use CSV indexes in `path_main` to track generated products. Simulation
flows also store the previous simulated time or trawl number in Prefect Variables. Therefore:

- preserve the working directory between runs;
- back up important Prefect state and product indexes;
- do not run multiple copies of a deployment unless the flow supports it; and
- use a distinct Prefect deployment when independent state is required.

Credentials and secrets should not be committed to recipe files. Configure rclone remotes,
Prefect profiles or blocks, and credential files outside the repository, then refer to them
by name or path.

