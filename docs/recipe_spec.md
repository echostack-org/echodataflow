(recipe-spec)=
# Recipe specifications


## Recipes are in pairs
Pipelines in Echodataflow are defined by a pair of recipes: The **deployment recipe** controls _how_ the flows are run; the **parameters recipe** controls _what_ are run in the flows.

A flow needs to be defined in both recipes for correct execution. The deployment engine will flag flows that are not specified in both recipes.

For example, for a flow to upload acoustic data to the cloud (`file_upload_acoustics`), the specifications in the recipes are:

```yaml
# deploy_edge.yaml - deployment recipe
flows:
  file_upload_acoustics:  # same flow name in both recipes
    flow: file_upload
    deployment_name: file-upload-acoustics
    interval: 10
```

```yaml
# params_edge.yaml - parameters recipe
flows:
  file_upload_acoustics:  # same flow name in both recipes
    src_dir: /data/processed
    dest_dir: cloud:bucket/mission/acoustics
    exclude_subdirs: [Sv]
    max_age: 2
```




## Deployment command

To deploy a pipeline, use `echodataflow-deploy run` with the following:

```text
echodataflow-deploy run
  --default-work-pool-name POOL  # default work pool
  --deploy-spec DEPLOY.yaml  # deployment recipe
  --param-config PARAMS.yaml  # parameters recipe
```




## Deployment recipe

### Top-level fields

| Field | Required |  |
| --- | --- | --- |
| `flow_start_time` | no | Simulate flow rerun start timestamp specified following ISO 8601 (e.g., 2026-10-26T16:31:16-08:00). |
| `default_work_pool_name` | no | Default pool for flows in this recipe. |
| `concurrency_groups` | no | Mapping of group names to settings containing a required positive integer `limit`. Flows reference these groups with `concurrency_group` to share a concurrency-limited work queue. All members of a group must use the same work pool. |
| `source` | no | Source of flow code: local folder or remote repository. Defaults to local. Accepts `mode: local` or `mode: git`. Git mode requires `git.url` and optionally accepts `git.branch`, which defaults to `main`. |
| `flows` | yes | Mapping of recipe keys to deployment settings. |





### Flow specifications

Each key below `flows` identifies a deployment and must also appear in the parameters recipe. The deployment engine will generate an error if some recipe keys are missing in one of the recipe files.

In most cases, a flow deployment corresponds with one flow function in the flow registry, and the deployment engine will use the flow key to look up for the correct flow function. For example, `file_upload` resolves through the registry to the Python function `flow_file_upload` in `echodataflow/flows/flows_helper.py`:

```yaml
# deploy_edge.yml
flows:
  file_upload:  # recipe key
    interval: 10
```
The corresponding parameters recipe uses the same key:
```yaml
# params_edge.yml
flows:
  file_upload:  # recipe key
    src_dir: /data/acoustics
    dest_dir: cloud:bucket/mission/acoustics
    exclude_subdirs: []
```


In some cases, the same flow function is used in multiple deployments. In this case, the `flow` and `deployment_name` need to be separately specified for each recipe key. For example, when the Python function `flow_file_upload` is used by two deployments to upload different data types (acoustic and trawl in this case), the yaml is specified like below:
```yaml
# deploy_edge.yml
flows:
  file_upload_acoustics:  # recipe key
    flow: file_upload     # registry key
    deployment_name: upload-acoustics  # Prefect deployment name
    interval: 10
  file_upload_biology:
    flow: file_upload
    deployment_name: upload-biology
    interval: 30
```
The corresponding parameters recipe uses the same recipe key:
```yaml
# params_edge.yml
flows:
  file_upload_acoustics:
    src_dir: /data/acoustics
    dest_dir: cloud:bucket/mission/acoustics
    exclude_subdirs: []
  file_upload_biology:
    src_dir: /data/biology
    dest_dir: cloud:bucket/mission/biology
    exclude_subdirs: []
```

These create two deployments of `flow_file_upload`, each with its own schedule
and parameters. The recipe keys must match across the two files;
`deployment_name` only controls the name shown by Prefect.



### Timing and trigger control

A flow can be specified with either a repeating schedule (through `interval`) or an event trigger (through `triggers`). If neither is specified, a deployment is created that can be started manaully through the server dashboard.

To run a flow on a repeating schedule, set `interval` in minutes. The engine compiles it to a cron expression. `cron_offset` defaults to `0` and shifts the starting minute within each hour. For example:
```yaml
flows:
  file_upload_acoustics:
    flow: file_upload
    interval: 10
    cron_offset: 3
```
This runs at minutes 3, 13, 23, 33, 43, and 53 of each hour. An offset has no effect without `interval`.

To start a flow in response to an event, set `triggers` with the triggering event(s). For example:
```yaml
flows:
  file_upload_acoustics:
    flow: file_upload
    triggers:
      - expect: prefect.flow-run.Completed
        resource_name: acoustic-processing
        resource_scope: related  # default, and therefore can be omitted
```
This sets `file_upload_acoustics` to be triggered by the completion of an upstream flow `acoustic-processing`.

The triggers can also include the key `resource_scope`. When omitted, it defaults to `related`, meaning the deployment a flow run belongs to. In the example above, an upload is triggered whenever any run of the
`acoustic-processing` deployment completes, regardless of the individual run's name.

Setting `resource_scope` to `primary` instead requires matching the individual flow run's name to trigger an event. For example, say a run of the `acoustic-processing` deployment is named `happy-otter`, the trigger can be configured specifically for when that particular run completes:
```yaml
flows:
  file_upload_acoustics:
    flow: file_upload
    triggers:
      - expect: prefect.flow-run.Completed
        resource_name: happy-otter
        resource_scope: primary
```



### Computing resources and work pools

A work pool routes flow runs to the workers and infrastructure that execute them. Several deployments can share one pool. The top-level `default_work_pool_name` is the default work pool for the recipe, unless a different `work_pool_name` is specified for a given flow. For example:
```yaml
default_work_pool_name: edge
flows:
  file_upload_acoustics:
    flow: file_upload
  raw2Sv:
    work_pool_name: processing
```

Here, the flow `file_upload_acoustics` will use the `edge` work pool, while the flow `raw2Sv` will use the `processing` work pool. 

When `work_pool_name` is omitted, the flow uses the recipe's default work pool specified in the top-level key `default_work_pool_name`. When that top-level key is also omitted, the default work pool is the one supplied in the `echodataflow-deploy` CLI call `--default-work-pool-name`. The recipe takes precedence over the command-line value. 




### Concurrency configuration

Echodataflow supports two types of concurrency limits, one at the flow level and the other at the run level.

At the flow level, flows that share the same concurrency group (specified by the keyword `concurrency_group` under the recipe key) are bounded by the limit set in the top-level specifications.

At the run level, runs of the same flow are bounded by the limit set by the `deployment_concurrency` key under the recipe key.

Use concurrency limits when overlapping runs would compete for resources or write
to the same outputs. A shared concurrency group limits runs across deployments;
`deployment_concurrency` limits runs of one deployment. Both can be configured,
and a run must satisfy the applicable limits before it can execute.

To share a limit, define a group under top-level `concurrency_groups` and reference
it with each flow's `concurrency_group`:

```yaml
concurrency_groups:
  disk_processing:
    limit: 1
flows:
  raw2Sv:  # recipe key
    concurrency_group: disk_processing
  fetch_spasso:
    concurrency_group: disk_processing
```

The engine creates or updates a Prefect work queue named `disk_processing` with
the specified limit. Here, at most one run across these deployments can execute
at a time; other runs wait for queue capacity. `limit` must be a positive integer,
and every member must use the same work pool. A referenced group must be defined.
When `concurrency_group` is omitted, the engine does not assign a named group queue.
Shared groups have no `collision_strategy` option.

To limit overlapping runs of one deployment and choose how to handle conflicts:

```yaml
flows:
  raw2Sv:
    deployment_concurrency:
      limit: 1
      collision_strategy: ENQUEUE
      grace_period_seconds: 600
```

`deployment_concurrency` accepts:

- `limit`: required positive integer specifying the maximum concurrent flow runs
  for this deployment.
- `collision_strategy`: `ENQUEUE` (the default) leaves a conflicting run waiting
  until capacity is available. Use this when every run should eventually execute.
  `CANCEL_NEW` cancels the new conflicting run while existing runs continue; use
  this when skipping an overlapping run is acceptable.
- `grace_period_seconds`: startup grace period for the reserved concurrency slot,
  from `60` to `86400` seconds. Increase it when infrastructure starts slowly.
  When omitted, Prefect uses its server setting,
  `PREFECT_SERVER_CONCURRENCY_INITIAL_DEPLOYMENT_LEASE_DURATION`, whose default
  is `300` seconds. This is not a flow runtime limit: running flows renew their
  concurrency leases. See [Prefect's concurrency schema](https://github.com/PrefectHQ/prefect/blob/main/src/prefect/client/schemas/objects.py)
  and [server defaults](https://github.com/PrefectHQ/prefect/blob/main/src/prefect/settings/models/server/concurrency.py).

When `deployment_concurrency` is omitted, the engine supplies no per-deployment
limit. When both concurrency settings are omitted, this recipe adds no concurrency
limits; existing pool, queue, worker, or server configuration may still limit runs.

### Task runner

A task runner controls how submitted Prefect tasks execute within a flow run.
For flows using Echodataflow's runtime task-runner selection, omitting `task_runner`
uses Prefect's `ThreadPoolTaskRunner`. Select this default by leaving the field out;
`type: thread` and `type: threadpool` are not accepted recipe values. A flow must
submit tasks for the runner to provide concurrent task execution.

The only explicit recipe type currently supported is `dask`. To use a Dask task
runner with its default local-cluster settings:

```yaml
flows:
  raw2Sv:
    task_runner:
      type: dask
```

To configure that cluster, add `cluster_kwargs`:

```yaml
flows:
  raw2Sv:
    task_runner:
      type: dask
      cluster_kwargs:
        n_workers: 2
        threads_per_worker: 1
        processes: true
        memory_limit: 2GiB
```

The accepted cluster options are:

- `n_workers`: positive integer number of Dask workers in this flow run's cluster.
  These are separate from the Prefect workers polling a work pool.
- `threads_per_worker`: positive integer number of task-execution threads per
  Dask worker.
- `processes`: boolean choosing separate worker processes (`true`) or workers in
  the same process (`false`).
- `memory_limit`: memory budget per Dask worker, such as `2GiB` or `auto`.
  Dask enforces this on a best-effort basis with process workers; it is not a
  memory reservation for the whole deployment.

When `cluster_kwargs` or individual options are omitted, the engine leaves them
to Dask's defaults: worker and thread counts are derived from the machine,
`processes` defaults to `true` for the standard local cluster, and `memory_limit`
defaults to `auto`, dividing system memory between workers.
See the [Dask LocalCluster reference](https://docs.dask.org/en/stable/deploying-python.html#localcluster)
for their behavior. Remote scheduler addresses and other Dask cluster options are
not accepted by the current recipe schema.

The engine passes task-runner settings to the flow's runtime environment. They
take effect for flows wired to Echodataflow's `task_runner_from_environment()`
helper; other flows retain their own runner configuration. Each concurrent flow
run may create its own Dask cluster, so size the cluster together with the
concurrency limits to fit the execution machine.




### Simulate rerun
Each trigger requires non-empty strings for `expect` (the event name) and
`resource_name` (the resource name to match). `interval` and `triggers` are
mutually exclusive.

For historical simulation, top-level `flow_start_time` supplies an ISO 8601
starting timestamp. Set `inject_time_offset` to `true` on each compatible flow to
inject `time_offset_seconds`, calculated as the difference between the current
time and that timestamp. This maps the current wall clock to a historical mission
clock; it does not change the deployment schedule.

```yaml
flow_start_time: "2026-10-01T00:00:00+00:00"
flows:
  raw2Sv:
    interval: 10
    inject_time_offset: true
```

Injection is disabled when omitted. When enabled without `flow_start_time`, the
injected offset is `0.0`. The target flow must declare `time_offset_seconds`;
validation rejects incompatible flows.





## Parameters recipe

The parameters recipe has one required top-level mapping:

```yaml
flows:
  registry-or-recipe-key:
    parameter_name: value
```

Values below a flow key are passed directly to its Prefect flow. Parameter and deployment
recipe keys must have one-to-one coverage.
