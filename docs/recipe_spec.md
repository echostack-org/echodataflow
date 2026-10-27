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





(recipe-spec_flow)=
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

Echodataflow supports two types of concurrency limits: a shared limit across multiple deployments (`concurrency_group`) and a limit for an individual deployment (`deployment_concurrency`).

**Across multiple deployments (`concurrency_group`):** flows assigned to the same `concurrency_group` share the concurrency limit defined under top-level `concurrency_groups`. This limit controls how many runs across those flows can execute at the same time/ For example:
```yaml
concurrency_groups:
  disk_processing:
    limit: 1
flows:
  raw2Sv:  # recipe key
    concurrency_group: disk_processing
  create_MVBS:
    concurrency_group: disk_processing
```
Here, at most one run (`limit: 1`) across the `raw2Sv` flow and the `create_MVBS` flow can run at the same time in the Prefect work queue named `disk_processing`. When `concurrency_group` is omitted, the flows are not bounded by the a named group queue.


**Within an individual deployment (`deployment_concurrency`):** runs of the same flow are bounded by the limit set by the `deployment_concurrency` key under the recipe key. When `deployment_concurrency` is omitted, multiple runs of the same flow deployment can happen simultaneously. `deployment_concurrency` accepts:
- `limit`: required positive integer specifying the maximum concurrent flow runs for this deployment.
- `collision_strategy`:
  - `ENQUEUE` (the default) leaves a conflicting run waiting until capacity is available. Use this when every run should eventually execute.
  - `CANCEL_NEW` cancels the new conflicting run while existing runs continue; use this when skipping an overlapping run is acceptable.
- `grace_period_seconds`: startup grace period for the reserved concurrency slot, from `60` to `86400` seconds. Increase it when infrastructure starts slowly.  When omitted, Prefect uses its server setting,
  `PREFECT_SERVER_CONCURRENCY_INITIAL_DEPLOYMENT_LEASE_DURATION`, which defaults to `300` seconds.


For example:
```yaml
flows:
  raw2Sv:
    deployment_concurrency:
      limit: 1
      collision_strategy: CANCEL_NEW
      grace_period_seconds: 600
```
Here, at most one run of `raw2Sv` can be active at a time, and new runs are cancelled while that run holds the concurrency slot. `grace_period_seconds: 600` gives the accepted run up to 10 minutes for its infrastructure to start before its slot reservation expires. This does not affect how the new runs are handled. 





(recipe-spec_task-runner)=
### Task runner

A task runner controls how Prefect tasks execute within a flow run. Currently, `dask` is the only supported task runner (using Prefect's `DaskTaskRunner`). If no task runner is specified, Prefect's default `ThreadPoolTaskRunner` will be used.

The `DaskTaskRunner` cluster can be specified by the following options:
- `n_workers`: positive integer number of Dask workers in this flow run's cluster.  These are separate from the Prefect workers polling a work pool.
- `threads_per_worker`: positive integer number of task-execution threads per Dask worker.
- `processes`: 
  - When `true`, each worker runs in a separate operating-system process with its own Python interpreter and memory space, allowing Python computations to use multiple CPU cores at the cost of transferring data between processes.
  - When `false`, workers share one process and execute tasks in threads, reducing transfer overhead. Python's Global Interpreter Lock (GIL) can limit CPU parallelism with threads, though I/O tasks and libraries that release the GIL can still benefit.
- `memory_limit`: memory budget per Dask worker, such as `2GiB` or `auto`. Dask enforces this on a best-effort basis with process workers; it is not a memory reservation for the whole deployment.

When `cluster_kwargs` or individual options are omitted, Dask's defaults are used: `n_workers` and `threads_per_worker` are derived from the machine, `processes` defaults to `true` for the standard local cluster, and `memory_limit` defaults to `auto`, dividing system memory between workers. See the [Dask LocalCluster reference](https://docs.dask.org/en/stable/deploying-python.html#localcluster)
for these behaviors. 

For example:
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
Here, the `DaskTaskRunner` is configured with 2 workers, each using 1 thread and a memory limit of 2 GiB. With `processes: true`, each worker runs in a separate operating-system process, giving it its own Python interpreter and memory space.


:::{note}
Remote scheduler addresses and other Dask cluster options are not yet supported by the recipe schema. Each flow run configured with `DaskTaskRunner` creates its own local Dask cluster, so choose worker counts, memory limits, and flow concurrency limits together to avoid overloading the machine. Support for connecting to an existing cluster is an area of improvement in the near future.
:::




### Simulated rerun

For pipelines that handle real-time data, the flows can be deployed to simulate a "rerun" by effectively moving the current time to an earlier timepoint as if the flows were deployed from the starting timestamp (`flow_start_time`). This can be configured by setting `flow_start_time` at the top-level and adding `inject_time_offset: True` to the supporting flows. 

Current flows that support simulated returns are: `flow_raw2Sv`, `flow_create_MVBS`, and `flow_classify_backscatter`. These flows accepts an argument `time_offset_seconds` that contains the difference between the current time and `flow_start_time`, which is automatically populated at run time.

For example:
```yaml
flow_start_time: "2026-10-01T00:00:00+00:00"
flows:
  raw2Sv:
    interval: 10
    inject_time_offset: true
  create_MVBS:
    interval: 20
    inject_time_offset: true
```
Assuming this rerun pipeline was deployed at `2026-10-26T10:15:23+00:00` actual time. Both the `raw2Sv` and `create_MVBS` will behave as if they were deployed at `2026-10-01T00:00:00+00:00`. For example, after the pipeline has been running for a while and the actual time is `2026-10-26T12:32:46+00:00`, the flows would behave as if it was `2026-10-01T02:17:46+00:00`.





## Parameters recipe

The parameters recipe is straightforward to specify, because the entries under a recipe key are simply input arguments of the flow function. 

For example,
```yaml
flows:
  file_upload_acoustics:  # recipe key
    src_dir: /data/acoustics
    dest_dir: cloud:bucket/mission/acoustics
    exclude_subdirs:
      - Sv
      - raw
    max_age: 2
```
Here, the flow function `flow_file_upload` underlying the `file_upload_acoustics` flow (specified in the deployment recipe, see [](recipe-spec_flow)) accepts 4 input arguments: `src_dir`, `dest_dir`, `exclude_subdirs`, and `max_age`, which are all listed in the parameters recipe. `max_age` can be omitted as it has a default value if not specified in the function `flow_file_upload`.

Note that the recipe keys need to be identical with those in the deployment recipe. The deployment engine will give an error when there are mismatched recipes keys on either one of the recipes.