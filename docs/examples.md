# Examples

This page first explains reusable deployment patterns, then provides a small simulated edge
workflow that does not require write access to somebody else's cloud account.

## Reading a parameter recipe

A parameter recipe contains exactly the keyword arguments passed to each flow. The example
in [`examples/params-edge-example.yaml`](examples/params-edge-example.yaml) demonstrates:

- `copy_raw`: replaying a public NOAA S3 manifest into a local arrival directory;
- `raw2Sv`: incrementally converting matching EK80 raw files;
- `create_MVBS`: producing recent, fixed-duration MVBS windows; and
- two instances of `file_upload`, one for acoustics and one for trawl data.

Paths in operational recipes should be absolute. YAML does not expand shell variables such
as `$HOME`, and Echodataflow does not currently template recipe values.

## Reading a deployment recipe

[`examples/deploy-edge-example.yaml`](examples/deploy-edge-example.yaml) demonstrates several
deployment scenarios without test-only comments:

### A producer and consumer

`copy_raw` and `raw2Sv` run on the same interval. The first simulates file arrival and the
second processes files not yet listed in `Sv_files.csv`. This is eventual coordination, not
a strict dependency. If ordering must be guaranteed, use an event trigger or compose both
steps in one parent flow.

### Staggering work

`create_MVBS` uses `cron_offset: 3`, so it normally begins after the raw conversion schedule.
Staggering reduces resource contention but does not guarantee that raw conversion completed.

### Replaying historical time

`flow_start_time` establishes the historical clock. `inject_time_offset: true` passes the
calculated offset to simulation and windowed-processing flows.

### Reusing one flow

`file_upload_acoustics` and `file_upload_trawl` both declare `flow: file_upload`. Their
distinct recipe keys give them independent deployments, parameters, schedules, and Prefect
state.

### Selecting deployment source and workers

`source.mode: local` loads code from the installed checkout. The top-level
`default_work_pool_name` applies unless an entry sets `work_pool_name` explicitly.

### Event-driven alternative

An interval can be replaced by a Prefect event trigger:

```yaml
flows:
  downstream:
    flow: create_MVBS
    deployment_name: create-MVBS-after-raw2Sv
    triggers:
      - expect: prefect.flow-run.Completed
        resource_name: raw2Sv
```

The `resource_name` must match the related Prefect deployment resource name. Test event
matching in the target Prefect workspace before relying on it operationally.

## Minimal simulated edge flow

There is no safe public bucket that can accept anonymous uploads from every documentation
reader. The reproducible example therefore uses public NOAA S3 for input and a second local
directory as a **mock cloud destination**. It still exercises the real `file_upload` flow and
the real `rclone copy` command. Once it works, replace the destination with an rclone remote
you control.

### 1. Prepare software

Follow [Installation](installation.md), including Echopype and rclone. Start a local Prefect
server and worker as described in [Deployment](deployment.md).

Clone this repository so the example assets are available:

```shell
git clone https://github.com/echostack-org/echodataflow.git
cd echodataflow
uv pip install -e .
```

### 2. Create isolated directories

```shell
mkdir -p /tmp/echodataflow-demo/raw
mkdir -p /tmp/echodataflow-demo/processed
mkdir -p /tmp/echodataflow-demo/trawl
mkdir -p /tmp/echodataflow-demo/mock-cloud
```

Copy a small trawl fixture and the raw-file manifest:

```shell
cp -R test_data/trawl/. /tmp/echodataflow-demo/trawl/
cp docs/examples/simulated-raw-files.csv /tmp/echodataflow-demo/raw-files.csv
```

The two public EK80 files in the manifest require roughly 200 MB of download space, and
conversion needs additional working space.

### 3. Copy and inspect the recipes

```shell
cp docs/examples/params-edge-example.yaml /tmp/echodataflow-demo/params.yaml
cp docs/examples/deploy-edge-example.yaml /tmp/echodataflow-demo/deploy.yaml
```

The checked-in parameters already use `/tmp/echodataflow-demo`. Review them before
deployment. The example omits `predict_hake` because its model weights are not publicly
distributed.

### 4. Deploy

```shell
echodataflow-deploy run \
  --default-work-pool-name local \
  --param-config /tmp/echodataflow-demo/params.yaml \
  --deploy-spec /tmp/echodataflow-demo/deploy.yaml
```

Open the Prefect UI and run `copy-raw` manually first. Then run `raw2Sv`, `create-MVBS`, and
the upload deployments, or allow their schedules to run. Expected locations are:

```text
/tmp/echodataflow-demo/raw/                 downloaded raw files
/tmp/echodataflow-demo/processed/Sv/        Sv Zarr stores
/tmp/echodataflow-demo/processed/MVBS/      MVBS Zarr stores
/tmp/echodataflow-demo/mock-cloud/acoustics copied acoustic products
/tmp/echodataflow-demo/mock-cloud/trawl     copied trawl files
```

The historical clock and Prefect Variable state mean repeated runs may correctly report that
there is nothing new to copy. Use a new deployment name for an independent replay, or remove
only the example's Prefect deployments and associated variables through Prefect after
inspecting them.

### 5. Switch to your cloud storage

Configure an rclone remote using its backend documentation. Test it outside Prefect:

```shell
rclone lsd my-remote:my-bucket
rclone copy /tmp/echodataflow-demo/trawl my-remote:my-bucket/echodataflow-demo/trawl
```

Then change only the upload destinations:

```yaml
file_upload_acoustics:
  dest_dir: my-remote:my-bucket/echodataflow-demo/acoustics

file_upload_trawl:
  dest_dir: my-remote:my-bucket/echodataflow-demo/trawl
```

Keep credentials in rclone's configuration or another secret-management mechanism, never in
the recipe. Redeploy after modifying parameters.

