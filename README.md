<!-- <div>
  <a href="https://echodataflow.readthedocs.io/en/latest/?badge=latest">
    <img src="https://readthedocs.org/projects/echodataflow/badge/?version=latest"/>
  </a>
  <a href="https://codecov.io/gh/echostack-org/echodataflow" > 
 <img src="https://codecov.io/gh/echostack-org/echodataflow/graph/badge.svg?token=YTMVVHG585"/> 
 </a>
</div> -->

# Echodataflow: Streamlined Data Pipeline Orchestration

Echodataflow streamlines echosounder data processing by combining [Prefect](https://www.prefect.io/)-based pipeline orchestration, YAML configuration, and [Echopype](https://github.com/echostack-org/echopype) into a modular tool for defining, configuring, and executing workflows.


**Note:** Echodataflow v.0.1.x have been deprecated. We will release v0.2.0 soon!



## Installation

1. Set up a computing environment using Conda:
   ```bash
   conda create --name echodataflow -c conda-forge python=3.12
   conda activate echodataflow
   ```

2. If you would like to run Echodataflow as an installed package, 
   install it from the repo like below:
   ```bash
   pip install https://github.com/echostack-org/echodataflow.git  # install from repo
   ```
   This installs the `echodataflow-deploy` command, which can be run from any directory.
   If you instead would like to install Echodataflow to develop it,
   clone the repo and install it like below:
   ```bash
   git clone https://github.com/echostack-org/echodataflow.git  # clone the repo
   pip install -e ".[test,lint,docs]"  # install in editable mode with dev tools
   ```

3. Pip install the `segmentation_inference` package that contains a version of the hake segmentation model.
   ```bash
   cd ..
   git clone https://github.com/uw-echospace/segmentation_inference.git  # clone the repo
   cd segmentation_inference
   pip install -e .
   ```


## Running the edge pipeline

Note: Starting the server and running work pool is unnecessary if local Mac Prefect background services are running.

1. Start the local Prefect server:
   ```shell
   prefect server start
   ```

2. In a new terminal, create and run a work pool:
   ```shell
   prefect worker start --pool "local"
   ```
   If you run into the error below:
   ```shell
   ValueError: `PREFECT_API_URL` must be set to start a Worker.
   ```
   Run:
   ```shell
   prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
   ```

3. In a new terminal, download the recipes from the [echodataflow-recipes repository](https://github.com/echostack-org/echodataflow-recipes) by clonining it to your computer:
   ```
   cd REPO_DIRECTORY  # switch to where you want the recipes repo to sit
   git clone https://github.com/echostack-org/echodataflow-recipes.git
   ```

4. Deploy and run the edge pipeline:
   ```shell
   echodataflow-deploy run \
   --default-work-pool-name local \
   --param-config REPO_DIRECTORY/recipes/params/params_{MISSION_NAME}.yaml \
   --deploy-spec REPO_DIRECTORY/recipes/deploy/deploy_{MISSION_NAME}.yaml
   ```
   The deployment source is selected from the `source` section in the deploy recipe.
   If not set, it is default to using the local codebase.


## Running the cloud pipeline

1. Start a cloud virtual machine using the Linux platform

2. Start up a system service that runs a Prefect worker

3. Establish connection with the cloud Prefect server

4. Download the recipes from the [echodataflow-recipes repository](https://github.com/echostack-org/echodataflow-recipes) by clonining it to your computer:
   ```
   cd REPO_DIRECTORY  # switch to where you want the recipes repo to sit
   git clone https://github.com/echostack-org/echodataflow-recipes.git
   ```

5. Deploy and run the cloud pipeline:
   ```bash
   echodataflow-deploy run \
   --default-work-pool-name local \
   --param-config REPO_DIRECTORY/recipes/params/params_{MISSION_NAME}.yaml \
   --deploy-spec REPO_DIRECTORY/recipes/deploy/deploy_{MISSION_NAME}.yaml
   ```
   The deployment source is selected from the `source` section in the deploy recipe.
   If not set, it is default to using the local codebase.

6. Start up system services that hosts the 2 sets of visualization


## Configuring deployment concurrency

Deploy recipes support two independent concurrency controls:

- `concurrency_group` limits the total number of simultaneous runs shared by
  multiple deployments in the same work pool. Echodataflow implements each group
  as a Prefect work queue with a concurrency limit.
- `deployment_concurrency` limits simultaneous runs of one deployment, regardless
  of whether that deployment belongs to a concurrency group.

### Shared concurrency across deployments

Define groups at the top level of the deploy recipe, then assign flows to them by
name:

```yaml
concurrency_groups:
  acoustic_ingestion:
    limit: 3

flows:
  ingest_NASC:
    concurrency_group: acoustic_ingestion

  ingest_MVBS:
    concurrency_group: acoustic_ingestion
```

In this example, `ingest_NASC` and `ingest_MVBS` can use at most three running
slots in total. Runs beyond the shared limit wait in the group's work queue. A
concurrency group must remain within one work pool.

Flows without `concurrency_group` use the work pool's default queue.

### Per-deployment concurrency

Use `deployment_concurrency` within a flow to limit only that deployment:

```yaml
flows:
  ingest_NASC:
    deployment_concurrency:
      limit: 1
      collision_strategy: CANCEL_NEW
```

Supported fields are:

- `limit` (required): maximum number of concurrent runs for the deployment.
- `collision_strategy` (optional): `ENQUEUE` or `CANCEL_NEW`; defaults to
  `ENQUEUE`. `ENQUEUE` makes a new run wait for a slot, while `CANCEL_NEW`
  cancels it when the limit is full.
- `grace_period_seconds` (optional): time allowed for run infrastructure to start
  before its concurrency slot is released. The value must be between 60 and
  86,400 seconds.

### Combining both controls

The controls can be used together:

```yaml
concurrency_groups:
  acoustic_ingestion:
    limit: 3

flows:
  ingest_NASC:
    concurrency_group: acoustic_ingestion
    deployment_concurrency:
      limit: 1
      collision_strategy: CANCEL_NEW

  ingest_MVBS:
    concurrency_group: acoustic_ingestion
```

Here, the two deployments share three work-queue slots, while `ingest_NASC` may
occupy only one of those slots. A run must satisfy both limits before it can run.


## Running Local Prefect and auto mounting services on macOS (launchd)

To run a local Prefect server and worker as background services on macOS, you can
use launchd with the provided plist templates:

- `src/echodataflow/services/deploy_prefect_server.launchd.plist`
- `src/echodataflow/services/deploy_prefect_worker.launchd.plist`

These templates intentionally use direct one-line `ProgramArguments` commands, similar
to `.service` `ExecStart` usage, with no wrapper shell script required.

Included is a template and subsequent commands for auto mounting an SMB volume. These can be omitted if the volume is stable.

1. Copy and customize the templates for your user:
   ```shell
   mkdir -p ~/.config/echodataflow ~/Library/LaunchAgents ~/.local/var/log/echodataflow
   cp src/echodataflow/services/services.env.example_local ~/.config/echodataflow/services.env
   cp src/echodataflow/services/deploy_prefect_server.launchd.plist ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
   cp src/echodataflow/services/deploy_prefect_worker.launchd.plist ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
   cp src/echodataflow/services/auto_mount.launchd.plist ~/Library/LaunchAgents/org.echodataflow.auto-mount.plist
   ```

2. Edit `~/.config/echodataflow/services.env` as needed:
   - Adjust `ECHODATAFLOW_ENV`
   - Adjust `ECHODATAFLOW_HOME`
   - Adjust `MAMBA_BIN`
   - Adjust `PREFECT_POOL`
   - Adjust `PREFECT_API_URL`
   - Adjust SMB parameters as needed

3. Load and start services:
   ```shell
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.auto-mount.plist
   launchctl kickstart -k gui/$(id -u)/org.echodataflow.prefect-server
   launchctl kickstart -k gui/$(id -u)/org.echodataflow.prefect-worker
   launchctl kickstart -k gui/$(id -u)/org.echodataflow.auto-mount
   ```

4. Check status and logs:
   ```shell
   # make sure "state = running" and "runs" not increasing
   launchctl print gui/$(id -u)/org.echodataflow.prefect-server
   launchctl print gui/$(id -u)/org.echodataflow.prefect-worker
   launchctl print gui/$(id -u)/org.echodataflow.auto-mount
   # -f to follow logs in real time
   tail -n 100 ~/.local/var/log/echodataflow/prefect-server.err.log
   tail -n 100 ~/.local/var/log/echodataflow/prefect-worker.err.log
   tail -n 100 ~/.local/var/log/echodataflow/auto-mount.err.log
   ```

5. To stop and unload services:
   ```shell
   launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
   launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
   launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.auto-mount.plist
   ```

6. SQLite health checks (local Prefect server):
   ```shell
   sqlite3 ~/.prefect/prefect.db "PRAGMA quick_check;"
   sqlite3 ~/.prefect/prefect.db "PRAGMA integrity_check;"
   ```

7. If server startup keeps failing with SQLite lock errors, reset local DB safely:
   ```shell
   # stop services first
   launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
   launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist

   # archive existing local Prefect DB files (do not delete first)
   ts=$(date +%Y%m%d_%H%M%S)
   mkdir -p ~/.prefect/db-backups/$ts
   mv ~/.prefect/prefect.db* ~/.prefect/db-backups/$ts/ 2>/dev/null || true

   # start server, then worker
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
   launchctl kickstart -k gui/$(id -u)/org.echodataflow.prefect-server
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
   launchctl kickstart -k gui/$(id -u)/org.echodataflow.prefect-worker
   ```

Notes:
- `ThrottleInterval=30` in plist files helps avoid aggressive restart loops.
- `database is locked` usually means SQLite write contention, not corruption.
- For heavier multi-flow usage, move Prefect server DB to Postgres.

## Running Local Prefect Services on Windows (Task Scheduler)

To run a local Prefect server and worker as background services on Windows, you can
use Windows Task Scheduler with the provided templates:

- `src/echodataflow/services/deploy_prefect_server.windows.task.xml`
- `src/echodataflow/services/deploy_prefect_worker.windows.task.xml`

1. Configure Prefect for the local server:

   ```powershell
   prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
   ```

2. Render local Task Scheduler XML files from the templates:

   ```powershell
   $ConfigDir = "$HOME\.config\echodataflow"
   New-Item -ItemType Directory -Force $ConfigDir | Out-Null

   $User = whoami
   $PrefectExe = (Get-Command prefect).Source
   $WorkDir = (Get-Location).Path
   $Pool = "local"

   $Tasks = @{
       "deploy_prefect_server.windows.task.xml" = "prefect-server.task.xml"
       "deploy_prefect_worker.windows.task.xml" = "prefect-worker.task.xml"
   }

   foreach ($Task in $Tasks.GetEnumerator()) {
       $Template = "src\echodataflow\services\$($Task.Key)"
       $Output = Join-Path $ConfigDir $Task.Value

       $Content = Get-Content $Template -Raw
       $Content = $Content.Replace("__ECHODATAFLOW_USER__", $User)
       $Content = $Content.Replace("__PREFECT_EXE__", $PrefectExe)
       $Content = $Content.Replace("__ECHODATAFLOW_WORKDIR__", $WorkDir)
       $Content = $Content.Replace("__PREFECT_POOL__", $Pool)
       $Content = $Content.Replace('encoding="UTF-8"', 'encoding="UTF-16"')

       [System.IO.File]::WriteAllText(
           $Output,
           $Content,
           [System.Text.Encoding]::Unicode
       )
   }
   ```

   Run these commands from the Echodataflow repository root with the
   `echodataflow` environment activated. The templates are kept as UTF-8 in the
   repository and rendered as UTF-16 for Windows Task Scheduler.

3. Register the scheduled tasks from an **Administrator PowerShell**:

   ```powershell
   schtasks /Create /TN "echodataflow-prefect-server" `
     /XML "$HOME\.config\echodataflow\prefect-server.task.xml" /F

   schtasks /Create /TN "echodataflow-prefect-worker" `
     /XML "$HOME\.config\echodataflow\prefect-worker.task.xml" /F
   ```

4. Start the tasks:

   ```powershell
   schtasks /Run /TN "echodataflow-prefect-server"

   Start-Sleep -Seconds 10

   schtasks /Run /TN "echodataflow-prefect-worker"
   ```

5. Check task status:

   ```powershell
   schtasks /Query /TN "echodataflow-prefect-server" /V /FO LIST
   schtasks /Query /TN "echodataflow-prefect-worker" /V /FO LIST
   ```

6. Verify the local Prefect server:

   Open:

   ```text
   http://127.0.0.1:4200
   ```

   The Prefect dashboard should load, and the worker should appear online under
   **Work Pools**.

7. To stop and delete the tasks:

   ```powershell
   schtasks /End /TN "echodataflow-prefect-worker"
   schtasks /End /TN "echodataflow-prefect-server"

   schtasks /Delete /TN "echodataflow-prefect-worker" /F
   schtasks /Delete /TN "echodataflow-prefect-server" /F
   ```