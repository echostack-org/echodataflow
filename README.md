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
   pip install git+https://github.com/echostack-org/echodataflow.git  # install from repo
   ```
   If you instead would like to install Echodataflow to develop it,
   clone the repo and install it like below:
   ```bash
   git clone git+https://github.com/echostack-org/echodataflow.git  # clone the repo
   pip install -e ".[test,lint,docs]"  # install in editable mode with dev tools
   ```
   


## Running the edge pipeline

1. Start the local Prefect server:
   ```shell
   prefect server start
   ```

2. In a new terminal, create and run a work pool:
   ```shell
   prefect worker start --pool "local"
   ```

3. Download the recipes from the [echodataflow-recipes repository](https://github.com/echostack-org/echodataflow-recipes) by clonining it to your computer:
   ```
   cd REPO_DIRECTORY  # switch to where you want the recipes repo to sit
   git clone https://github.com/echostack-org/echodataflow-recipes.git
   ```

4. Deploy and run the ship pipeline:
   ```shell
   python -m echodataflow.deployment.deploy_cli run \
   --source-mode local \
   --default-work-pool-name local \
   --param-config REPO_DIRECTORY/recipes/params/params_{MISSION_NAME}.yaml \
   --deploy-spec REPO_DIRECTORY/recipes/deploy/deploy_{MISSION_NAME}.yaml
   ```


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
   python -m echodataflow.deployment.deploy_cli run \
   --source-mode local \
   --default-work-pool-name local \
   --param-config REPO_DIRECTORY/recipes/params/params_{MISSION_NAME}.yaml \
   --deploy-spec REPO_DIRECTORY/recipes/deploy/deploy_{MISSION_NAME}.yaml
   ```

6. Start up system services that hosts the 2 sets of visualization


## Running Local Prefect Services on macOS (launchd)

To run a local Prefect server and worker as background services on macOS, you can
use launchd with the provided plist templates:

- `src/echodataflow/services/deploy_prefect_server.launchd.plist`
- `src/echodataflow/services/deploy_prefect_worker.launchd.plist`

These templates intentionally use direct one-line `ProgramArguments` commands, similar
to `.service` `ExecStart` usage, with no wrapper shell script required.

1. Copy and customize the templates for your user:
   ```shell
   mkdir -p ~/.config/echodataflow ~/Library/LaunchAgents ~/.local/var/log/echodataflow
   cp src/echodataflow/services/services.env.example ~/.config/echodataflow/services.env
   cp src/echodataflow/services/deploy_prefect_server.launchd.plist ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
   cp src/echodataflow/services/deploy_prefect_worker.launchd.plist ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
   ```

2. Edit `~/.config/echodataflow/services.env` as needed:
   - Adjust `ECHODATAFLOW_ENV`
   - Adjust `ECHODATAFLOW_HOME`
   - Adjust `MAMBA_BIN`
   - Adjust `PREFECT_POOL`
   - Adjust `PREFECT_API_URL`

3. Load and start services:
   ```shell
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
   launchctl kickstart -k gui/$(id -u)/org.echodataflow.prefect-server
   launchctl kickstart -k gui/$(id -u)/org.echodataflow.prefect-worker
   ```

4. Check status and logs:
   ```shell
   # make sure "state = running" and "runs" not increasing
   launchctl print gui/$(id -u)/org.echodataflow.prefect-server
   launchctl print gui/$(id -u)/org.echodataflow.prefect-worker
   # -f to follow logs in real time
   tail -n 100 ~/.local/var/log/echodataflow/prefect-server.err.log
   tail -n 100 ~/.local/var/log/echodataflow/prefect-worker.err.log
   ```

5. To stop and unload services:
   ```shell
   launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
   launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
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
use PowerShell with Windows Task Scheduler and the provided templates:

- `src/echodataflow/services/deploy_prefect_server.windows.task.xml`
- `src/echodataflow/services/deploy_prefect_worker.windows.task.xml`

1. Copy and customize the service environment file:
   ```powershell
   New-Item -ItemType Directory -Force "$HOME\.config\echodataflow"
   New-Item -ItemType Directory -Force "$HOME\.local\var\log\echodataflow"

   Copy-Item src\echodataflow\services\services.env.example_local `
     "$HOME\.config\echodataflow\services.env"
   ```

2. Edit `$HOME\.config\echodataflow\services.env` as needed:
   - Adjust `ECHODATAFLOW_ENV`
   - Adjust `ECHODATAFLOW_HOME`
   - Adjust `ECHODATAFLOW_WORKDIR`
   - Adjust `ECHODATAFLOW_LOG_DIR`
   - Adjust `MAMBA_BIN`
   - Adjust `PREFECT_POOL`
   - Adjust `PREFECT_API_URL`

3. Copy and customize the Task Scheduler XML templates:
   ```powershell
   Copy-Item src\echodataflow\services\deploy_prefect_server.windows.task.xml `
     "$HOME\.config\echodataflow\prefect-server.task.xml"

   Copy-Item src\echodataflow\services\deploy_prefect_worker.windows.task.xml `
     "$HOME\.config\echodataflow\prefect-worker.task.xml"
   ```

4. Both XML templates are self-contained and read their runtime configuration
   from `$HOME\.config\echodataflow\services.env`, so they do not need separate
   `deploy_prefect_*.windows.ps1` files or hard-coded repository script paths.

5. Register the scheduled tasks:
   ```powershell
   schtasks /Create /TN "echodataflow-prefect-server" `
     /XML "$HOME\.config\echodataflow\prefect-server.task.xml" /F

   schtasks /Create /TN "echodataflow-prefect-worker" `
     /XML "$HOME\.config\echodataflow\prefect-worker.task.xml" /F
   ```

6. Start the tasks:
   ```powershell
   schtasks /Run /TN "echodataflow-prefect-server"
   Start-Sleep -Seconds 10
   schtasks /Run /TN "echodataflow-prefect-worker"
   ```

7. Check task status:
   ```powershell
   schtasks /Query /TN "echodataflow-prefect-server" /V /FO LIST
   schtasks /Query /TN "echodataflow-prefect-worker" /V /FO LIST
   ```

8. Verify the local Prefect server:

   Open:

   ```text
   http://127.0.0.1:4200
   ```

   The Prefect dashboard should load, and the worker should appear online under
   **Work Pools**.

9. To stop and delete the tasks:
   ```powershell
   schtasks /End /TN "echodataflow-prefect-worker"
   schtasks /End /TN "echodataflow-prefect-server"

   schtasks /Delete /TN "echodataflow-prefect-worker" /F
   schtasks /Delete /TN "echodataflow-prefect-server" /F
   ```

## License

Echodataflow is licensed under the open source [Apache 2.0 license](https://opensource.org/license/Apache-2.0).
