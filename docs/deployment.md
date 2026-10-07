# Deployment

Echodataflow deployments run on Prefect. The Prefect API stores schedules and run state;
workers poll work pools and execute scheduled flow runs.

## Choose a Prefect topology

For development and a single edge computer, run a local Prefect server and a process worker
on the same machine. For a distributed or production system, use Prefect Cloud or a
self-hosted server backed by PostgreSQL and point workers at that API.

The examples below use a work pool named `local`:

```shell
prefect work-pool create --type process local
```

Run this once against the selected Prefect API. Use `prefect work-pool ls` to confirm that
the pool exists.

## Interactive local setup

Start a local server:

```shell
prefect server start
```

In a second terminal, select it and start a worker:

```shell
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect worker start --pool local
```

The Prefect dashboard is normally available at <http://127.0.0.1:4200>. Keep both processes
running while testing deployments.

## macOS services with launchd

The repository provides templates for the local server and worker:

- `src/echodataflow/services/deploy_prefect_server.launchd.plist`
- `src/echodataflow/services/deploy_prefect_worker.launchd.plist`
- `src/echodataflow/services/services.env.example_local`

Copy them into user-owned locations:

```shell
mkdir -p ~/.config/echodataflow ~/Library/LaunchAgents ~/.local/var/log/echodataflow
cp src/echodataflow/services/services.env.example_local \
  ~/.config/echodataflow/services.env
cp src/echodataflow/services/deploy_prefect_server.launchd.plist \
  ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
cp src/echodataflow/services/deploy_prefect_worker.launchd.plist \
  ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
```

Edit `~/.config/echodataflow/services.env`. At minimum, verify:

- `ECHODATAFLOW_ENV`: Conda environment containing Echodataflow and Prefect;
- `ECHODATAFLOW_HOME`: absolute home directory;
- `ECHODATAFLOW_WORKDIR`: repository or stable working directory;
- `ECHODATAFLOW_LOG_DIR`: writable log directory;
- `MAMBA_BIN`: absolute Conda or Mamba executable path;
- `PREFECT_POOL`: existing work-pool name; and
- `PREFECT_API_URL`: the local server API URL.

Load and start the server before the worker:

```shell
launchctl bootstrap gui/$(id -u) \
  ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
launchctl bootstrap gui/$(id -u) \
  ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
launchctl kickstart -k gui/$(id -u)/org.echodataflow.prefect-server
launchctl kickstart -k gui/$(id -u)/org.echodataflow.prefect-worker
```

Inspect state and logs:

```shell
launchctl print gui/$(id -u)/org.echodataflow.prefect-server
launchctl print gui/$(id -u)/org.echodataflow.prefect-worker
tail -n 100 ~/.local/var/log/echodataflow/prefect-server.err.log
tail -n 100 ~/.local/var/log/echodataflow/prefect-worker.err.log
```

Unload the worker before the server:

```shell
launchctl bootout gui/$(id -u) \
  ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
launchctl bootout gui/$(id -u) \
  ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
```

The server template performs a SQLite health check. Keep
`PREFECT_DB_AUTORESET_ON_FAILURE=false` when you want a failed check to require manual
review. If automatic reset is enabled, the template archives the database files before
creating a new database.

## Linux services with systemd

The repository includes `src/echodataflow/services/deploy_prefect_worker.service`. It is a
template: copy it before editing so repository updates do not replace local configuration.

```shell
sudo cp src/echodataflow/services/deploy_prefect_worker.service \
  /etc/systemd/system/echodataflow-prefect-worker.service
sudo mkdir -p /home/USER/.config/echodataflow
cp src/echodataflow/services/services.env.example_cloud \
  /home/USER/.config/echodataflow/services.env
```

Replace the template's `/home/exouser` paths and edit `services.env`. For Prefect Cloud,
authenticate the service environment and set the workspace API URL, or ensure the selected
Prefect profile is available to the service account.

```shell
sudo systemctl daemon-reload
sudo systemctl enable --now echodataflow-prefect-worker.service
sudo systemctl status echodataflow-prefect-worker.service
journalctl -u echodataflow-prefect-worker.service -n 100
```

To self-host the Prefect server on Linux, create a separate service rather than adding a
second command to the worker unit:

```ini
[Unit]
Description=Prefect server for Echodataflow
After=network.target

[Service]
Type=simple
User=USER
WorkingDirectory=/home/USER
EnvironmentFile=/home/USER/.config/echodataflow/services.env
ExecStart=/usr/bin/env bash -lc 'exec "${MAMBA_BIN}" run -n "${ECHODATAFLOW_ENV}" prefect server start --host 0.0.0.0 --port 4200'
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

Save it as `/etc/systemd/system/echodataflow-prefect-server.service`, replace `USER`, then
enable it as above. Binding to `0.0.0.0` exposes the service to the network: place it behind
appropriate firewall, TLS, and authentication controls. SQLite is suitable for modest local
use; use PostgreSQL for a busier shared deployment.

## Deploy workflows

Deploy a paired parameter and deployment recipe with:

```shell
echodataflow-deploy run \
  --default-work-pool-name local \
  --param-config /path/to/recipes/params/params_mission.yaml \
  --deploy-spec /path/to/recipes/deploy/deploy_mission.yaml
```

The command validates both files, resolves flow names through the registry, creates the
Prefect deployments, and applies them to the configured API. It does not start a worker.

The deployment recipe's `source.mode` controls where workers load code:

```yaml
source:
  mode: local
```

Local mode uses the installed Echodataflow source tree and is convenient for development or
a fixed edge installation. Git mode makes deployments pull a branch:

```yaml
source:
  mode: git
  git:
    url: https://github.com/echostack-org/echodataflow.git
    branch: main
```

Pin a release tag or commit-oriented branch for operations where reproducibility matters.

## Verify and operate deployments

After deployment:

```shell
prefect deployment ls
prefect worker ls
```

Use the Prefect UI to confirm schedules, parameters, work pools, and recent run state. Run a
deployment manually before relying on its schedule. Useful operational checks include:

- the worker is online in the expected pool;
- the worker environment contains Echodataflow, processing packages, and `rclone`;
- all recipe paths are absolute and accessible to the service account;
- cloud credentials work non-interactively; and
- output and Prefect log locations have adequate disk space.

## Troubleshooting

If a worker reports that `PREFECT_API_URL` is missing, set it in the active Prefect profile
or service environment. If deployments remain scheduled but never run, verify the pool name
and worker health. Import errors usually mean the worker environment differs from the
environment used to create the deployment.

For local SQLite errors, stop the worker and server before inspecting the database:

```shell
sqlite3 ~/.prefect/prefect.db "PRAGMA quick_check;"
sqlite3 ~/.prefect/prefect.db "PRAGMA integrity_check;"
```

Archive the database rather than deleting it if a reset is required. A locked database is
not necessarily corrupt; it may indicate multiple server processes or excessive concurrent
writes.
