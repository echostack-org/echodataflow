(deployment-system-services)=
# Deployment system services

Echodataflow requires a Prefect server and workers in work pools to deploy a pipeline. While it is possible to deploy a pipeline by running a server and workers [simply via terminal](server-workpool-terminal), it is more robust to run them as long-running processes via system services in the operating system (OS) so that the processes will restart automatically if system crashes.

In the sections we explain the mechanism we use to run a server and a worker as system services on a machine running MacOS.
<!-- TODO add instructions for Linux and Windows-->

Note that Prefect also provides a mechanism to run a server and related resources [via Docker compose](https://docs.prefect.io/v3/how-to-guides/self-hosted/docker-compose).



## Template for environment settings

The system services templates provided in Echodataflow use `services.env` to control the many parameters used in the system services through environment variables. To set this up, copy `src/echodataflow/services/services.env.example_local_{OS_TYPE}` to `~/.config/echodataflow/services.env` by:
```shell
mkdir -p ~/.config/echodataflow
cp src/echodataflow/services/services.env.example_local_mac ~/.config/echodataflow/services.env
```

and edit the following parameters:

| Variable | Explanation |
| --- | --- |
| `ECHODATAFLOW_ENV` | Name of the Conda environment containing Echodataflow and Prefect. |
| `ECHODATAFLOW_HOME` | Absolute path to the home directory of the user running the services. |
| `ECHODATAFLOW_WORKDIR` | Absolute path to the repository or another stable working directory from which the services run. |
| `ECHODATAFLOW_LOG_DIR` | Writable directory where service logs are stored. |
| `MAMBA_BIN` | Absolute path to the Conda or Mamba executable used to run commands in `ECHODATAFLOW_ENV`. |
| `PREFECT_POOL` | Name of the existing Prefect work pool that the worker polls for flow runs. |
| `PREFECT_API_URL` | URL of the Prefect API used by the worker, such as `http://127.0.0.1:4200/api` for a local server. |
| `PREFECT_DB_PATH` | Path to the local Prefect SQLite database file checked by the macOS server template at startup. |
| `PREFECT_DB_AUTORESET_ON_FAILURE` | Optional startup setting for the macOS server template. Set to `true` to archive and reset the SQLite database if its health check fails, or `false` to require manual review. Defaults to `false` when unset. |
| `SMB_USER` | Username used by the SMB auto-mount service to authenticate to the network share. |
| `SMB_PASSWORD` | Password for `SMB_USER` on the SMB server. |
| `SMB_HOST` | Hostname or IP address of the SMB server. |
| `SMB_SHARE` | Name of the shared folder on the SMB server. |
| `SMB_MOUNT_POINT` | Local directory where the SMB auto-mount service mounts the network share. |




## Deploy on a MacOS machine

On MacOS, system services are deployed through `launchd` via `.plist` files. The following setup runs a local Prefect server and a worker as system services
under your user account.

First, copy the MacOS environment template and the server and worker service templates:

```shell
mkdir -p ~/.config/echodataflow ~/Library/LaunchAgents ~/.local/var/log/echodataflow
cp src/echodataflow/services/services.env.example_local_mac \
  ~/.config/echodataflow/services.env
cp src/echodataflow/services/deploy_prefect_server.launchd.plist \
  ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
cp src/echodataflow/services/deploy_prefect_worker.launchd.plist \
  ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
```

Edit `~/.config/echodataflow/services.env` using the variable explanations above, and set
`PREFECT_API_URL=http://127.0.0.1:4200/api` for the local server. If you already configured this file, keep it and skip the environment template copy.

Then, load and start the server and then the worker:
```shell
launchctl bootstrap gui/$(id -u) \
  ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
launchctl bootstrap gui/$(id -u) \
  ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
launchctl kickstart -k gui/$(id -u)/org.echodataflow.prefect-server
launchctl kickstart -k gui/$(id -u)/org.echodataflow.prefect-worker
```

Inspect the state and logs to make sure the services are running fine:

```shell
launchctl print gui/$(id -u)/org.echodataflow.prefect-server
launchctl print gui/$(id -u)/org.echodataflow.prefect-worker
tail -n 100 ~/.local/var/log/echodataflow/prefect-server.err.log
tail -n 100 ~/.local/var/log/echodataflow/prefect-worker.err.log
```

To stop running the services, unload the worker before the server:

```shell
launchctl bootout gui/$(id -u) \
  ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
launchctl bootout gui/$(id -u) \
  ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
```
