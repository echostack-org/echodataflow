(infrastructure-system-services)=
# Infrastructure system services

Echodataflow requires a Prefect server and workers in work pools to deploy a pipeline. While it is possible to deploy a pipeline by running a server and workers [simply via terminal](overview_simple-setup), it is more robust to run them as long-running processes via system services in the operating system (OS) so that the processes will restart automatically if system crashes.

Below we explain the mechanism we use to run a server and a worker as system services on machine running MacOS or Linux.

Note that Prefect also provides a mechanism to run a server and related resources [via Docker compose](https://docs.prefect.io/v3/how-to-guides/self-hosted/docker-compose).



## Template for environment settings

The system services templates provided in Echodataflow use `services.env` to control the many parameters used in the system services. To set this up, copy `src/echodataflow/services/{macos,linux}/services.env.example` to `~/.config/echodataflow/services.env` by:
```shell
mkdir -p ~/.config/echodataflow
cp src/echodataflow/services/macos/services.env.example ~/.config/echodataflow/services.env
```

and edit the following parameters:

| Variable | Explanation |
| --- | --- |
| `ECHODATAFLOW_ENV` | Name of the Conda environment containing Echodataflow and Prefect. |
| `ECHODATAFLOW_HOME` | Absolute path to the home directory of the user running the services. |
| `ECHODATAFLOW_LOG_DIR` | Writable directory where service logs are stored. |
| `MAMBA_BIN` | Absolute path to the Conda or Mamba executable used to run commands in `ECHODATAFLOW_ENV`. |
| `PREFECT_POOL` | Name of the existing Prefect work pool that the worker polls for flow runs. |
| `PREFECT_API_URL` | URL of the Prefect API used by the worker, such as `http://127.0.0.1:4200/api` for a local server. |
| `PREFECT_DB_PATH` | Path to the local Prefect SQLite database file checked by the macOS and Linux server templates at startup. |
| `PREFECT_DB_AUTORESET_ON_FAILURE` | Optional startup setting for the macOS and Linux server templates. Set to `true` to archive and reset the SQLite database if its health check fails, or `false` to require manual review. Defaults to `false` when unset. |





## Deploy on a MacOS machine

On MacOS, system services are deployed through `launchd` via `.plist` files. The following setup runs a local Prefect server and a worker as system services under your user account using configurations from `services.env` via Bash scripts (`run_prefect_server.sh` and `run_prefect_worker.sh`). 

First, from the repository root, copy the MacOS environment template, shared Bash scripts, and server and worker service templates:
```shell
mkdir -p ~/.config/echodataflow ~/Library/LaunchAgents ~/.local/var/log/echodataflow

cp src/echodataflow/services/macos/services.env.example \
  ~/.config/echodataflow/services.env
chmod 600 ~/.config/echodataflow/services.env

cp src/echodataflow/services/shared/run_prefect_*.sh ~/.config/echodataflow/
cp src/echodataflow/services/macos/deploy_prefect_server.launchd.plist \
  ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
cp src/echodataflow/services/macos/deploy_prefect_worker.launchd.plist \
  ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
```

Edit `~/.config/echodataflow/services.env` following in explanations above.

Then, load and start the server and the worker:
```shell
launchctl bootstrap gui/$(id -u) \
  ~/Library/LaunchAgents/org.echodataflow.prefect-server.plist
launchctl bootstrap gui/$(id -u) \
  ~/Library/LaunchAgents/org.echodataflow.prefect-worker.plist
launchctl kickstart -k gui/$(id -u)/org.echodataflow.prefect-server
launchctl kickstart -k gui/$(id -u)/org.echodataflow.prefect-worker
```

Afterwards, inspect the state and logs to make sure the services are running fine:

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

:::{tip}
If these scripts are updated, copy the updated versions again using the commands above, and unload the previously running services using `bootout`, and load the updated services again using `bootstrap` and `kickstart`.
:::





## Deploy on a Linux machine

On Linux, system services are deployed through `systemd` via `.service` files. The following setup runs a local Prefect server and a worker as system services under your user account using configurations from `services.env` via Bash scripts (`run_prefect_server.sh` and `run_prefect_worker.sh`). 

First, from the repository root, copy the Linux environment template, shared Bash scripts, and server and worker service templates:
```shell
mkdir -p ~/.config/echodataflow ~/.config/systemd/user

cp src/echodataflow/services/linux/services.env.example ~/.config/echodataflow/services.env
chmod 600 ~/.config/echodataflow/services.env

cp src/echodataflow/services/shared/run_prefect_*.sh ~/.config/echodataflow/
cp src/echodataflow/services/linux/deploy_prefect_*.service ~/.config/systemd/user/
```

Edit `~/.config/echodataflow/services.env` following in explanations above.

Then, load and start the server and the worker:
```shell
systemctl --user daemon-reload
systemctl --user enable --now deploy_prefect_server.service deploy_prefect_worker.service
systemctl --user status deploy_prefect_server.service deploy_prefect_worker.service
journalctl --user -u deploy_prefect_server.service -u deploy_prefect_worker.service -n 100
```

Afterwards, inspect the error logs to make sure the services are running fine:
```shell
tail -n 100 ~/.local/var/log/echodataflow/prefect-server.err.log
tail -n 100 ~/.local/var/log/echodataflow/prefect-worker.err.log
```

The worker is ordered after the local server, but this does not guarantee API readiness; failed processes restart after 30 seconds. Logs from the running Prefect commands go to `ECHODATAFLOW_LOG_DIR`; early startup errors go to the journal. After five starts within five minutes, systemd stops retrying. Correct the error and use `systemctl --user reset-failed` before restarting.

To start services at boot and keep them running after logout, enable lingering:
```shell
sudo loginctl enable-linger "$USER"
```

To stop the services and disable automatic startup:
```shell
systemctl --user disable --now deploy_prefect_worker.service deploy_prefect_server.service
```
