(smb-auto-mount)=
# SMB auto-mount service

This optional service keeps an SMB network share mounted, checking every five
minutes and attempting to mount it when absent. Both MacOS and Linux use the
shared `auto_mount.sh` Bash script, which selects the mount command and
configuration path for the OS. SMB settings are kept separately from Prefect
settings in `smb.env`.

## Deploy on a MacOS machine

On MacOS, system services are deployed through `launchd` via `.plist` files. The following setup runs the SMB auto-mount service under your user account using configurations from `~/.config/echodataflow/smb.env` via `auto_mount.sh`.

First, from the repository root, copy the MacOS environment template, shared
Bash script, and auto-mount service template:

```shell
mkdir -p ~/.config/echodataflow ~/Library/LaunchAgents ~/.local/var/log/echodataflow

# Skip this copy if smb.env is already configured
cp src/echodataflow/services/macos/smb.env.example ~/.config/echodataflow/smb.env
chmod 600 ~/.config/echodataflow/smb.env

cp src/echodataflow/services/shared/auto_mount.sh ~/.config/echodataflow/
cp src/echodataflow/services/macos/auto_mount.launchd.plist \
  ~/Library/LaunchAgents/org.echodataflow.auto-mount.plist
```

Edit `~/.config/echodataflow/smb.env` to set `SMB_USER`, `SMB_PASSWORD`,
`SMB_HOST`, `SMB_SHARE`, and `SMB_MOUNT_POINT`. Quote values containing shell
special characters. `SMB_LOG_DIR` controls mount-service logs
independently of Prefect.

Then, load and start the auto-mount service:

```shell
launchctl bootstrap gui/$(id -u) \
  ~/Library/LaunchAgents/org.echodataflow.auto-mount.plist
launchctl kickstart -k gui/$(id -u)/org.echodataflow.auto-mount
```

Afterwards, inspect the state and logs to make sure the service is running fine:

```shell
launchctl print gui/$(id -u)/org.echodataflow.auto-mount
tail -n 100 ~/.local/var/log/echodataflow/auto-mount.out.log
tail -n 100 ~/.local/var/log/echodataflow/auto-mount.err.log
```

To stop running the service, unload the mounting agent:

```shell
launchctl bootout gui/$(id -u) \
  ~/Library/LaunchAgents/org.echodataflow.auto-mount.plist
```

Stopping the service leaves an existing mount in place. To unmount it after all processes have finished using the share, run `umount` with your configured `SMB_MOUNT_POINT`.






## Deploy on a Linux machine

On Linux, system services are deployed through `systemd` via `.service` files. The following setup runs the SMB auto-mount service with root privileges using configurations from `/etc/echodataflow/smb.env` via `auto_mount.sh`. Note that unlike the Prefect infrastructure (server and worker) that are run as **user** services, this is a **system** service: the script sources root-owned configuration and uses `mount.cifs`. Install your distribution's `cifs-utils` package first.

First, from the repository root, copy the Linux environment template, shared Bash script, and auto-mount service template, and create a credentials file:

```shell
sudo install -d -m 700 /etc/echodataflow

# Skip these two commands if smb.env is already configured
sudo install -m 600 src/echodataflow/services/linux/smb.env.example /etc/echodataflow/smb.env
sudo install -m 600 /dev/null /etc/echodataflow/smb.credentials

sudo install -m 644 src/echodataflow/services/shared/auto_mount.sh /etc/echodataflow/auto_mount.sh
sudo install -m 644 src/echodataflow/services/linux/auto_mount.service /etc/systemd/system/auto_mount.service
```

Edit `/etc/echodataflow/smb.env` to set `SMB_HOST`, `SMB_SHARE`,
`SMB_MOUNT_POINT`, `SMB_CREDENTIALS_FILE`, and the numeric user/group IDs
(`SMB_UID` and `SMB_GID`) of the account that should access the mounted files:

```shell
sudoedit /etc/echodataflow/smb.env /etc/echodataflow/smb.credentials
```

Put the credentials in `smb.credentials` using this format:

```text
username=your_smb_user
password=your_smb_password
```

Keep both files owned by root and mode `600`. Passwords are read from the
credentials file rather than passed on the command line.

Then, load and start the auto-mount service:

```shell
sudo systemctl daemon-reload
sudo systemctl enable --now auto_mount.service
```

Afterwards, inspect the state and logs to make sure the service is running fine:

```shell
sudo systemctl status auto_mount.service
sudo journalctl -u auto_mount.service -n 100
```

Linux service output goes to the systemd journal. Failed mount attempts are
retried every 300 seconds; unhandled startup failures restart the service after
30 seconds. After five starts within five minutes, systemd stops retrying.
Correct the error and use `sudo systemctl reset-failed auto_mount.service`
before restarting.

To stop the service and disable automatic startup:

```shell
sudo systemctl disable --now auto_mount.service
```

Stopping the service leaves an existing mount in place. To unmount it after all
processes have finished using the share:

```shell
sudo umount /mnt/echodataflow  # Use your configured SMB_MOUNT_POINT
```
