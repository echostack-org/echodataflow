#!/bin/bash
# Exit on unhandled command failures (-e) or unset variables (-u)
set -eu

# uname reports macOS as Darwin. The two OSes require different SMB tools
# macOS: launchd runs this as the logged-in user, using their configuration
# Linux: systemd runs this as root; sourced shell code must be root-owned,
# so configuration lives in /etc rather than a user-writable directory
PLATFORM="$(uname -s)"
case "$PLATFORM" in
  Darwin) ENV_FILE="$HOME/.config/echodataflow/smb.env" ;;
  Linux) ENV_FILE=/etc/echodataflow/smb.env ;;
  *) echo "Unsupported platform: $PLATFORM" >&2; exit 1 ;;
esac

# Load the OS-specific settings into this shell, expanding variable references
[[ -f "$ENV_FILE" ]] || { echo "Missing $ENV_FILE" >&2; exit 1; }
source "$ENV_FILE"

# Require the connection settings shared by both platforms
# ${VAR:?message} exits with the message when VAR is unset or empty
: "${SMB_HOST:?SMB_HOST is not set in smb.env}"
: "${SMB_SHARE:?SMB_SHARE is not set in smb.env}"
: "${SMB_MOUNT_POINT:?SMB_MOUNT_POINT is not set in smb.env}"

# Validate the credentials and logging settings specific to each OS
if [[ "$PLATFORM" == Darwin ]]; then
  # mount_smbfs takes the SMB username and password in the share URL
  : "${SMB_USER:?SMB_USER is not set in smb.env}"
  : "${SMB_PASSWORD:?SMB_PASSWORD is not set in smb.env}"

  # Append macOS service output to files; Linux output goes to the journal
  LOG_DIR="${ECHODATAFLOW_LOG_DIR:-$HOME/.local/var/log/echodataflow}"
  mkdir -p "$LOG_DIR"
  exec >>"$LOG_DIR/auto-mount.out.log" 2>>"$LOG_DIR/auto-mount.err.log"
else
  # mount.cifs reads a separate root-owned credentials file. UID/GID select
  # the local account that should own files presented by the mounted share
  : "${SMB_CREDENTIALS_FILE:?SMB_CREDENTIALS_FILE is not set in smb.env}"
  : "${SMB_UID:?SMB_UID is not set in smb.env}"
  : "${SMB_GID:?SMB_GID is not set in smb.env}"
  [[ -f "$SMB_CREDENTIALS_FILE" ]] || { echo "Missing SMB credentials file" >&2; exit 1; }
fi

# Check the exact mount location before attempting to mount it again
# macOS uses mount output; Linux provides the mountpoint utility
is_mounted() {
  if [[ "$PLATFORM" == Darwin ]]; then
    mount | grep -Fq -- " on $SMB_MOUNT_POINT ("
  else
    mountpoint -q -- "$SMB_MOUNT_POINT"
  fi
}

# Mount using the platform's SMB implementation: mount_smbfs on macOS,
# or mount -t cifs (provided by cifs-utils) with root privileges on Linux
# Do not enable shell tracing here: the macOS command includes the password
mount_share() {
  if [[ "$PLATFORM" == Darwin ]]; then
    mount_smbfs "//${SMB_USER}:${SMB_PASSWORD}@${SMB_HOST}/${SMB_SHARE}" "$SMB_MOUNT_POINT"
  else
    mount -t cifs "//$SMB_HOST/$SMB_SHARE" "$SMB_MOUNT_POINT" \
      -o "credentials=$SMB_CREDENTIALS_FILE,uid=$SMB_UID,gid=$SMB_GID"
  fi
}

# Create the local mount directory if it does not already exist
mkdir -p "$SMB_MOUNT_POINT"
echo "$(date): auto mount service started"
# Check every five minutes and mount if absent. Mount failures are handled
# explicitly so set -e does not end the loop; the next check retries them
# Stopping the service leaves an existing mount in place
while true; do
  if ! is_mounted; then
    echo "$(date): mounting $SMB_MOUNT_POINT"
    if mount_share; then
      echo "$(date): mount successful"
    else
      echo "$(date): mount failed; retrying in 300 seconds" >&2
    fi
  fi
  sleep 300
done
