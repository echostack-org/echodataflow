# Infrastructure service templates

- `shared/`: Bash scripts used by both macOS and Linux
- `macos/`: launchd agents and example Prefect and SMB configuration
- `linux/`: systemd units and example Prefect and SMB configuration
- `windows/`: Task Scheduler definitions for the Prefect server and worker

Copy the templates for your OS and the required shared scripts to the installed
locations described in [Infrastructure system services](../../../docs/system_services.md).
The source layout does not change the installed script or configuration paths.

These files are package assets, rather than a Python package, and are included
explicitly in wheels and source distributions.
