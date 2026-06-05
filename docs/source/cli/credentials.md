# Credentials

While the Prefect dashboard offers a user-friendly way to add AWS or other credentials used in the application, Echodataflow also provides CLI capabilities for managing credentials. This command allows you to add credentials in bulk from an .ini file and can sync with Prefect when needed.


## Subcommands

### `echodataflow load-credentials`

**Usage**: Load credentials from the .ini file and create corresponding credential blocks.

```sh
echodataflow load-credentials [--sync]
```

**Options**:
- `--sync`: If provided, syncs blocks updated using the Prefect Server.

**Example**:
To load all credentials from the .ini file located at ~/.echodataflow/credentials.ini and sync them with the Prefect Server, use:
```sh
echodataflow load-credentials --sync
```

### `echodataflow init`

**Usage**: To create working directory, add ruleset and initialize an empty credentials.ini file

```sh
echodataflow init
```
