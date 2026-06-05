# Rules

Configuring rules is essential in data pipelines as it ensures that the application runs in a logically valid flow. Echodataflow validates the stages to be executed against preconfigured rules. If new rules need to be added, they can be easily configured using the CLI.

**Pre-configured rules**:

The following are some of the pre-configured rules that Echodataflow checks:

```text
echodataflow_open_raw:echodataflow_compute_Sv
echodataflow_open_raw:echodataflow_combine_echodata
echodataflow_open_raw:echodataflow_compute_TS
echodataflow_combine_echodata:echodataflow_compute_Sv
echodataflow_compute_Sv:echodataflow_compute_MVBS
echodataflow_compute_Sv:echodataflow_frequency_differencing
echodataflow_frequency_differencing:echodataflow_apply_mask
```

## Subcommand

### `echodataflow rules`

**Usage** : To view or add flow rules

```bash
echodataflow rules [--add] [--add-from-file] [--clean]
```

**Options**:

- `--add`: Add a new rule interactively. Requires input in `parent_flow:child_flow` format.

**Example**:
```sh
echodataflow rules --add
Please enter the parent and child allowed flow in `parent_flow:child_flow` format: echodataflow_compute_Sv:echodataflow_compute_MVBS
```


- `--add-from-file`: Path to a file containing rules to be added. Each rule should be on a new line in `parent_flow:child_flow` format.

**Example**:
```sh
echodataflow rules --add-from-file path/to/rules.txt
```


- `--clean`: Clean and validate rules in the ruleset.

**Example**:
```sh
echodataflow rules --clean
```
