# Code Generation

Echodataflow packages function calls to provide flexibility in configuring and running stages, as well as managing their attributes. This encapsulation allows for the execution and deployment of individual flows on any infrastructure. To facilitate this, Echodataflow provides a CLI command that generates the boilerplate code needed to create new stages. Simply add the necessary function calls, and your new stage is ready to go. Remember to add the new stage to the [rules](./rules) configuration.

## Subcommand

### `echodataflow gs`

**Usage**: To generate boilerplate code for a specific stage

```sh
echodataflow gs <stage_name>
```

**Arguments**:

<stage_name>: Name of the stage for which to generate boilerplate code.

**Example**

```sh
echodataflow gs compute_Sv
```

This command creates a template configuration file for the specified stage, allowing you to customize and integrate it into your workflow. The generated file includes:

- A `flow`: this orchestrates the execution of all files that need to be processed, either concurrently or in parallel, based on the configuration.
- A `task` (helper function): this assists the flow by processing individual files.

Here is a snippet of the generated file. Logging statements have been removed for brevity:

```python

@flow
@echodataflow(processing_stage="example-stage", type="FLOW")
def echodataflow_example_stage(
        groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]
):
    working_dir = get_working_dir(stage=stage, config=config)

    futures = defaultdict(list)

    for name, gr in groups.items():
        for ed in gr.data:
            gname = ed.out_path.split(".")[0] + ".Examplestage"
            new_process = process_example_stage.with_options(
                task_run_name=gname, name=gname, retries=3
                    )
            future = new_process.submit(
                ed=ed, working_dir=working_dir, config=config, stage=stage
                )
            futures[name].append(future)

    for name, flist in futures.items():
        try:
            groups[name].data = [f.result() for f in flist]
        except Exception as e:
            groups[name].data[0].error = ErrorObject(errorFlag=True, error_desc=str(e))

    return groups

@task
@echodataflow()
def process_example_stage(
    config: Dataset, stage: Stage, out_data: Union[Dict, Output], working_dir: str
):

    file_name = ed.filename + "_example stage.zarr"
    try:
        out_zarr = get_out_zarr(
            group=stage.options.get("group", True),
            working_dir=working_dir,
            transect=ed.group_name,
            file_name=file_name,
            storage_options=config.output.storage_options_dict,
        )

        if (
            stage.options.get("use_offline") == False
            or isFile(out_zarr, config.output.storage_options_dict) == False
        ):

            ed_list = get_ed_list.fn(config=config, stage=stage, transect_data=ed)
            xr_d = # Processing code
            xr_d.to_zarr(
                store=out_zarr,
                mode="w",
                consolidated=True,
                storage_options=config.output.storage_options_dict,
            )

        ed.out_path = out_zarr
        ed.error = ErrorObject(errorFlag=False)
    except Exception as e:
        ed.error = ErrorObject(errorFlag=True, error_desc=e)
    finally:
        return ed

```
