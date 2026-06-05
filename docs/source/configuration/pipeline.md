# Data Processing Pipeline Configuration

This documentation outlines the YAML configuration used to define and manage the data processing pipeline. The configuration governs the execution of a series of stages, each responsible for specific processing tasks.

## Active Recipe

The `active_recipe` parameter specifies the recipe to be executed on the input data. This configuration influences the workflow by determining the specific actions taken within the pipeline.

## Dask Cluster Configuration

- `use_local_dask`: When set to `true`, initiates a local Dask cluster with three workers by default. This distributed computing framework enhances processing efficiency.

- `n_workers`: Specifies the number of workers for the local Dask cluster.

- `scheduler_address`: Option to specify the address for the Dask scheduler. Use this or `use_local_dask` to control cluster creation. For more precise control, refer to `prefect_config` to set `DaskTaskRunner`.

## Pipeline Configuration

The pipeline is composed of a sequence of stages, each with its own distinct attributes.

### Stage Configuration

- `recipe_name`: The name of the recipe associated with this stage.

- `stages`: A list of stages the data passes through within the pipeline.

### Stage Attributes

Each stage contains:

- `name`: The function to execute within this stage.

- `module`: The module in which the function is located.

- `options`: Configurable settings specific to the Echodataflow functionality.

#### Options

- `save_raw_file`: When `true`, saves the downloaded raw file to the output directory.

- `use_raw_offline`: Skips the download process, utilizing the raw file present in the output directory. Missing files are downloaded.

- `use_offline`: Skips the current process if Zarr files exist in the output directory.

- `out_path`: Configures the output directory for the current process.

### Prefect Configuration

The `prefect_config` section configures Prefect-related settings for the flow. Refer https://docs.prefect.io/2.11.5/concepts/flows/#flow-settings for all the options available for configuration.

- `retries`: Determines the number of retries the flow attempts before transitioning to a failure state.

- `task_runner`: Sets the task runner configuration for this specific stage. E.g., `DaskTaskRunner` with a designated address.

- `persist_result`: When `true`, persists Prefect results. Useful for advanced Prefect configurations.

- `result_storage`: Specifies the location and serializer for storing results.

### External Parameters

- `external_params`: This section allows configuring external parameters relevant to the function. Currently supports only primitive types. Parameters specific to the current process can be defined here.

## Example Stages

Here's an overview of some of the pipeline stages:

1. `echodataflow_open_raw`: Executes the `open_raw` function from the `echodataflow.stages.subflows.open_raw` module. Allows various options and Prefect configurations.

2. `echodataflow_combine_echodata`: Utilizes the `combine_echodata` function from the designated module, with relevant options and configurations.

3. `echodataflow_compute_Sv`: Executes the `compute_Sv` function, supporting offline mode.

4. `echodataflow_compute_MVBS`: Executes the `compute_MVBS` function, supporting offline mode. External parameters like `range_meter_bin` and `ping_time_bin` can be set here.

Example:

```yaml
active_recipe: standard # Specify the recipe to execute on input data
use_local_dask: false # set to true to spin up a local dask cluster of n_workers workers
scheduler_address: tcp://127.0.0.1:61918 # Specify scheduler address or use_local_dask to control cluster creation. For more granular control, under prefect_config, use DaskTaskRunner(address=<scheduler_address>)
pipeline: # List of pipeline configurations; only the active_recipe will be executed.
- recipe_name: standard # Name of the recipe
  stages: # list of processes to execute on the data
  - name: echodataflow_open_raw # Name of the function
    module: echodataflow.stages.subflows.open_raw # Module of the function
    options: # Echodataflow configuration options
      save_raw_file: true # Save the downloaded raw file to output directory. Refer <link> for more information on how to configure output directory.
      use_raw_offline: true # Skip the download process and take the raw file present in the output directory. Note: Missing files will be downloaded in the output directory.
      use_offline: true # Skip this process if zarr files are already present in the output directory.
      out_path: ./temp_files # Configure the output directory for this process
    prefect_config: # Configure any prefect related settings for a flow. For an exhaustive list of configurations refer <https://docs.prefect.io/2.11.5/concepts/flows/#flow-settings>. Task based configurations are optimized and handled by echodataflow
      retries: 3 # Number of retries before failing the flow
      task_runner: DaskTaskRunner(address=tcp://127.0.0.1:59487) # Configure Runner setting for this specific stage
      persist_result: true # Persist the prefect results. Note: By default the output will be stored in the output directory, this option should only be used if dealing with advanced prefect configuration and integration
      result_storage: LocalFileSystem(basepath=my-results) # Location and type of serializer to be used for storing the result
    external_params: # External parameters relevant to the function can be configured using below. Currently only primitive types are supported under this configuration
      sonar_model: EK60
      xml_path: s3//
  - name: echodataflow_combine_echodata
    module: echodataflow.stages.subflows.combine_echodata
    options:
      use_offline: true
    prefect_config:
      retries: 0
      task_runner: DaskTaskRunner(address=tcp://127.0.0.1:59487)
  - name: echodataflow_compute_Sv
    module: echodataflow.stages.subflows.compute_Sv
    options:
      use_offline: true
  - name: echodataflow_compute_MVBS
    module: echodataflow.stages.subflows.compute_MVBS
    options:
      use_offline: true
    external_params:
      range_meter_bin: 20m
      ping_time_bin: 20S
```

Here are a couple of example configurations used during the demo:
1. [Local Configuration](../local/pipelineconfiguration.md)
2. [AWS Configuration](../aws/pipelineconfiguration.md)
