# Pipeline Configuration: Mean Volume Backscattering Strength on AWS
In this section, we will provide you with the pipeline configuration that we'll be using for our MVBS processing. The configuration is presented in YAML format, which is a structured and human-readable way to define settings for data processing.

Here's the configuration we'll be using:

```yaml
active_recipe: standard
use_local_dask: true
n_workers: 3
pipeline:
- recipe_name: standard
  stages:
  - name: echodataflow_open_raw
    module: echodataflow.stages.subflows.open_raw
    options:
      save_raw_file: true
      use_raw_offline: true
      use_offline: true
  - name: echodataflow_combine_echodata
    module: echodataflow.stages.subflows.combine_echodata
    options:
      use_offline: true
  - name: echodataflow_compute_Sv
    module: echodataflow.stages.subflows.compute_Sv
    options:
      use_offline: true
  - name: echodataflow_compute_MVBS
    module: echodataflow.stages.subflows.compute_MVBS
    options:
      use_offline: true
    external_params:
      range_meter_bin: 20
      ping_time_bin: 20S

```

<!-- Let's break down the components of this configuration:

- **active_recipe**: Specifies the recipe to be used for processing, which is set as "standard" in this case.

- **use_local_dask**: This flag indicates that we'll be utilizing a local Dask Cluster for parallel processing.

- **n_workers**: Determines the number of worker processes in the Dask Cluster. Here, we're using 3 workers for efficient parallelization.

- **pipeline**: This section defines the sequence of stages to execute. In this example, we're following the "standard" recipe, which comprises four stages.

    - **echodataflow_open_raw**: This stage utilizes the `open_raw` subflow module to open raw data files. It includes options such as saving raw files, using raw data in offline mode, and utilizing offline data.

    - **echodataflow_combine_echodata**: This stage employs the `combine_echodata` subflow module to combine echodatas based on transect. It includes an option to use offline data.

    - **compute_Sv**: This stage employs the `compute_Sv` subflow module to compute Backscattering Strength. It includes an option to use offline data.

    - **compute_MVBS**: This stage employs the `compute_MVBS` subflow module to calculate MVBS. It includes an option to use offline data. -->

**Note**: For a more comprehensive understanding of each option and its functionality, you can refer to the [Pipeline documentation](../configuration/pipeline.md).

Keep in mind that in this example, we'll be setting up a local Dask Cluster with 3 workers for parallel processing. This configuration will enable us to efficiently process our data for MVBS analysis. To turn it off, toggle `use_local_dask` to false.

Feel free to explore and modify the configuration to understand better.
