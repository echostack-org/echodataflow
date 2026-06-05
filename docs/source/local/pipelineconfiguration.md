# Pipeline Configuration
In this section, we will have a look at the pipeline configuration that we'll be using for our target strength processing. The configuration is presented in YAML format, which is a structured and human-readable way to define settings for data processing.

Here's the configuration we'll be using:

```yaml
active_recipe: target_strength
use_local_dask: true
n_workers: 5
pipeline:
- recipe_name: target_strength
  stages:
  - name: echodataflow_open_raw
    module: echodataflow.stages.subflows.open_raw
    options:
      save_raw_file: true
      use_raw_offline: true
      use_offline: true
  - name: echodataflow_compute_TS
    module: echodataflow.stages.subflows.compute_TS
    options:
      use_offline: true
```

<!-- Let's break down the components of this configuration:

- **active_recipe**: Specifies the recipe to be used for processing, which is set as "target_strength" in this case.

- **use_local_dask**: This flag indicates that we'll be utilizing a local Dask Cluster for parallel processing.

- **n_workers**: Determines the number of worker processes in the Dask Cluster. Here, we're using 5 workers for efficient parallelization.

- **pipeline**: This section defines the sequence of stages to execute. In this example, we're following the "target_strength" recipe, which comprises two stages.

- **echodataflow_open_raw**: This stage utilizes the `open_raw` subflow module to open raw data files. It includes options such as saving raw files, using raw data in offline mode, and utilizing offline data.

- **echodataflow_compute_TS**: This stage employs the `compute_TS` subflow module to compute target strength. It includes an option to use offline data. -->

**Note**: For a more comprehensive understanding of each option and its functionality, you can refer to the [Pipeline documentation](../configuration/pipeline.md).

Keep in mind that in this example, we'll be setting up a local Dask Cluster with 5 workers for parallel processing. This configuration will enable us to efficiently process our data for target strength analysis. To turn it off, toggle `use_local_dask` to false.

Feel free to explore and modify the configuration to understand better.
