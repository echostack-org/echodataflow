# Datastore Configuration

In this section, we'll delve into the configuration that defines how the data will be organized and managed for processing. This configuration is provided in YAML format and plays a crucial role in structuring data inputs and outputs.

Here's the detailed breakdown of the configuration:

```yaml
name: Bell_M._Shimada-SH1707-EK60
sonar_model: EK60
raw_regex: (.*)-?D(?P<date>\w{1,8})-T(?P<time>\w{1,6})
args:
  urlpath: s3://ncei-wcsd-archive/data/raw/{{ ship_name }}/{{ survey_name }}/{{ sonar_model }}/*.raw
  parameters:
    ship_name: Bell_M._Shimada
    survey_name: SH1707
    sonar_model: EK60
  storage_options:
    anon: true
  group:
    file: ./EK60_SH1707_Shimada.txt
  group_name: 2017
  json_export: true
output:
  urlpath: ./echodataflow-output
  retention: false
  overwrite: true
```

<!-- Let's delve into the individual components of the configuration presented here:

- **name**: Specifies a descriptive name for the configuration, aiding in identifying its purpose.

- **sonar_model**: Indicates the type of sonar being utilized, which in this case is "EK60".

- **raw_regex**: Defines a regular expression pattern for extracting date and time information from raw data file names.

- **args**: This section provides crucial arguments for structuring data inputs:

  - **urlpath**: Defines the URL pattern to access the raw data files stored on a remote server. The placeholders `{{ ship_name }}`, `{{ survey_name }}`, and `{{ sonar_model }}` are dynamically replaced with the specified values.

  - **storage_options**: Sets storage options, such as anonymous access (`anon: true`), for retrieving the data.

  - **transect**: Specifies a file (`EK60_SH1707_Shimada.txt`) containing the list of files to process, along with default transect information.

  - **json_export**: Enables JSON metadata export.

- **output**: This section configures the output settings for processed data:

  - **urlpath**: Determines the output directory (`./echodataflow-output`) where the processed data will be stored.

  - **retention**: Disables data retention, indicating that only Target Strength data will be stored in this case.

  - **overwrite**: Allows data overwriting if the data already exists. -->

**Note**:
- For a more comprehensive understanding of each option and its functionality, you can refer to the [Datast documentation](../configuration/datastore.md/).
- The pipeline will store Target Strength output under `./echodataflow-output`. As the retention is set to false, only Target Strength files will be stored. To specify files for processing, create a list of file names and store it in `EK60_SH1707_Shimada.txt`, which should be placed under the transect directory.

This configuration facilitates efficient data organization and management for the processing pipeline. Feel free to tailor it to your specific data and processing requirements.
