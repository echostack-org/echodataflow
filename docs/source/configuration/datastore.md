# Datastore Configuration

This document provides detailed explanations for the keys used in the provided YAML configuration used to define an Echodataflow run.

## Run Details

- `name`: This key specifies the name of the Echodataflow run. It is used to identify and label the execution of the Echodataflow process.

- `sonar_model`: This key indicates the model of the sonar used for data collection during the run.

- `raw_regex`: This key indicates the regex to be used while parsing the source directory to match the files to be processed.

## Input Arguments

- `urlpath`: This key defines the source data URL pattern for accessing raw data files. The pattern can contain placeholders that will be dynamically replaced during execution.

- `parameters`: This section holds parameters used in the source data URL. These parameters dynamically replace placeholders in the URL path.

- `storage_options`: This section defines storage options for accessing source data. It may include settings to anonymize access to the data.

- `group`: Provides information about the transect data, including the URL of the transect file and storage options.
  - `grouping_regex`: Define regex here to parse group name from the filename and combine files based on it.

- `group_name`: Specifies the name of the transect group when not using a file to pass transect information.

- `json_export`: When set to true, this key indicates that raw JSON metadata of files should be exported for processing.

- `raw_json_path`: This key defines the path where the raw JSON metadata will be stored. It can be used to skip parsing files in the source directory and instead fetch files from this JSON.

## Output Arguments

- `urlpath`: This key defines the destination data URL where processed data will be stored.

- `overwrite`: When set to true, this key specifies that the data should overwrite any existing data in the output directory.

- `retention`: Retains output at each stage, working as a central flag for all the processes but does not trump the save_offline flag.

- `storage_options`: This section defines storage options for the destination data, which may include details such as the block name and type.

## Logging
- `kafka`: Defines Kafka logging configuration.
  - `topic`: The Kafka topic to which logs will be sent.
  - `servers`: A list of Kafka servers to connect to.

## Notes

- The provided configuration serves as a structured setup for executing an Echodataflow run, allowing customization through the specified keys.
- Dynamic placeholders like `ship_name`, `survey_name`, and `sonar_model` are replaced with actual values based on the context.

Example:

```yaml
name: Bell_M._Shimada-SH1707-EK60 # Name of the Echodataflow Run
sonar_model: EK60 # Sonar Model
raw_regex: (.*)-?D(?P<date>\w{1,8})-T(?P<time>\w{1,6}) # Regex to parse the filenames
args: # Input arguments
  urlpath: s3://ncei-wcsd-archive/data/raw/{{ ship_name }}/{{ survey_name }}/{{ sonar_model }}/*.raw # Source data URL
  parameters: # Source data URL parameters
    ship_name: Bell_M._Shimada
    survey_name: SH1707
    sonar_model: EK60
  storage_options: # Source data storage options
    anon: true
  group: # Source data transect information
    file: ./x0007_fileset.txt # Transect file URL. Accepts .zip or .txt file. File name Must follow r"x(?P<transect_num>\d+)" regex or set group_name to process without grouping. To process all the files at the source, remove the transect section from this yaml.
    grouping_regex: x(?P<transect_num>\d+) # Regex to parse group name from the filename. Skip if you want everything into default group or entire file name as group name
    storage_options: # Transect file storage options
      block_name: echodataflow-aws-credentials # Block name. For more information on Blocks refer blocks.md
      type: AWS # Block type
  group_name: default_group # Set when not using a file to pass transect information or group all files under one group. Skip if you want file name as group name.
  json_export: true # Export raw json metadata of files to be processed
  raw_json_path: s3://echodataflow-workground/combined_files/raw_json # Path to store the raw json metadata. Can also work to skip the process of parsing the files at source directory and fetch files present in this json instead.
output: # Output arguments
  urlpath: s3://echodataflow-workground/combined_files_dask # Destination data URL parameters
  overwrite: true # Flag to overwrite the data if present in the output directory
  retention: false # Deletes all the data stored while executing the pipeline
  storage_options: # Destination data storage options
    block_name: echodataflow-aws-credentials
    type: AWS
logging:
  kafka:
    topic: echodataflow_logs
    servers:
    - localhost:9092
```


Here are a couple of example configurations used during the demo:
1. [Local Configuration](../local/datastoreconfiguration.md)
2. [AWS Configuration](../aws/datastoreconfiguration.md)
