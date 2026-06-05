# Echodataflow

## Example notebook : [FlowDemo](FlowDemo.ipynb)

## Pre-requisite:
### Files :
1. [pipeline.yaml](/echodataflow/config/pipeline.yaml)
2. [datastore.yaml](/echodataflow/config/datastore.yaml)
3. logging_config.yaml (optional; still in progress for setting up external logging)
### Dataset :
- Hake.zip (contains two transects: x0007_fileset and x00010_fileset)
- Transects contain files from Summer 2017 for Summer 2017 for
    - survey ship : Bell_M._Shimada
    - survey name : SH1707
    - sonar model : EK60

## The workflow if executed with current configuration will execute the following steps:

### 1. Trigger pipeline :
- Parses the YAML configuration into a dictionary and performs basic validation checks.
### 2. Initialization Flow :
- Parses the datastore YAML to fetch all the raw files defined under the URL path corresponding to the specified transect.
- Sequentially executes the functions defined in pipeline.yaml for the collected list of files.
### 3. Open Raw :
- Processes a list of raw file URLs concurrently, performing the following actions for each file:
    * Downloads the raw file to the designated output path as specified in pipeline.yaml or creates a temporary folder if preferred.
    * Converts the raw file to the Zarr format and stores it in the output folder specified in datastore.yaml.
    * Deletes the temporary Zarr and raw files based on the defined retention settings.
    * Returns the echodata object.

### 4. Combine Echodata :
- Combines a list of echodata objects based on the transect defined in datastore.yaml.
- Returns the combined echodata object.
### 5. Compute Sv :
- Calculates Sv by processing a list of echodata objects, one per transect.
- Returns an Xarray Dataset.
### 6. Compute MVBS :
- Computes MVBS using the Sv values obtained from the previous step.
- Utilizes external binning parameters specified in pipeline.yaml.
