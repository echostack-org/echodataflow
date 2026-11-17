# Edge-to-cloud integration of acoustic and trawl data

## Overview

This example integrates acoustic and trawl data to generate estimates of Pacific hake abundance and biomass in near real time. The workflow runs across a ship (the "edge") and the cloud:
- **Edge:** process raw echosounder files, detect hake with a deep learning echogram segmentation model, and upload reduced acoustic products, including nautical areal scattering coefficient (NASC) and mean volume backscattering strength (MVBS).Trawl biological data, such as fish length, weight, and sex, are uploaded separately.
- **Cloud:** combine acoustic and trawl data to estimate biomass and abundance, and display the results in interactive echogram and biological visualization apps

The recipes are in `recipes_nrt/edge2cloud_hake/` in the [echodataflow-recipes repository](https://github.com/echostack-org/echodataflow-recipes), with separate `edge/` and `cloud/` configurations. Follow Steps 1–3 below to run the edge workflows, and Step 4-6 to run the cloud workflows and visualization.

The example recipes are configured to use the acoustic data collected during the 2025 [Integrated West Coast Pelagics Survey (IWCPS)](https://www.fisheries.noaa.gov/west-coast/science-data/integrated-west-coast-pelagics-survey) and mock trawl data (i.e. not real catch data), both hosted on an [Open Storage Network](https://www.openstoragenetwork.org/) cloud bucket.

:::{note}
Raw acoustic data files and processed outputs can require substantial disk space. Make sure your local (edge) machine and the cloud virtual machine (VM) has room for downloaded files, intermediate products, and visualization caches. The data generated from the edge and cloud workflows will be stored in the folders `recipes_nrt/edge2cloud_hake/edge/data` and `recipes_nrt/edge2cloud_hake/cloud/data`, respectively, within your cloned `echodataflow-recipes` folder.
:::





## Deployment steps

### 1. Prepare the environment on the edge machine
Follow [](installation) to create an echodataflow environment, and then activate the environment by calling:

```shell
conda activate echodataflow
```



### 2. Run edge Prefect server and worker

Follow [](overview_simple-setup) to run a Prefect server and a worker on two terminals on a local machine (this is the "edge"). Keep these two terminals running when you try to run the workflow.

Once the server and the worker are running, you can open the Prefect dashboard [http://127.0.0.1:4200](http://127.0.0.1:4200) and check that a worker is online under **Work Pools → local → Workers**.




### 3. Deploy the edge workflows

#### Obtain recipes and resources
To deploy the edge workflows, we first make sure we have the recipes and resources ready:
- Clone the [echodataflow-recipes](https://github.com/echostack-org/echodataflow-recipes) repository:
    ```shell
    cd TEST_DIRECTORY  # switch into a test directory
    git clone https://github.com/echostack-org/echodataflow-recipes.git
    ```
    The `recipes_nrt/edge2cloud_hake` folder in the cloned repository has the necessary [deployment and parameters recipes](overview_recipes) and other necessary files in the `resources/` subfolder
- Download the echogram segmentation model that detects hake: download `hake_model.zip` from the GitHub [release asset](https://github.com/echostack-org/echodataflow-recipes/releases/tag/data-v1), unzip and put the content `hake_model.ckpt` under `recipes_nrt/edge2cloud_hake/resources`.


(rclone_config)=
#### Configure file upload destination
In a terminal, run `rclone config` and follow the [rclone configuration guide](https://rclone.org/commands/rclone_config/) to create a remote for your cloud storage, or use an existing rclone remote.

In the parameters recipe `recipes_nrt/edge2cloud_hake/edge/params.yaml`, the cloud storage location where we will upload data to should be specified in the form `remote:bucket/path/to/dir`.  There should be a corresponding `remote` section in `rclone.conf`. Run `rclone config file` to find the location of this configuration file.

The `remote` section should be an S3 remote section with explicit `access_key_id` and `secret_access_key` values.
For AWS S3, this would look like:
```ini
[aws_remote]
type = s3
provider = AWS
access_key_id = YOUR_ACCESS_KEY
secret_access_key = YOUR_SECRET_KEY
region = us-west-2
```

For OSN, this would look like:

```ini
[osn_remote]
type = s3
provider = Ceph
access_key_id = YOUR_ACCESS_KEY
secret_access_key = YOUR_SECRET_KEY
endpoint = https://sdsc.osn.xsede.org
```

Once rclone is configured, in `recipes_nrt/edge2cloud_hake/edge/params.yaml`, set `dest_dir` for both the acoustic and trawl data upload destination using the rclone format `remote:bucket/path/to/dir`:
```yaml
flows:
  file_upload_acoustics:
    dest_dir: aws_remote:my_bucket/test_data/acoustics
    ...
  file_upload_trawl:
    dest_dir: aws_remote:my_bucket/test_data/trawl
    ...
```


#### Deploy the edge workflows
Now we can deploy the edge workflows by running below in another terminal:
```shell
# switch to the recipes repo folder
cd TEST_DIRECTORY/echodataflow-recipes

# deploy the edge workflows
echodataflow-deploy run \
  --default-work-pool-name local \
  --deploy-spec recipes_nrt/edge2cloud_hake/edge/deploy.yaml \
  --param-config recipes_nrt/edge2cloud_hake/edge/params.yaml
```
Once the command runs through, explore the local Prefect dashboard [http://127.0.0.1:4200](http://127.0.0.1:4200) to see all the deployed deployments and other information.






### 4. Connect to Prefect Cloud and run a worker

To run the cloud workflows, we first make sure we have the environment and Prefect Cloud workspace ready by doing the following:
- Configure and start a cloud VM
- On the cloud VM, follow step 1 to install and activate an echodataflow environment, and clone the `echodataflow-recipes` repository following the beginning of step 3.
- Separately, create an account and workspace on [Prefect Cloud](https://app.prefect.cloud/), or use an existing workspace.

#### Connect to Prefect Cloud
Then we can connect to Prefect Cloud by running below in a terminal on the cloud VM:
```shell
# activate the echodataflow environment
conda activate echodataflow

# create a local configuration profile named my_cloud
prefect profile create my_cloud

# switch to use the my_cloud profile
prefect profile use my_cloud

# logs in on Prefect Cloud and create an API key
# and then login using the API key
prefect cloud login -k API_KEY

# list the available workspaces to this API key
prefect --profile my_cloud cloud workspace ls

# select the workspace for my_cloud by specifying one of the workspaces above
prefect --profile my_cloud cloud workspace set --workspace "ACCOUNT_HANDLE/WORKSPACE_HANDLE"
```

You can also select the workspace (the last step) interactively by:
```shell
# set your workspace interactively
prefect --profile my_cloud cloud workspace set
```


:::{note}
The `my_cloud` profile is a local configuration that stores connection settings for your Prefect Cloud workspace. It is separate from the account and workspace created on Prefect Cloud. See the [Prefect Cloud connection guide](https://docs.prefect.io/v3/how-to-guides/cloud/connect-to-cloud) for details.
:::

:::{tip}
The following Prefect commands may come in handy:
```shell
# view the current configuration
prefect config view

# view available profiles
prefect profile ls
```
:::



#### Run a worker
After establishing the Prefect Cloud connection above, run the following on the cloud VM to create the `cloud-local` work pool (as specified in `recipes_nrt/edge2cloud_hake/cloud/deploy.yaml`) and start a worker for it:
```shell
# create the work pool "cloud-local" and start a worker
prefect --profile my_cloud worker start --pool cloud-local --type process
```
Now, Prefect Cloud can orchestrate the runs that will be executed by this worker on the cloud VM.

Similar to the local Prefect dashboard, open your workspace in the [Prefect Cloud dashboard](https://app.prefect.cloud/) and check that a worker is online under **Work Pools → cloud-local → Workers**. 




### 5. Deploy cloud workflows


#### Configure cloud data product locations
Similar to what we did on the edge machine, we need to [config rclone](rclone_config) with a remote for the cloud storage location containing data uplaoded from the edge.

Once rclone is configured, in `recipes_nrt/edge2cloud_hake/cloud/params.yaml`, set the input paths to match the edge upload destinations using the rclone format `remote:bucket/path/to/dir`:

```yaml
flows:
  ingest_haul:
    path_bio_files: aws_remote:my_bucket/test_data/trawl
    ...
  ingest_NASC:
    path_NASC_files: aws_remote:my_bucket/test_data/acoustics/NASC
    ...
  update_cache_MVBS:
    path_MVBS: aws_remote:my_bucket/test_data/acoustics/MVBS
    ...
```



#### Deploy cloud workflows
In a terminal on the cloud VM, activate the environment and run the deployment command from the recipes repository using the `my_cloud` profile:
```shell
# activate the echodataflow environment
conda activate echodataflow

# switch to the recipes repo folder
cd TEST_DIRECTORY/echodataflow-recipes

# deploy the cloud workflows
PREFECT_PROFILE=my_cloud echodataflow-deploy run \
  --default-work-pool-name cloud-local \
  --deploy-spec recipes_nrt/edge2cloud_hake/cloud/deploy.yaml \
  --param-config recipes_nrt/edge2cloud_hake/cloud/params.yaml
```





### 6. Run visualization services

The cloud VM runs two visualization apps: echograms and biological estimates. Their settings are in `recipes_nrt/edge2cloud_hake/cloud/viz.yaml` in the recipes repository.

Wait for the cloud workflows to produce the required Zarr, CSV, and GeoJSON files before opening the visualizations.

#### Run the apps in terminals
In a terminal on the cloud VM, run:
```shell
# activate the echodataflow environment
conda activate echodataflow

# switch to the echodataflow-recipes
cd TEST_DIRECTORY/echodataflow-recipes

# start the echogram app
python -m echodataflow.viz.viz_echogram \
  --config-file recipes_nrt/edge2cloud_hake/cloud/viz.yaml
```

In another terminal on the cloud VM, run:
```shell
# activate the echodataflow environment
conda activate echodataflow

# switch to the echodataflow-recipes
cd TEST_DIRECTORY/echodataflow-recipes

# start the biological estimates app
python -m echodataflow.viz.viz_bio \
  --config-file recipes_nrt/edge2cloud_hake/cloud/viz.yaml
```
Keep both terminals running. With the default ports set in `viz.yaml`, open `http://<CLOUD_VM_HOST_>:1802` for the echogram apps and `http://<CLOUD_VM_HOST>:1804` for the biological estimates apps. Here, `CLOUD_VM_HOST` can be the VM hostname or IP address, or `localhost` when browsing on the VM itself.


#### (Alternative) Run as system services
For persistent operations, it is advantageous to run these visualizations as system services. In the Echodataflow package we supply `viz_echogram.service` and `viz_bio.service` templates under `src/echodataflow/viz/services` that can be run on a Linux cloud VM.

To run these services. Follow the steps below:
- Copy the environment template from the root of that repository and name it `viz_services.env`:
    ```shell
    mkdir -p ~/.config/echodataflow
    cp src/echodataflow/viz/services/viz_services.env.example_cloud \
      ~/.config/echodataflow/viz_services.env
    ```
- In `viz_services.env`, set the following variables:
    - `ECHODATAFLOW_ENV`: your echodataflow environment
    - `ECHODATAFLOW_HOME`: your home directory
    - `ECHODATAFLOW_VIZ_CONFIG`: absolute path of `recipes_nrt/edge2cloud_hake/cloud/viz.yaml`
    - `MAMBA_BIN`: full path of your conda or mamba executable
- Install `viz_echogram.service` and `viz_bio.service` as user services
    ```shell
    mkdir -p ~/.config/systemd/user
    cp src/echodataflow/viz/services/viz_echogram.service ~/.config/systemd/user/
    cp src/echodataflow/viz/services/viz_bio.service ~/.config/systemd/user/
    systemctl --user daemon-reload
    systemctl --user enable --now viz_echogram.service viz_bio.service
    systemctl --user status viz_echogram.service viz_bio.service
    ```
- To make sure these user services start at boot time and keep them running after logout, enable lingering for your account:
    ```shell
    sudo loginctl enable-linger "$USER"
    ```
- To inspect the logs for these services, run:
    ```shell
    journalctl --user -u viz_echogram.service -u viz_bio.service -f
    ```

:::{note}
If you make changes to `viz_services.env`, restart the apps with
```shell
systemctl --user restart viz_echogram.service viz_bio.service
```
:::
