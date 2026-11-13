# Near-real-time edge-to-cloud integration of acoustic and trawl data

## Overview

- What the workflow does
- Explain the edge vs cloud split of workflow components
- Explain the edge and cloud setup briefly and link to the specific steps below
- Add a note about large data outputs
- Point to the exact recipe folder



## Deployment steps

### 1. Prepare the environment on the edge machine
Follow [](installation) to create an echodataflow environment, and then activate the environment by calling:

```shell
conda activate echodataflow
```



### 2. Run edge Prefect server and worker

Follow [](overview_local-setup) to run a Prefect server and a worker on two terminals on a local machine (this is the "edge"). Keep these two terminals running when you try to run the workflow.

Once the server and the worker are running, you can open the Prefect dashboard [http://127.0.0.1:4200](http://127.0.0.1:4200) and check that a worker is online under **Work Pools → local → Workers**.




### 3. Deploy the edge workflows

To deploy the edge workflows, we first make sure we have the recipes and resources ready:
- clone the [`echodataflow-recipes`] repository:
    ```shell
    cd TEST_DIRECTORY  # switch into a test directory
    git clone https://github.com/echostack-org/echodataflow-recipes.git
    ```
    The `recipes_nrt/edge2cloud_hake` folder in the cloned repository has the necessary [deployment and parameters recipes](overview_recipes) and a list of cloud raw file paths in the `resources/` subfolder/
- download the echogram segmentation model that detects hake: download `hake_model.zip` from the GitHub [release asset](https://github.com/echostack-org/echodataflow-recipes/releases/tag/data-v1), unzip and put the content `hake_model.ckpt` under `recipes_nrt/edge2cloud_hake/resources`.

Then we can deploy the edge workflows by running below in another terminal:
```shell
# switch to the recipes repo folder
cd TEST_DIRECTORY/echodataflow-recipes

# deploy the edge workflows
echodataflow-deploy run \
  --deploy-spec recipes_nrt/edge2cloud_hake/edge/deploy.yaml \
  --param-config recipes_nrt/edge2cloud_hake/edge/params.yaml
```
Once the command runs through, explore the local Prefect dashboard [http://127.0.0.1:4200](http://127.0.0.1:4200) to see all the deployed deployments and other information.






### 4. Run cloud Prefect worker and connect to Prefect Cloud

To run the cloud workflows, we first make sure we have the environment and Prefect Cloud workspace ready:
- configure and start a cloud virtual machine (VM)
- on the cloud VM, follow step 1 to install and activate an echodataflow environment, and clone the `echodataflow-recipes` repository following step 3.
- separately, create an account and workspace on [Prefect Cloud](https://app.prefect.cloud/), or use an existing workspace.

#### Connect to Prefect Cloud
Then we can connect to Prefect Cloud by running below in another terminal on the cloud VM:
```shell
# activate the echodataflow environment
conda activate echodataflow

# create a local configuration profile named my_cloud
prefect profile create my_cloud

# logs in on Prefect Cloud and saves connection settings in my_cloud
# follow prompts to authenticate with a web browser or an API key
prefect --profile my_cloud cloud login

# select the workspace for my_cloud
# replace `<account-handle>/<workspace-handle>` with your account and workspace handles
prefect --profile my_cloud cloud workspace set --workspace "<account-handle>/<workspace-handle>"
```

:::{note}
The `my_cloud` profile is a local configuration that stores connection settings for your Prefect Cloud workspace. It is separate from the account and workspace created on Prefect Cloud.

See the [Prefect Cloud connection guide](https://docs.prefect.io/v3/how-to-guides/cloud/connect-to-cloud) for details.
:::


#### Run a worker in workpool `cloud-local`
After establishing the Prefect Cloud connection above, run the following on the cloud VM to create the `cloud-local` work pool (as specified in `recipes_nrt/edge2cloud_hake/cloud/deploy.yaml`) and start a worker for it:
```shell
# create the work pool "cloud-local" and start a worker
prefect --profile my_cloud worker start --pool cloud-local --type process
```
Keep this terminal running while using the workflow. 


#### Check Prefect Cloud dashboard
Now, Prefect Cloud can orchestrate the runs with the worker executing them on the cloud VM.

Similar to the local Prefect dashboard, open your workspace in the [Prefect Cloud dashboard](https://app.prefect.cloud/) and check that a worker is online under **Work Pools → cloud-local → Workers**. 

Use the same `my_cloud` profile when deploying the cloud workflows in step 5 (for example, by setting `PREFECT_PROFILE=my_cloud` in the deployment terminal).





### 5. Deploy cloud workflows

In another terminal on the cloud VM, activate the environment and run the deployment command from the recipes repository using the `my_cloud` profile:
```shell
# activate the echodataflow environment
conda activate echodataflow

# switch to the recipes repo folder
cd TEST_DIRECTORY/echodataflow-recipes

# deploy the cloud workflows
PREFECT_PROFILE=my_cloud echodataflow-deploy run \
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
- Copy the environment template from the root of that repository and name it `services.env`:
    ```shell
    mkdir -p ~/.config/echodataflow
    cp src/echodataflow/viz/services/services.env.example_cloud \
    ~/.config/echodataflow/services.env
    ```
- In `services.env`, set the following variables:
    - `ECHODATAFLOW_ENV`: your echodataflow environment
    - `MAMBA_BIN`: full path of your conda or mamba executable
    - `ECHODATAFLOW_VIZ_CONFIG`: absolute path of `recipes_nrt/edge2cloud_hake/cloud/viz.yaml`
    - `ECHODATAFLOW_WORKDIR`: absolute path of your echodataflow repository
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
If you make changes to `services.env`, restart the apps with `systemctl --user restart viz_echogram.service viz_bio.service`.
:::
