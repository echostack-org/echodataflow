<!-- <div>
  <a href="https://echodataflow.readthedocs.io/en/latest/?badge=latest">
    <img src="https://readthedocs.org/projects/echodataflow/badge/?version=latest"/>
  </a>
  <a href="https://codecov.io/gh/echostack-org/echodataflow" > 
 <img src="https://codecov.io/gh/echostack-org/echodataflow/graph/badge.svg?token=YTMVVHG585"/> 
 </a>
</div> -->

# Echodataflow: Streamlined Data Pipeline Orchestration

Echodataflow streamlines echosounder data processing by combining [Prefect](https://www.prefect.io/)-based pipeline orchestration, YAML configuration, and [Echopype](https://github.com/echostack-org/echopype) into a modular tool for defining, configuring, and executing workflows.


**Note:** Echodataflow v.0.1.x have been deprecated. We will release v0.2.0 soon!



## Installation

1. Set up a computing environment using Conda:
   ```bash
   conda create --name echodataflow -c conda-forge python=3.12
   conda activate echodataflow
   ```

2. If you would like to run Echodataflow as an installed package, 
   install it from the repo like below:
   ```bash
   pip install git+https://github.com/echostack-org/echodataflow.git  # install from repo
   ```
   If you instead would like to install Echodataflow to develop it,
   clone the repo and install it like below:
   ```bash
   git clone git+https://github.com/echostack-org/echodataflow.git  # clone the repo
   pip install -e .[test,lint,docs]  # install in editable mode with dev tools
   ```
   


## Running the edge pipeline

1. Start the local Prefect server:
   ```bash
   prefect server start
   ```

2. In a new terminal, create and run a work pool:
   ```bash
   prefect worker start --pool "local"
   ```

3. Download the recipes from the [echodataflow-recipes repository](https://github.com/echostack-org/echodataflow-recipes) by clonining it to your computer:
   ```
   cd REPO_DIRECTORY  # switch to where you want the recipes repo to sit
   git clone https://github.com/echostack-org/echodataflow-recipes.git
   ```

4. Deploy and run the ship pipeline:
   ```bash
   python -m echodataflow.deployment.deploy_cli run --default-work-pool-name local --param-config REPO_DIRECTORY/recipes/params/config_ship_2025.yaml --deploy-spec REPO_DIRECTORY/recipes/deploy/deploy_ship_2025.yaml --source-mode local
   ```


## Running the cloud pipeline

1. Start a cloud virtual machine using the Linux platform

2. Start up a system service that runs a Prefect worker

3. Establish connection with the cloud Prefect server

4. Download the recipes from the [echodataflow-recipes repository](https://github.com/echostack-org/echodataflow-recipes) by clonining it to your computer:
   ```
   cd REPO_DIRECTORY  # switch to where you want the recipes repo to sit
   git clone https://github.com/echostack-org/echodataflow-recipes.git
   ```

5. Deploy and run the ship pipeline:
   ```bash
   python -m echodataflow.deployment.deploy_cli run --default-work-pool-name local --param-config REPO_DIRECTORY/recipes/params/config_cloud_2025.yaml --deploy-spec REPO_DIRECTORY/recipes/deploy/deploy_cloud_2025.yaml --source-mode local
   ```

6. Start up system services that hosts the 2 sets of visualization



## License

Echodataflow is licensed under the open source [Apache 2.0 license](https://opensource.org/license/Apache-2.0).
