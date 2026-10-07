# Installation

Echodataflow requires Python 3.12 or newer. A Conda environment is recommended because the processing stack contains compiled scientific and geospatial dependencies.


## Create a Conda environment

Create and activate an environment:

```shell
conda create --name echodataflow -c conda-forge python=3.13 uv
conda activate echodataflow
```

If your system does not already have rclone, include it when creating the environment by using the command below, then activate the environment:

```shell
conda create --name echodataflow -c conda-forge python=3.13 uv rclone
conda activate echodataflow
```

Once done, verify that rclone is installed by running:
```shell
rclone version
```


## Install from the repository

The Echodataflow `v0.2.0` release is still under development. To run Echodataflow as an installed package, install it directly from the repository:

```shell
uv pip install "echodataflow[dev,mission,inference] @ git+https://github.com/echostack-org/echodataflow.git"
```

The installation enables the `echodataflow-deploy` command, which can be run from any directory.
Verify that this is correctly installed by running:

```shell
echodataflow-deploy --help
python -c "import echodataflow; print(echodataflow.__version__)"
```

Adding `[dev,mission,inference]` in the `uv pip install` command above
install the full suite of optional dependencies specified in `pyproject.toml`:

- `dev`: testing, linting, code quality, documentation tools, and Jupyter kernel support.
- `mission`: Echopype, Echoshader, Echoregions, and other mission processing dependencies.
- `inference`: the hake segmentation model, needed for running the `predict_hake` flow.

:::{note}
The hake segmentation model weight can be separately downloaded from [LINK] and
set its path via the `path_weight` for the `predict_hake` flow to function.
:::




## Get the deployment recipes

Example recipes are maintained separately on a companion repository
[echodataflow-recipes](https://github.com/echostack-org/echodataflow-recipes).
You can download the recipes directly from the repo or get a copy by
cloning the repo:

```shell
git clone https://github.com/echostack-org/echodataflow-recipes.git
```

Each deployment uses two YAML files:

- `recipes/deploy/deploy_*.yaml` contains deployment configurations such as schedules, triggers, work pools, and source code location.
  selection.
- `recipes/params/params_*.yaml` contains parameters passed to the flows.

For any given deployment, the pair of files must contain the same keys below their top-level `flows` mappings.



## Install for development

To actively change Echodataflow code to add new flows or other modifications,
create and activate a conda environment, and then clone and install Echodataflow
in editable mode with the same full suite of optional dependencies:

```shell
# Creat and activate a conda environment for development
conda create --name echodataflow -c conda-forge python=3.13 uv
conda activate echodataflow

# Clone and install Echodataflow in editable mode (-e)
git clone https://github.com/echostack-org/echodataflow.git
cd echodataflow
uv pip install -e ".[dev,mission,inference]"
```

The `inference` extra installs `segmentation_inference`; download model weights separately
as described above. See [Development](development.md) for contribution instructions and testing.

After installation, follow [Deployment](deployment.md) to configure Prefect, start a worker,
and deploy workflows.
