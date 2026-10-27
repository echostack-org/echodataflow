# Installation

Echodataflow is available and tested for Python 3.13-3.14. We recommend using a Conda environment because of its scientific and geospatial dependencies.


## Create a Conda environment

Create and activate an environment:

```shell
conda create --name echodataflow -c conda-forge python=3.13 uv
conda activate echodataflow
```

If your system does not already have rclone, include it when creating 
the environment by using the command below, then activate the environment:

```shell
conda create --name echodataflow -c conda-forge python=3.13 uv rclone
conda activate echodataflow
```

Once done, verify that rclone is installed by running:
```shell
rclone version
```


## Install Echodataflow

Echodataflow `v0.2.0` is currently under development and has not been released, and therefore cannot be installed directly from PyPI or conda yet.

### Instal as an installed package
To run Echodataflow as an installed package, install it directly from the repository:
```shell
uv pip install "echodataflow[dev,mission,inference] @ git+https://github.com/echostack-org/echodataflow.git"
```

### Install for development
To install Echodataflow for development, clone the repository and install from 
the local repository where you will be making code changes:
```shell
# clone the repo
git clone https://github.com/echostack-org/echodataflow.git

# switch into the repo directory
cd echodataflow

# install in editable mode from the local repo
uv pip install -e ".[dev,mission,inference]"
```

Adding `[dev,mission,inference]` in the `uv pip install` command above
install the full suite of dependencies specified in `pyproject.toml`:
- `dev`: testing, linting, code quality, documentation tools, and Jupyter kernel support.
- `mission`: mission-specific dependencies for the example pipelines.
- `inference`: the hake segmentation model, needed for running the `classify_backscatter` flow.




### Testing the installation

After installation, verify that the package is correctly installed by running:
```shell
python -c "import echodataflow; print(echodataflow.__version__)"
```

The installation enables the `echodataflow-deploy` command, which can be run from any directory. Testing that this works by running
```shell
echodataflow-deploy --help
```




:::{note}
The hake segmentation model weight can be separately downloaded from [LINK] and
set its path via the `path_weight` for the `classify_backscatter` flow to function.
:::



## Get the deployment recipes

Example recipes are maintained separately on a companion repository
[echodataflow-recipes](https://github.com/echostack-org/echodataflow-recipes).
You can download the recipes directly from the repo or get a copy by cloning it:

```shell
git clone https://github.com/echostack-org/echodataflow-recipes.git
```
