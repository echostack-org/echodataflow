# Echodataflow

<div align="center">
  <!-- <a href="https://echodataflow.readthedocs.io/en/latest/?badge=latest">
    <img src="https://readthedocs.org/projects/echodataflow/badge/?version=latest" alt="docs"/>
  </a> -->
  <a href="https://codecov.io/gh/echostack-org/echodataflow">
    <img src="https://codecov.io/gh/echostack-org/echodataflow/graph/badge.svg?token=YTMVVHG585" alt="codecov"/>
  </a>
  <a href="https://www.prefect.io/">
    <img src="https://img.shields.io/badge/orchestration-Prefect-blue?logo=prefect&logoColor=white" alt="Prefect"/>
  </a>
  <a href="https://scientific-python.org/specs/spec-0000/">
    <img src="https://img.shields.io/badge/SPEC-0-green?labelColor=%23004811&color=%235CA038" alt="SPEC 0"/>
  </a>
</div>


Echodataflow provides recipe-driven orchestration for echosounder data processing workflows.
It combines [Prefect](https://www.prefect.io/), YAML deployment recipes, and tools
such as [Echopype](https://github.com/echostack-org/echopype) to run workflows on different
computing infrastructures.


Check out example recipes hosted on the companion repository [echodataflow-recipes](https://github.com/echostack-org/echodataflow-recipes).


**Note**: Echodataflow `v0.1.x` is deprecated. We are currently preparing for the `v0.2.0` release, which contains a redesigned architecture and more straightforward mechanisms to add pipeline components.


## Documentation

Learn more about echodataflow in the documentation at [echodataflow.readthedocs.io](https://echodataflow.readthedocs.io/), including [how to install](docs/installation.md). 



## Contributors

Echodataflow is currently developed and maintained by Wu-Jung Lee ([@leewujung](https://github.com/leewujung)) and Lloyd Izard ([@LOCEANlloydizard](https://github.com/LOCEANlloydizard)), with contributions from Caesar Tuguinay ([@ctuguinay](https://github.com/ctuguinay)). The project was initiated by Don Setiawan ([@lsetiawan](https://github.com/lsetiawan)). Soham Butala ([@Sohambutala](https://github.com/Sohambutala)) developed the first version with guidance from Valentina Staneva ([@valentina-s](https://github.com/valentina-s)).



## Acknowledgement
We thank funding support from the NOAA Northwest Fisheries Science Center and the Active Acoustics Strategic Inititave (AA-SI) to the development of Echodataflow.

<a href="https://upload.wikimedia.org/wikipedia/commons/7/79/NOAA_logo.svg?utm_source=commons.wikimedia.org&utm_campaign=index&utm_content=original" style="text-decoration:none;">
    <img src="https://upload.wikimedia.org/wikipedia/commons/7/79/NOAA_logo.svg?utm_source=commons.wikimedia.org&utm_campaign=index&utm_content=original" alt="NOAA" width="120">
</a>


## License

Echodataflow is distributed under the [Apache License 2.0](LICENSE).
