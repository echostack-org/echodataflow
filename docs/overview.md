(overview)=
# How Echodataflow works

Echodataflow executes echosounder data processing pipelines by following the "recipes," or text-based specification of the workflow steps and parameters. It is developed to streamline reusing and experimenting data processing steps and parameter combinations following the same general blueprints. 




## Recipes

Each data processing pipeline is defined by a pair of recipes: the "deployment" and "parameters" recipes. The recipes are organized around the concept of "flow," whcih is the central container of workflow logic in Prefect. The flows are regular Python functions decorated by `@flow` that makes it possible for Prefect to execute and monitor the operations.

In Echodataflow:
- The **deployment recipe** contains flow deployment specifications that controls _how_ the flows are run, such as timing and computing resources.
- The **parameters recipe** contains input parameters for the flow functions, which controls _what_ are run in the flows.

Under the hood, the Echodataflow deployment engine parses the recipes to assemble and execute the workflow using mechanisms available from the the open-source [Prefect](https://www.prefect.io/) workflow orchestration package. 

The flows currently available in the Echodataflow package were developed to support the development team's research work to execute near-real-time echosounder data processing on ocean platforms and post-processing archived data. See [](recipe-spec) for details on preparing the deployment parameters recipes and the [Echodataflow-recipes repository](https://github.com/OSOceanAcoustics/echodataflow-recipes) for reproducible examples of near-real-time and post-processing worflows.




## Local deployment setup
To deploy a pipeline on a local computere, we need a local Prefect server and workers in work pools:
- The [server](https://docs.prefect.io/v3/concepts/server) orchestrates the execution of flows and provides visibility into the status of each flow run
- The [work pools](https://docs.prefect.io/v3/concepts/work-pools) connect the orchestration layer to the computing infrastructure where flows run and define how that infrastructure is configured.


(server-workpool-terminal)=
For quick local testing, start a Prefect server and a work pool in separate terminals:

1. In the first terminal, start the server:

   ```shell
   prefect server start
   ```

2. In the second terminal, configure Prefect to connect to the local server, then start a worker for the `local` work pool:

   ```shell
   prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
   prefect worker start --pool local --type process
   ```

   To confirm that the work pool exists, run
   ```shell
   prefect work-pool ls
   ```

Open the Prefect dashboard at <http://127.0.0.1:4200> to monitor flow runs. Keep the server and worker running while testing deployments.



:::{note}
For operational deployments from a local server, these can be run via system services so that they start automatically on boot and restart if they fail. See [](deployment-system-services) for details on running them on different operating systems.
:::





## Use Prefect Cloud
It is also possible to use Prefect Cloud to orchestrate flow executions without running a local Prefect server. Below we show the steps to authenticate with Prefect Cloud and start a worker locally that Prefect Cloud can orchestrate.

1. To authenticate and select the workspace that will receive your deployments, run:
```shell
prefect cloud login
prefect cloud workspace set --workspace "<account-handle>/<workspace-handle>"
```

You can also manually configure the `PREFECT_API_URL` setting to specify the Prefect Cloud API. See the [Prefect Cloud connection guide](https://docs.prefect.io/v3/how-to-guides/cloud/connect-to-cloud)
for details and API key configuration.


2. To execute flows on your computer while Prefect Cloud handles orchestration, start a local worker in the environment configured above by running:

```shell
prefect worker start --pool local --type process
```


:::{note}
If the worker is not connected correctly to Prefect Cloud, check if `PREFECT_API_URL` is incorrectly set to a local server or points to the wrong workspace:
```shell
`prefect config view | grep PREFECT_API_URL`
```
It may be useful to unset it before logging in by running:
```shell
prefect config unset PREFECT_API_URL
```
even though `prefect cloud login` normally sets the Cloud workspace URL and API key itself.
:::
