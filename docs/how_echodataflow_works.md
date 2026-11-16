(overview)=
# How Echodataflow works

Echodataflow executes echosounder data processing pipelines by following the "recipes," or text-based specification of the pipeline steps and parameters. It is developed to streamline reusing and experimenting data processing steps and parameter combinations following the same general blueprints by leveraging the open-source [Prefect](https://www.prefect.io/) workflow orchestration package. See [](prefect-terminology) to learn about the orchestration concepts.





(overview_recipes)=
## Recipes

Each data processing pipeline is defined by a pair of recipes: the "deployment" and "parameters" recipes. The recipes are organized around the concept of "flow," whcih is the central container of workflow logic in Prefect.

- The **deployment recipe** contains flow deployment specifications that controls _how_ the flows are run, such as timing and computing resources.
- The **parameters recipe** contains input parameters for the flow functions, which controls _what_ are run in the flows.

Under the hood, the Echodataflow deployment engine parses the recipes to assemble and execute the pipeline using mechanisms available from Prefect. 

The flows currently available in the Echodataflow package were developed to support the development team's research work to execute near-real-time echosounder data processing on ocean platforms and post process archived data. See [](recipe-spec) for details on preparing the deployment and parameters recipes, and the [echodataflow-recipes repository](https://github.com/OSOceanAcoustics/echodataflow-recipes) for reproducible examples of these worflows.




(overview_simple-setup)=
## Simple deployment setup
To deploy a pipeline on a local computere, we need a local Prefect server and workers in work pools:
- The [server](https://docs.prefect.io/v3/concepts/server) orchestrates the execution of flows and provides visibility into the status of each flow run
- The [work pools](https://docs.prefect.io/v3/concepts/work-pools) connect the orchestrated operations to the computing infrastructure where flows run.


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


:::{tip}
:class: tip
For operational deployments from a local machine, the server and the worker can be run via system services so that they start automatically on boot. See [](infrastructure-system-services) for details.
:::


:::{note}
Prefect offers many other [deployment options](https://www.prefect.io/docs/v3/concepts/deployments) on:
1. Where orchestration runs: on a [self-hosted Prefect server](https://www.prefect.io/docs/v3/concepts/server) like what we set up above, or [using Prefect Cloud](prefect-cloud)
2. Where flows run: on infrastructures ranging from local processes like what we set up above, or via Docker, Kubernetes, and cloud serverless services. See Prefect's [work pools documentation](https://www.prefect.io/docs/v3/concepts/work-pools) for more detail.
:::






## Deploy the pipeline

Once the orchestration server and the computing infrastructure are set up, a pipeline can be deploy by running:

```shell
echodataflow-deploy run \
  --default-work-pool-name local \  # work pool name
  --deploy-spec deploy_pipeline.yaml \  # deployment recipe
  --param-config params_pipeline.yaml  # parameters recipe
```
