(prefect-cloud)=
# Use Prefect Cloud
It ispossible to use Prefect Cloud to orchestrate flow executions without running a local Prefect server. Below we show the basic steps to authenticate with Prefect Cloud and start a worker locally that Prefect Cloud can orchestrate. See the official [Prefect Cloud documentation](https://www.prefect.io/docs/v3/how-to-guides/cloud/connect-to-cloud#how-to-connect-to-prefect-cloud) for more information.

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
