(prefect-terminology)=
# Prefect terminology

Echodataflow uses Prefect to orchestrate processing on shipboard and cloud infrastructure. The following terms describe how processing code becomes a scheduled workflow. See the [Prefect concepts documentation](https://docs.prefect.io/v3/concepts/index) for more detail.

| Term | Meaning in Echodataflow |
| --- | --- |
| [Flow](https://docs.prefect.io/v3/concepts/flows/) | A Python function decorated with `@flow` that coordinates a workflow, such as converting newly arrived raw files to Sv or integrating acoustic and biological estimates. A flow can combine multiple tasks, call ordinary Python functions, and call other flows as subflows. |
| [Task](https://docs.prefect.io/v3/concepts/tasks/) | A Python function decorated with `@task` that defines an observable unit of work. Tasks support configurable retries, caching, and execution through a task runner. One task can wrap one operation or combine several operations into a processing step. |
| [Flow run / task run](https://docs.prefect.io/v3/concepts/states/) | One execution of a flow or task, with its own inputs, logs, and state, such as `Running`, `Completed`, or `Failed`. A deployment can produce many flow runs over time. |
| [Deployment](https://docs.prefect.io/v3/concepts/deployments/) | A stored configuration for running a flow, including its entrypoint, parameters, execution infrastructure, and optional schedule. Echodataflow creates deployments from YAML recipes; see [Recipe specification](recipe_spec.md). |
| [Server](https://docs.prefect.io/v3/concepts/server/) | The orchestration API, database-backed run history, and UI used to manage deployments and observe runs. Echodataflow can use a self-hosted Prefect server; Prefect Cloud provides a hosted orchestration service. |
| [Worker](https://docs.prefect.io/v3/concepts/workers/) | A process that polls a work pool for scheduled flow runs and starts them on the configured infrastructure. It needs access to the flow code, dependencies, and data required by those runs. |
| [Work pool](https://docs.prefect.io/v3/concepts/work-pools/) | A configuration connecting deployments to execution infrastructure, such as local processes or containers. Workers poll compatible pools to pick up work. |
| [Work queue](https://docs.prefect.io/v3/concepts/work-pools/#work-queues) | A queue within a work pool that groups runs and can control their priority and concurrency. Echodataflow uses work queues for shared concurrency groups across deployments. |
| [Task runner](https://docs.prefect.io/v3/concepts/task-runners/) | The execution mechanism for tasks submitted within a flow. It controls task concurrency or parallelism, separately from the worker that starts the flow run. |
| [Schedule](https://docs.prefect.io/v3/concepts/schedules/) | A rule that creates flow runs at specified times or intervals. Repeated runs let pipeline stages check for newly available input. |

For execution details, see Prefect's [task runner documentation](https://docs.prefect.io/v3/concepts/task-runners/). Calling a task directly runs it synchronously; `.submit()` or `.map()` submits work to the flow's task runner.
