# Echodataflow

Echodataflow provides recipe-driven orchestration for echosounder data processing workflows.
It combines [Prefect](https://www.prefect.io/), YAML deployment recipes, and tools
such as [Echopype](https://github.com/echostack-org/echopype) to run workflows on different
computing infrastructures.

Echodataflow `v0.1.x` is deprecated. We are currently preparing for the `v0.2.0` release,
which contains a redesigned architecture and more straightforward mechanisms to add workflow
components.


## Why Echodataflow?

Operational data pipelines must do more than call a sequence of functions. They need to
notice new files, avoid overlapping runs, retry transient failures, keep processing state,
move selected products between edge and cloud systems, and make failures observable.
Echodataflow provides those orchestration concerns around scientific processing code.

Its main goals are to:

- define deployments without embedding mission-specific paths and schedules in Python;
- reuse tested scientific operations across interactive, scheduled, and event-driven runs;
- support continuous processing where network access may be intermittent;
- make the movement from prototype to operational workflow incremental; and
- expose execution state, logs, retries, and schedules through Prefect.

## Where to begin

- [Installation](installation.md) sets up Echodataflow and its system tools.
- [How Echodataflow works](guide.md) introduces flows, tasks, operations, and recipes.
- [Deployment](deployment.md) covers Prefect, macOS `launchd`, Linux `systemd`, and
  deploying workflows.
- [Examples](examples.md) explains common recipe patterns and walks through a simulated
  edge workflow.
- [Development](development.md) covers contribution practices and how to add a workflow
  step.
- [Workflow reference](reference.md) catalogs the currently registered flows and recipe
  fields.

## Project status

Recipes used for real missions live in the separate
[echodataflow-recipes](https://github.com/echostack-org/echodataflow-recipes) repository.
Treat those recipes as deployment-specific examples: review paths, credentials, schedules,
and resource requirements before using them on another system.
