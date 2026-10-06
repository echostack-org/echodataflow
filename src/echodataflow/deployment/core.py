DEFAULT_ENTRYPOINT_ROOT = "echodataflow/flows"
TASK_RUNNER_ENV_VAR = "ECHODATAFLOW_TASK_RUNNER"

ALLOWED_DEPLOY_KEYS = {
    "concurrency_groups",
    "flow_start_time",
    "default_work_pool_name",
    "source",
    "flows",
}
ALLOWED_FLOW_DEPLOY_KEYS = {
    "concurrency_group",
    "deployment_concurrency",
    "deployment_name",
    "flow",
    "interval",
    "cron_offset",
    "triggers",
    "inject_time_offset",
    "task_runner",
    "work_pool_name",
}
ALLOWED_CONCURRENCY_GROUP_KEYS = {"limit"}
ALLOWED_DEPLOYMENT_CONCURRENCY_KEYS = {
    "limit",
    "collision_strategy",
    "grace_period_seconds",
}
ALLOWED_TASK_RUNNER_KEYS = {"type", "cluster_kwargs"}
ALLOWED_DASK_CLUSTER_KEYS = {
    "memory_limit",
    "n_workers",
    "processes",
    "threads_per_worker",
}
ALLOWED_TRIGGER_KEYS = {
    "expect",
    "resource_name",
    "resource_scope",
}
ALLOWED_SOURCE_KEYS = {"mode", "git"}
ALLOWED_GIT_SOURCE_KEYS = {"url", "branch"}
