"""Shared deployment helpers for rewrite cloud and ship entrypoints."""

from __future__ import annotations

import datetime
import importlib.util
import inspect
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from prefect.deployments.runner import RunnerDeployment
from prefect.events import DeploymentEventTrigger
from prefect.flows import Flow
from prefect.variables import Variable
from yaml import safe_load

from echodataflow.deployment.core import (
    ALLOWED_CONCURRENCY_GROUP_KEYS,
    ALLOWED_CONCURRENCY_LIMIT_KEYS,
    ALLOWED_DASK_CLUSTER_KEYS,
    ALLOWED_DEPLOY_KEYS,
    ALLOWED_FLOW_DEPLOY_KEYS,
    ALLOWED_GIT_SOURCE_KEYS,
    ALLOWED_SOURCE_KEYS,
    ALLOWED_TASK_RUNNER_KEYS,
    ALLOWED_TRIGGER_KEYS,
    DEFAULT_ENTRYPOINT_ROOT,
    TASK_RUNNER_ENV_VAR,
)


@dataclass(frozen=True)
class DeploymentSpec:
    flow_key: (
        str  # the flow key from deploy config, used to look up flow params and deploy settings
    )
    deployment_name: str  # the deployment name to use for this flow
    flow_obj: Flow[..., Any]  # the actual Flow object resolved from the registry
    entrypoint: str  # source-relative entrypoint for the actual deployed flow
    parameters: dict[str, Any]  # parameters passed directly to the deployed flow
    concurrency_group: str | None = None
    concurrency_limit: dict[str, Any] | None = None
    task_runner: dict[str, Any] | None = None
    cron: str | None = None  # precomputed cron schedule, when interval mode is used
    work_pool_name: str | None = (
        None  # the work pool name to use for this deployment, if different from default
    )
    triggers: list[Any] | None = None  # precomputed Prefect trigger objects


def resolve_registered_flows(
    deploy_cfg: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Validate and load only the registry flows requested by a deploy recipe."""
    from echodataflow.deployment.flow_registry import FLOW_REGISTRY

    resolved: dict[str, dict[str, Any]] = {}
    for recipe_key, deploy_meta in deploy_cfg.get("flows", {}).items():
        if not isinstance(deploy_meta, dict):
            raise ValueError(f"deploy_cfg.flows.{recipe_key} must be a mapping")

        registry_key = deploy_meta.get("flow", recipe_key)
        if not isinstance(registry_key, str) or not registry_key.strip():
            raise ValueError(f"deploy_cfg.flows.{recipe_key}.flow must be a non-empty string")
        registry_key = registry_key.strip()

        try:
            registration = FLOW_REGISTRY[registry_key]
        except KeyError as e:
            available = ", ".join(sorted(FLOW_REGISTRY)) or "<none>"
            raise KeyError(
                f"Flow {registry_key!r}, requested by recipe entry {recipe_key!r}, "
                f"is not registered. Available flows: {available}"
            ) from e

        entrypoint_path, separator, function_name = registration.entrypoint.partition(":")
        if not separator or not entrypoint_path.endswith(".py") or not function_name:
            raise ValueError(
                f"Registered flow {registry_key!r} has invalid entrypoint "
                f"{registration.entrypoint!r}; expected 'package/module.py:function'"
            )
        module_name = entrypoint_path.removesuffix(".py").replace("/", ".")

        try:
            flow_module_obj = importlib.import_module(module_name)
        except ImportError as e:
            raise ImportError(f"Failed to import registered flow module {module_name}: {e}") from e

        try:
            flow_obj = cast(Flow[..., Any], getattr(flow_module_obj, function_name))
        except AttributeError as e:
            raise AttributeError(
                f"Registered flow {registry_key!r} points to missing function "
                f"{function_name!r} in {module_name}"
            ) from e

        resolved[recipe_key] = {
            "flow_obj": flow_obj,
            "entrypoint": registration.entrypoint,
        }

    return resolved


def load_config(config_path: Path) -> dict[str, Any]:
    with open(config_path, "r") as file:
        return safe_load(file)


def _infer_local_source_root() -> Path:
    """Infer local source root from installed echodataflow package location."""
    spec = importlib.util.find_spec("echodataflow")
    if spec is None:
        raise ValueError("Could not locate installed 'echodataflow' package")

    # Package installs point to .../<root>/echodataflow/__init__.py
    # Local source root should be <root>
    if spec.origin:
        return Path(spec.origin).resolve().parent.parent

    # Namespace package fallback
    if spec.submodule_search_locations:
        first = next(iter(spec.submodule_search_locations), None)
        if first:
            return Path(first).resolve().parent

    raise ValueError("Could not infer local source root from 'echodataflow' package")


def _validate_local_source_layout(local_source_root: Path) -> Path:
    """
    Validate the local source root and entrypoint exists
    and return the root to use for local deployments.
    """
    root = local_source_root.resolve()
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Local source root does not exist or is not a directory: {root}")

    candidate = root / DEFAULT_ENTRYPOINT_ROOT
    if not candidate.exists() or not candidate.is_dir():
        raise ValueError(
            "Required entrypoint directory was not found under local source root: "
            f"entrypoint_root={DEFAULT_ENTRYPOINT_ROOT!r}, local_source_root={root}"
        )

    return root


def resolve_deployment_source(
    deploy_cfg: dict[str, Any],
    log_context: str | None = None,
) -> Any:
    """
    Resolve deployment source based on deploy config and environment variable override.
    """
    source_cfg = deploy_cfg.get("source", {})
    if source_cfg is None:
        source_cfg = {}

    # Priority: 1) deploy config setting, 2) default to local
    mode = (source_cfg.get("mode") or "local").lower()

    if source_cfg.get("mode"):
        source_mode_origin = "deploy_cfg.source.mode"
    else:
        source_mode_origin = "default:local"

    if mode == "local":
        default_local_dir = _validate_local_source_layout(_infer_local_source_root())
        source = str(default_local_dir)
        if log_context:
            print(
                f"[{log_context}] source_mode={mode} "
                f"(origin={source_mode_origin}) target={source}"
            )
        return source

    if mode == "git":
        git_cfg = source_cfg.get("git", {})
        if not isinstance(git_cfg, dict):
            raise ValueError("Deploy source.git must be a mapping")
        url = git_cfg.get("url")
        if not url:
            raise ValueError("Deploy source.git.url is required when source mode is 'git'")

        # Import lazily so tests and local-only runs do not require Git storage objects
        from prefect.runner.storage import GitRepository

        branch = git_cfg.get("branch", "main")  # default to the "main" branch unless specified
        source = GitRepository(url=url, branch=branch)
        if log_context:
            print(
                f"[{log_context}] source_mode={mode} "
                f"(origin={source_mode_origin}) target={url}@{branch}"
            )
        return source

    raise ValueError(f"Unsupported deploy source mode: {mode}")


def get_time_offset_targets(deploy_cfg: dict[str, Any]) -> tuple[str, ...]:
    """Return flow names that should receive time_offset_seconds injection."""
    targets: list[str] = []
    for flow_name, deploy_meta in deploy_cfg.get("flows", {}).items():
        if not isinstance(deploy_meta, dict):
            continue
        if deploy_meta.get("inject_time_offset"):
            targets.append(flow_name)
    return tuple(targets)


def _compute_time_offset_seconds(flow_start_time: str | None) -> float:
    if flow_start_time is None:
        return 0.0

    curr_time_offset = datetime.datetime.now(
        datetime.timezone.utc
    ) - datetime.datetime.fromisoformat(flow_start_time).astimezone(datetime.timezone.utc)
    return curr_time_offset.total_seconds()


def build_cron(interval: int | None, cron_offset: int = 0) -> str | None:
    if interval is None:
        return None
    if cron_offset > 0:
        return f"{cron_offset}-59/{interval} * * * *"
    return f"*/{interval} * * * *"


def build_triggers(
    trigger_items: list[dict[str, Any]],
) -> list[Any]:
    triggers = []

    for item in trigger_items:
        kwargs = {
            "expect": {item["expect"]},
        }

        if item["resource_scope"] == "primary":
            kwargs["match"] = {
                "prefect.resource.name": item["resource_name"],
            }
        else:
            kwargs["match_related"] = {
                "prefect.resource.name": item["resource_name"],
                "prefect.resource.role": "deployment",
            }

        triggers.append(
            DeploymentEventTrigger(**kwargs)
        )

    return triggers

def _reject_unknown_keys(
    value: dict[str, Any],
    *,
    allowed: set[str],
    path: str,
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"Unsupported field(s) at {path}: {unknown}")


def _validate_positive_integer(value: Any, *, path: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{path} must be a positive integer")


def validate_task_runner_config(value: Any, *, path: str) -> None:
    """Validate the supported YAML representation of a task runner."""
    if not isinstance(value, dict):
        raise ValueError(f"{path} must be a mapping")
    _reject_unknown_keys(value, allowed=ALLOWED_TASK_RUNNER_KEYS, path=path)
    if value.get("type") != "dask":
        raise ValueError(f"{path}.type must be 'dask'")

    cluster_kwargs = value.get("cluster_kwargs", {})
    cluster_path = f"{path}.cluster_kwargs"
    if not isinstance(cluster_kwargs, dict):
        raise ValueError(f"{cluster_path} must be a mapping")
    _reject_unknown_keys(
        cluster_kwargs,
        allowed=ALLOWED_DASK_CLUSTER_KEYS,
        path=cluster_path,
    )
    for key in ("n_workers", "threads_per_worker"):
        if key in cluster_kwargs:
            _validate_positive_integer(cluster_kwargs[key], path=f"{cluster_path}.{key}")
    if "processes" in cluster_kwargs and not isinstance(cluster_kwargs["processes"], bool):
        raise ValueError(f"{cluster_path}.processes must be a boolean")


def validate_deploy_config(deploy_cfg: Any) -> None:
    """Reject unknown fields throughout a deployment specification."""
    if not isinstance(deploy_cfg, dict):
        raise ValueError("deploy_cfg must be a mapping")

    _reject_unknown_keys(deploy_cfg, allowed=ALLOWED_DEPLOY_KEYS, path="deploy_cfg")

    flows = deploy_cfg.get("flows")
    if not isinstance(flows, dict):
        raise ValueError("deploy_cfg.flows must be a mapping")

    concurrency_groups = deploy_cfg.get("concurrency_groups", {})
    if not isinstance(concurrency_groups, dict):
        raise ValueError("deploy_cfg.concurrency_groups must be a mapping")
    for group_name, group_config in concurrency_groups.items():
        group_path = f"deploy_cfg.concurrency_groups.{group_name}"
        if not isinstance(group_name, str) or not group_name.strip():
            raise ValueError("deploy_cfg.concurrency_groups keys must be non-empty strings")
        if not isinstance(group_config, dict):
            raise ValueError(f"{group_path} must be a mapping")
        _reject_unknown_keys(
            group_config,
            allowed=ALLOWED_CONCURRENCY_GROUP_KEYS,
            path=group_path,
        )
        if "limit" not in group_config:
            raise ValueError(f"{group_path}.limit is required")
        _validate_positive_integer(group_config["limit"], path=f"{group_path}.limit")

    for flow_key, deploy_meta in flows.items():
        flow_path = f"deploy_cfg.flows.{flow_key}"
        if not isinstance(deploy_meta, dict):
            raise ValueError(f"{flow_path} must be a mapping")
        _reject_unknown_keys(
            deploy_meta,
            allowed=ALLOWED_FLOW_DEPLOY_KEYS,
            path=flow_path,
        )

        registry_key = deploy_meta.get("flow")
        if registry_key is not None and (
            not isinstance(registry_key, str) or not registry_key.strip()
        ):
            raise ValueError(f"{flow_path}.flow must be a non-empty string")

        concurrency_group = deploy_meta.get("concurrency_group")
        if concurrency_group is not None:
            if not isinstance(concurrency_group, str) or not concurrency_group.strip():
                raise ValueError(f"{flow_path}.concurrency_group must be a non-empty string")
            if concurrency_group not in concurrency_groups:
                raise ValueError(
                    f"{flow_path}.concurrency_group references undefined group "
                    f"{concurrency_group!r}"
                )

        task_runner = deploy_meta.get("task_runner")
        if task_runner is not None:
            validate_task_runner_config(task_runner, path=f"{flow_path}.task_runner")

        concurrency_limit = deploy_meta.get("concurrency_limit")
        if concurrency_limit is not None:
            limit_path = f"{flow_path}.concurrency_limit"
            if not isinstance(concurrency_limit, dict):
                raise ValueError(f"{limit_path} must be a mapping")
            _reject_unknown_keys(
                concurrency_limit,
                allowed=ALLOWED_CONCURRENCY_LIMIT_KEYS,
                path=limit_path,
            )
            if "limit" not in concurrency_limit:
                raise ValueError(f"{limit_path}.limit is required")
            _validate_positive_integer(concurrency_limit["limit"], path=f"{limit_path}.limit")
            strategy = concurrency_limit.get("collision_strategy", "ENQUEUE")
            if strategy not in {"ENQUEUE", "CANCEL_NEW"}:
                raise ValueError(
                    f"{limit_path}.collision_strategy must be 'ENQUEUE' or 'CANCEL_NEW'"
                )

        triggers = deploy_meta.get("triggers")
        if isinstance(triggers, list):
            for index, trigger in enumerate(triggers):
                if isinstance(trigger, dict):
                    _reject_unknown_keys(
                        trigger,
                        allowed=ALLOWED_TRIGGER_KEYS,
                        path=f"{flow_path}.triggers[{index}]",
                    )

    source = deploy_cfg.get("source")
    if source is None:
        return
    if not isinstance(source, dict):
        raise ValueError("deploy_cfg.source must be a mapping")
    _reject_unknown_keys(source, allowed=ALLOWED_SOURCE_KEYS, path="deploy_cfg.source")

    git_source = source.get("git")
    if git_source is None:
        return
    if not isinstance(git_source, dict):
        raise ValueError("deploy_cfg.source.git must be a mapping")
    _reject_unknown_keys(
        git_source,
        allowed=ALLOWED_GIT_SOURCE_KEYS,
        path="deploy_cfg.source.git",
    )


def validate_optional_non_empty_list(
    value: Any,
    *,
    field_name: str,
    item_label: str,
) -> list[Any] | None:
    """Validate an optional list field that must be non-empty when provided."""
    if value is None:
        return None

    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    if len(value) == 0:
        raise ValueError(f"{field_name} must contain at least one {item_label}")

    return value


def validate_triggers(
    triggers: Any,
    *,
    flow_key: str,
) -> list[dict[str, Any]] | None:
    """
    Validate deployment trigger config.

    When configured, triggers must be a non-empty list of mappings with
    non-empty string values for `expect` and `resource_name`.
    """

    triggers = validate_optional_non_empty_list(
        triggers,
        field_name=f"deploy_cfg.flows.{flow_key}.triggers",
        item_label="trigger",
    )

    if triggers is None:
        return None

    validated_triggers: list[dict[str, Any]] = []

    for trigger_item in triggers:
        if not isinstance(trigger_item, dict):
            raise ValueError(f"deploy_cfg.flows.{flow_key}.triggers entries must be mappings")

        expect = trigger_item.get("expect")
        resource_name = trigger_item.get("resource_name")
        resource_scope = trigger_item.get(
            "resource_scope",
            "related",
        )

        if not isinstance(expect, str) or not expect.strip():
            raise ValueError(
                f"deploy_cfg.flows.{flow_key}.triggers entries "
                "must define a non-empty 'expect'"
            )

        if not isinstance(resource_name, str) or not resource_name.strip():
            raise ValueError(
                f"deploy_cfg.flows.{flow_key}.triggers entries "
                "must define a non-empty 'resource_name'"
            )

        if resource_scope not in {
            "primary",
            "related",
        }:
            raise ValueError(
                f"deploy_cfg.flows.{flow_key}.triggers "
                "resource_scope must be 'primary' or 'related'"
            )

        validated_triggers.append(
            {
                "expect": expect.strip(),
                "resource_name": resource_name.strip(),
                "resource_scope": resource_scope,
            }
        )

    return validated_triggers


def validate_flow_coverage(
    param_cfg: dict[str, Any],
    deploy_cfg: dict[str, Any],
) -> None:
    """Raise ValueError if param/deploy flows do not correspond with each other."""
    flows_cfg = param_cfg.get("flows")
    if not isinstance(flows_cfg, dict):
        raise ValueError("Param config file must contain a top-level 'flows' mapping")

    deploy_flows = deploy_cfg.get("flows")
    if not isinstance(deploy_flows, dict):
        raise ValueError("Deploy config must contain a top-level 'flows' mapping")

    config_flows = set(flows_cfg.keys())
    deploy_flow_keys = set(deploy_flows.keys())
    missing_from_deploy = config_flows - deploy_flow_keys
    missing_from_config = deploy_flow_keys - config_flows
    errors: list[str] = []
    if missing_from_deploy:
        errors.append(f"In config but missing from deploy: {sorted(missing_from_deploy)}")
    if missing_from_config:
        errors.append(f"In deploy but missing from config: {sorted(missing_from_config)}")
    if errors:
        raise ValueError("Flow coverage mismatch. " + " | ".join(errors))


def _flow_accepts_parameter(flow_obj: Any, parameter: str) -> bool:
    """Return True when the flow function accepts the named parameter.

    Prefect Flow objects expose the wrapped function via `.fn`. If a flow object
    does not expose an inspectable function (e.g. certain test doubles), we skip
    strict validation and allow the deployment build to proceed.
    """
    flow_fn = getattr(flow_obj, "fn", None)
    if not callable(flow_fn):
        return True

    signature = inspect.signature(flow_fn)
    return parameter in signature.parameters


def build_deploy_specs(
    *,
    param_cfg: dict[str, Any],
    deploy_cfg: dict[str, Any],
    resolved_flows: dict[str, dict[str, Any]],
) -> list[DeploymentSpec]:
    """
    Build deployment specs from deploy/param config and pre-filtered flows mapping.
    Specs contain fully compiled parameters and schedule/trigger metadata.
    """
    validate_deploy_config(deploy_cfg)

    specs: list[DeploymentSpec] = []
    flows_params = param_cfg["flows"]
    time_offset_targets = get_time_offset_targets(deploy_cfg)
    time_offset_seconds = _compute_time_offset_seconds(deploy_cfg.get("flow_start_time"))

    for key, deploy_meta in deploy_cfg.get("flows", {}).items():
        if not isinstance(deploy_meta, dict):
            continue

        flow_info = resolved_flows[key]

        # Check if time_offset_seconds is indeed accepted by the flows specified in deploy config
        if key in time_offset_targets and not _flow_accepts_parameter(
            flow_info["flow_obj"], "time_offset_seconds"
        ):
            raise ValueError(
                f"deploy_cfg.flows.{key}.inject_time_offset is enabled, "
                "but the target flow does not define 'time_offset_seconds'"
            )

        # Scheduling is optional for manually run deployments, but the two
        # supported scheduling mechanisms are mutually exclusive.
        if (deploy_meta.get("triggers") is not None) and (deploy_meta.get("interval") is not None):
            raise ValueError(
                f"deploy_cfg.flows.{key} must define only one of 'triggers' or 'interval'"
            )

        # Set up triggers or cron schedule based on deploy config
        triggers = validate_triggers(
            deploy_meta.get("triggers"),
            flow_key=key,
        )
        compiled_triggers = build_triggers(triggers) if triggers is not None else None

        cron: str | None = None
        if compiled_triggers is None:
            interval = deploy_meta.get("interval")
            cron = build_cron(interval, deploy_meta.get("cron_offset", 0))

        # Build deployment_parameters
        flow_params = flows_params.get(key)
        if not isinstance(flow_params, dict):
            raise ValueError(f"param_cfg.flows.{key} must be a mapping of flow parameters")

        deployment_parameters = dict(flow_params)
        if key in time_offset_targets:
            deployment_parameters["time_offset_seconds"] = time_offset_seconds
        specs.append(
            DeploymentSpec(
                flow_key=key,
                deployment_name=deploy_meta.get("deployment_name", key),
                flow_obj=flow_info["flow_obj"],
                entrypoint=flow_info["entrypoint"],
                parameters=deployment_parameters,
                concurrency_group=deploy_meta.get("concurrency_group"),
                concurrency_limit=deploy_meta.get("concurrency_limit"),
                task_runner=deploy_meta.get("task_runner"),
                cron=cron,
                work_pool_name=deploy_meta.get("work_pool_name"),
                triggers=compiled_triggers,
            )
        )

    return specs


def create_deployments(
    *,
    specs: list[DeploymentSpec],
    source: Any,
    default_work_pool_name: str,
) -> tuple[list[RunnerDeployment], list[RunnerDeployment]]:
    grouped: list[RunnerDeployment] = []
    standalone: list[RunnerDeployment] = []

    for spec in specs:
        flow_obj = spec.flow_obj
        deployment_kwargs: dict[str, Any] = {
            "name": spec.deployment_name,
            "parameters": dict(spec.parameters),
        }

        # Use precomputed schedule metadata from the deployment spec
        if spec.triggers is not None:
            deployment_kwargs["triggers"] = spec.triggers
        elif spec.cron is not None:
            deployment_kwargs["cron"] = spec.cron

        # Add work_pool_name if specified and different from default
        has_non_default_work_pool = (
            spec.work_pool_name is not None and spec.work_pool_name != default_work_pool_name
        )
        if has_non_default_work_pool:
            deployment_kwargs["work_pool_name"] = spec.work_pool_name

        if spec.concurrency_group is not None:
            deployment_kwargs["work_queue_name"] = spec.concurrency_group

        if spec.concurrency_limit is not None:
            from prefect.client.schemas.objects import (
                ConcurrencyLimitConfig,
                ConcurrencyLimitStrategy,
            )

            concurrency_limit = dict(spec.concurrency_limit)
            concurrency_limit["collision_strategy"] = ConcurrencyLimitStrategy(
                concurrency_limit.get("collision_strategy", "ENQUEUE")
            )
            deployment_kwargs["concurrency_limit"] = ConcurrencyLimitConfig(**concurrency_limit)

        # The worker reloads the flow entrypoint, so runner settings must be
        # present in its runtime environment instead of only on this Flow object
        if spec.task_runner is not None:
            deployment_kwargs["job_variables"] = {
                "env": {TASK_RUNNER_ENV_VAR: json.dumps(spec.task_runner)}
            }

        sourced_flow = flow_obj.from_source(
            source=source,
            entrypoint=spec.entrypoint,
        )
        deployment = sourced_flow.to_deployment(**deployment_kwargs)

        if has_non_default_work_pool:
            standalone.append(deployment)
        else:
            grouped.append(deployment)

    return grouped, standalone


def configure_concurrency_groups(
    *,
    specs: list[DeploymentSpec],
    concurrency_groups: dict[str, dict[str, Any]],
    default_work_pool_name: str,
) -> None:
    """Create or update shared, concurrency-limited Prefect work queues."""
    if not concurrency_groups:
        return

    from prefect.client.orchestration import get_client
    from prefect.exceptions import ObjectNotFound

    with get_client(sync_client=True) as client:
        for group_name, group_config in concurrency_groups.items():
            members = [spec for spec in specs if spec.concurrency_group == group_name]
            if not members:
                continue

            work_pools = {spec.work_pool_name or default_work_pool_name for spec in members}
            if len(work_pools) != 1:
                raise ValueError(
                    f"Concurrency group {group_name!r} spans multiple work pools: "
                    f"{sorted(work_pools)}"
                )
            work_pool_name = work_pools.pop()
            limit = group_config["limit"]

            try:
                queue = client.read_work_queue_by_name(
                    group_name,
                    work_pool_name=work_pool_name,
                )
            except ObjectNotFound:
                client.create_work_queue(
                    name=group_name,
                    concurrency_limit=limit,
                    work_pool_name=work_pool_name,
                )
            else:
                client.update_work_queue(queue.id, concurrency_limit=limit)
