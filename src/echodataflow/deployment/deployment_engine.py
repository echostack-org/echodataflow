"""Shared deployment helpers for rewrite cloud and ship entrypoints."""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, cast
import importlib.util

from prefect.deployments.runner import RunnerDeployment
from prefect.events import DeploymentEventTrigger
from prefect.flows import Flow
from prefect.variables import Variable
from yaml import safe_load

from echodataflow.deployment.core import DEFAULT_ENTRYPOINT_ROOT

DEFAULT_DEPLOYMENT_WRAPPER_ENTRYPOINT = "echodataflow/flows/flows_helper.py:flow_deployment_wrapper"


@dataclass(frozen=True)
class DeploymentSpec:
    flow_key: str  # the flow key from deploy config, used to look up flow params and deploy settings
    deployment_name: str  # the deployment name to use for this flow
    flow_module: str  # the module name as string
    flow_name: str  # the flow function name including flow_ prefix
    flow_obj: Flow[..., Any] | None = None  # the actual Flow object, resolved from flow_module and flow_name
    flow_alias: str | None = None  # an optional alias for the flow, used for deployment naming
    cron_offset: int = 0  # the cron offset in minutes, used to stagger deployments
    work_pool_name: str | None = None  # the work pool name to use for this deployment, if different from default
    triggers: list[dict[str, Any]] | None = None  # a list of trigger definitions, each with 'expect' and 'resource_name' keys
    emit_events: list[str] | None = None  # a list of event names to emit after the flow completes, used for triggering downstream flows


def discover_all_flows() -> dict[str, dict[str, Any]]:
    """
    Discover all flow_* functions from all modules in echodataflow.flows folder.
    Returns mapping: flow_name -> {"flow_obj", "flow_module", "flow_function_name"}
    """

    flows_pkg_spec = importlib.util.find_spec("echodataflow.flows") # this points to __init__.py
    if flows_pkg_spec is None or flows_pkg_spec.origin is None:
        raise ValueError("Could not locate echodataflow.flows package")
    
    flows_dir = Path(flows_pkg_spec.origin).parent  # this points to src/echodataflow/flows
    discovered: dict[str, dict[str, Any]] = {}
    
    # Find all .py files in flows directory (excluding __init__.py)
    for py_file in flows_dir.glob("*.py"):
        if py_file.name.startswith("_"):
            continue
        
        module_name = py_file.stem  # filename without .py
        try:
            flow_module = importlib.import_module(f"echodataflow.flows.{module_name}")
        except ImportError as e:
            raise ImportError(f"Failed to import echodataflow.flows.{module_name}: {e}")
        
        # Find all flow_* attributes in the module
        for flow_function_name in dir(flow_module):
            if not flow_function_name.startswith("flow_"):
                continue

            flow_name = flow_function_name.removeprefix("flow_")
            flow_obj = cast(Flow[..., Any], getattr(flow_module, flow_function_name))
            
            discovered[flow_name] = {
                "flow_module": module_name,  # the module name as string
                "flow_function_name": flow_function_name,  # flow function name including flow_ prefix
                "flow_obj": flow_obj,  # the actual Flow object
            }
    
    return discovered


def filter_flows_for_deploy(
    all_flows: dict[str, dict[str, Any]],
    deploy_cfg: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    """
    Filter discovered flows to only those specified in deploy config.
    Build a flow-name mapping keyed by `flow_<name>` from discovered flows,
    then resolve deploy entries by `flow_<key>` or `flow_<alias>`.
    Returns mapping keyed by deploy flow_<key> to discovered flow info.
    """
    filtered: dict[str, dict[str, Any]] = {}
    flow_name_map = {f"flow_{name}": flow_info for name, flow_info in all_flows.items()}
    
    for key, deploy_meta in deploy_cfg.get("flows", {}).items():
        requested_name = f"flow_{key}"  # flow function name: 
                                             # either flow_<key> or flow_<alias>
        alias_name: str | None = None
        if isinstance(deploy_meta, dict):
            alias = deploy_meta.get("flow_alias")
            if isinstance(alias, str) and alias:
                alias_name = f"flow_{alias}"

        matched_name = requested_name
        if matched_name not in flow_name_map and alias_name is not None:
            matched_name = alias_name

        if matched_name not in flow_name_map:
            available = ", ".join(sorted(flow_name_map)) or "<none>"
            raise KeyError(
                f"Flow '{key}' not found in discovered flows "
                f"(checked {requested_name!r}"
                f"{f' and {alias_name!r}' if alias_name else ''}). "
                f"Available flows: {available}"
            )
        filtered[key] = flow_name_map[matched_name]
    
    return filtered


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

    curr_time_offset = (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.datetime.fromisoformat(flow_start_time).astimezone(datetime.timezone.utc)
    )
    return curr_time_offset.total_seconds()


def build_cron(interval: int | None, cron_offset: int = 0) -> str | None:
    if interval is None:
        return None
    if cron_offset > 0:
        return f"{cron_offset}-59/{interval} * * * *"
    return f"*/{interval} * * * *"


def build_triggers(trigger_items: list[dict[str, Any]]) -> list[Any]:
    return [
        DeploymentEventTrigger(
            expect={item["expect"]},
            match_related={
                "prefect.resource.name": item["resource_name"],
            },
        )
        for item in trigger_items
    ]


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
            raise ValueError(
                f"deploy_cfg.flows.{flow_key}.triggers entries must be mappings"
            )

        expect = trigger_item.get("expect")
        resource_name = trigger_item.get("resource_name")
        if not isinstance(expect, str) or not expect.strip():
            raise ValueError(
                f"deploy_cfg.flows.{flow_key}.triggers entries must define a non-empty 'expect'"
            )
        if not isinstance(resource_name, str) or not resource_name.strip():
            raise ValueError(
                f"deploy_cfg.flows.{flow_key}.triggers entries must define a non-empty 'resource_name'"
            )

        validated_triggers.append(
            {
                "expect": expect.strip(),
                "resource_name": resource_name.strip(),
            }
        )

    return validated_triggers


def validate_emit_events(
    emit_events: Any,
    *,
    flow_key: str,
) -> list[str] | None:
    """
    Validate emitted event names from deploy config.

    When configured, emit_events must be a non-empty list of non-empty strings.
    """
    emit_events = validate_optional_non_empty_list(
        emit_events,
        field_name=f"deploy_cfg.flows.{flow_key}.emit_events",
        item_label="event name",
    )
    if emit_events is None:
        return None

    validated_events: list[str] = []
    for event_name in emit_events:
        if not isinstance(event_name, str):
            raise ValueError(
                f"deploy_cfg.flows.{flow_key}.emit_events entries must be strings"
            )
        event_name = event_name.strip()
        if not event_name:
            raise ValueError(
                f"deploy_cfg.flows.{flow_key}.emit_events entries must be non-empty"
            )
        validated_events.append(event_name)

    return validated_events


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
        errors.append(
            f"In config but missing from deploy: {sorted(missing_from_deploy)}"
        )
    if missing_from_config:
        errors.append(
            f"In deploy but missing from config: {sorted(missing_from_config)}"
        )
    if errors:
        raise ValueError("Flow coverage mismatch. " + " | ".join(errors))


def build_deploy_specs(
    *,
    deploy_cfg: dict[str, Any],
    filtered_flows: dict[str, dict[str, Any]],
) -> list[DeploymentSpec]:
    """
    Build deployment specs from deploy config and pre-filtered flows mapping.
    """
    specs: list[DeploymentSpec] = []

    for key, deploy_meta in deploy_cfg.get("flows", {}).items():
        if not isinstance(deploy_meta, dict):
            continue

        if key not in filtered_flows:
            continue

        # Disallow specifying entrypoint in deploy config
        if "entrypoint" in deploy_meta:
            raise ValueError(
                f"deploy_cfg.flows.{key}.entrypoint is not supported; "
                "entrypoint is discovered from the flow module"
            )

        # Raise error if both triggers and cron are specified in deploy config
        if (deploy_meta.get("triggers") is None) == (deploy_meta.get("interval") is None):
            raise ValueError(
                f"deploy_cfg.flows.{key} must define exactly one of 'triggers' or 'interval'"
            )

        # Validate triggers and emit_events if provided
        flow_info = filtered_flows[key]
        triggers = validate_triggers(
            deploy_meta.get("triggers"),
            flow_key=key,
        )
        emit_events = validate_emit_events(
            deploy_meta.get("emit_events"),
            flow_key=key,
        )

        specs.append(
            DeploymentSpec(
                flow_key=key,
                deployment_name=deploy_meta.get("deployment_name", key),
                flow_module=flow_info["flow_module"],
                flow_name=flow_info["flow_function_name"],
                flow_obj=flow_info["flow_obj"],
                flow_alias=deploy_meta.get("flow_alias"),
                cron_offset=deploy_meta.get("cron_offset", 0),
                work_pool_name=deploy_meta.get("work_pool_name"),
                triggers=triggers,
                emit_events=emit_events,
            )
        )

    return specs


def create_deployments(
    *,
    specs: list[DeploymentSpec],
    param_cfg: dict[str, Any],
    deploy_cfg: dict[str, Any],
    source: Any,
    default_work_pool_name: str,
) -> tuple[list[RunnerDeployment], list[RunnerDeployment]]:
    flows_params = param_cfg["flows"]
    flows_deploy_settings = deploy_cfg["flows"]
    time_offset_targets = get_time_offset_targets(deploy_cfg)
    time_offset_seconds = _compute_time_offset_seconds(deploy_cfg.get("flow_start_time"))

    grouped: list[RunnerDeployment] = []
    standalone: list[RunnerDeployment] = []

    for spec in specs:
        if spec.flow_obj is None:
            raise ValueError(f"Deployment spec '{spec.deployment_name}' has no resolved flow")
        flow_obj = spec.flow_obj
        deployment_kwargs: dict[str, Any] = {
            "name": spec.deployment_name,
            "parameters": dict(flows_params[spec.flow_key]),
        }

        # Inject time_offset_seconds if this flow is marked for it
        if spec.flow_key in time_offset_targets:
            deployment_kwargs["parameters"]["time_offset_seconds"] = time_offset_seconds

        # Always route through the generic deployment wrapper so deployment-level
        # behavior can be configured centrally without changing flow functions
        deployment_kwargs["parameters"]["flow_module"] = spec.flow_module
        deployment_kwargs["parameters"]["flow_name"] = spec.flow_name
        if spec.emit_events is not None:
            deployment_kwargs["parameters"]["emit_events"] = spec.emit_events

        # Build triggers or cron schedule for the deployment
        if spec.triggers is not None:
            deployment_kwargs["triggers"] = build_triggers(spec.triggers)
        else:
            interval = flows_deploy_settings[spec.flow_key].get("interval")
            cron = build_cron(interval, spec.cron_offset)
            if cron is not None:
                deployment_kwargs["cron"] = cron

        # Add work_pool_name if specified and different from default
        has_non_default_work_pool = (
            spec.work_pool_name is not None and spec.work_pool_name != default_work_pool_name
        )
        if has_non_default_work_pool:
            deployment_kwargs["work_pool_name"] = spec.work_pool_name

        deployment = (
            flow_obj.from_source(
                source=source,
                entrypoint=DEFAULT_DEPLOYMENT_WRAPPER_ENTRYPOINT,
            ).to_deployment(**deployment_kwargs)
        )

        if has_non_default_work_pool:
            standalone.append(deployment)
        else:
            grouped.append(deployment)

    return grouped, standalone
