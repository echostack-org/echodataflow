"""Shared deployment helpers for rewrite cloud and ship entrypoints."""

from __future__ import annotations

import ast
import datetime
import importlib
import importlib.util
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, cast

from prefect.deployments.runner import RunnerDeployment
from prefect.events import DeploymentEventTrigger
from prefect.flows import Flow
from prefect.variables import Variable
from yaml import safe_load

from echodataflow.deployment.core import DEFAULT_ENTRYPOINT_ROOT


@dataclass(frozen=True)
class DeploymentSpec:
    flow_key: str
    deployment_name: str
    entrypoint: str
    flow_obj: Flow[..., Any] | None = None
    flow_alias: str | None = None
    cron_offset: int = 0
    apply_separately: bool = False
    work_pool_name: str | None = None
    triggers: list[dict[str, Any]] | None = None


def discover_all_flows() -> dict[str, dict[str, Any]]:
    """
    Discover all flow_* function names from all modules in echodataflow.flows.
    This avoids importing every flow module before the deploy config has filtered
    out unrelated flows.

    Returns mapping: flow_name -> {"module_name", "flow_attr", "entrypoint"}
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
        module_ast = ast.parse(py_file.read_text(), filename=str(py_file))

        for node in module_ast.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue

            attr_name = node.name
            if not attr_name.startswith("flow_"):
                continue

            flow_name = attr_name.removeprefix("flow_")
            entrypoint = f"{DEFAULT_ENTRYPOINT_ROOT}/{module_name}.py:{attr_name}"

            discovered[flow_name] = {
                "module_name": module_name,
                "flow_attr": attr_name,
                "entrypoint": entrypoint,
            }

    return discovered


def import_selected_flows(filtered_flows: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Import only the modules needed by the already-filtered flow set."""
    imported_modules: dict[str, ModuleType] = {}

    for flow_info in filtered_flows.values():
        if "flow_obj" in flow_info and "flow_module" in flow_info:
            continue

        module_name = cast(str, flow_info["module_name"])
        attr_name = cast(str, flow_info["flow_attr"])

        if module_name not in imported_modules:
            try:
                imported_modules[module_name] = importlib.import_module(
                    f"echodataflow.flows.{module_name}"
                )
            except ImportError as e:
                raise ImportError(
                    f"Failed to import echodataflow.flows.{module_name}: {e}"
                ) from e

        flow_module = imported_modules[module_name]
        flow_info["flow_module"] = flow_module
        flow_info["flow_obj"] = cast(Flow[..., Any], getattr(flow_module, attr_name))

    return filtered_flows


def filter_flows_for_deploy(all_flows: dict[str, dict[str, Any]], deploy_cfg: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """
    Filter discovered flows to only those specified in deploy config.
    Build a flow-name mapping keyed by `flow_<name>` from discovered flows,
    then resolve deploy entries by `flow_<flow_key>` or `flow_<flow_alias>`.
    Returns mapping keyed by deploy flow_key to discovered flow metadata.
    """
    filtered: dict[str, dict[str, Any]] = {}
    flow_name_map = {f"flow_{name}": flow_info for name, flow_info in all_flows.items()}
    
    for flow_key, deploy_meta in deploy_cfg.get("flows", {}).items():
        requested_name = f"flow_{flow_key}"
        alias_name: str | None = None
        if isinstance(deploy_meta, dict):
            flow_alias = deploy_meta.get("flow_alias")
            if isinstance(flow_alias, str) and flow_alias:
                alias_name = f"flow_{flow_alias}"

        matched_name = requested_name
        if matched_name not in flow_name_map and alias_name is not None:
            matched_name = alias_name

        if matched_name not in flow_name_map:
            available = ", ".join(sorted(flow_name_map)) or "<none>"
            raise KeyError(
                f"Flow '{flow_key}' not found in discovered flows "
                f"(checked {requested_name!r}"
                f"{f' and {alias_name!r}' if alias_name else ''}). "
                f"Available flows: {available}"
            )
        filtered[flow_key] = flow_name_map[matched_name]
    
    return filtered


def load_config(config_path: Path) -> dict[str, Any]:
    with open(config_path, "r") as file:
        return safe_load(file)


def _default_local_source_root() -> Path:
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
    source_mode_override: str | None = None,
    log_context: str | None = None,
) -> Any:
    """
    Resolve deployment source based on deploy config and environment variable override.
    """
    source_cfg = deploy_cfg.get("source", {})
    if source_cfg is None:
        source_cfg = {}

    # Priority: 1) env var override, 2) deploy config setting, 3) default to local
    mode = (source_mode_override or source_cfg.get("mode") or "local").lower()

    # Capture the origin of source mode
    if source_mode_override:
        source_mode_origin = "env:PREFECT_SOURCE_MODE"
    elif source_cfg.get("mode"):
        source_mode_origin = "deploy_cfg.source.mode"
    else:
        source_mode_origin = "default:local"

    if mode == "local":
        default_local_dir = _validate_local_source_layout(_default_local_source_root())
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


def set_prefect_variables(
    deploy_cfg: dict[str, Any],
    param_cfg: dict[str, Any],
) -> None:
    """Set Prefect Variables from deploy and param specifications."""
    Variable.set("flow_start_time", deploy_cfg.get("flow_start_time"), overwrite=True)
    
    init_dict = param_cfg.get("init", {})
    Variable.set("counter_raw_copy", init_dict.get("counter_raw_copy"), overwrite=True)


def build_cron(interval: int | None, cron_offset: int = 0) -> str | None:
    if interval is None:
        return None
    if cron_offset > 0:
        return f"{cron_offset}-59/{interval} * * * *"
    return f"*/{interval} * * * *"


# TODO: decide if want to keep this
def sanitize_parameters(flow_cfg: dict[str, Any]) -> dict[str, Any]:
    return dict(flow_cfg)


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

    for flow_key, deploy_meta in deploy_cfg.get("flows", {}).items():
        if not isinstance(deploy_meta, dict):
            continue

        if flow_key not in filtered_flows:
            continue

        flow_info = filtered_flows[flow_key]
        entrypoint = deploy_meta.get("entrypoint") or flow_info["entrypoint"]

        specs.append(
            DeploymentSpec(
                flow_key=flow_key,
                deployment_name=deploy_meta.get("deployment_name", flow_key),
                entrypoint=entrypoint,
                flow_obj=flow_info["flow_obj"],
                flow_alias=deploy_meta.get("flow_alias"),
                cron_offset=deploy_meta.get("cron_offset", 0),
                apply_separately=deploy_meta.get("apply_separately", False),
                work_pool_name=deploy_meta.get("work_pool_name"),
                triggers=deploy_meta.get("triggers"),
            )
        )

    return specs


def create_deployments(
    *,
    specs: list[DeploymentSpec],
    param_cfg: dict[str, Any],
    deploy_cfg: dict[str, Any],
    source: Any,
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
            "parameters": sanitize_parameters(flows_params[spec.flow_key]),
        }

        # Inject time_offset_seconds if this flow is marked for it
        if spec.flow_key in time_offset_targets:
            deployment_kwargs["parameters"]["time_offset_seconds"] = time_offset_seconds

        if spec.triggers is not None:
            deployment_kwargs["triggers"] = build_triggers(spec.triggers)
        else:
            interval = flows_deploy_settings[spec.flow_key].get("interval")
            cron = build_cron(interval, spec.cron_offset)
            if cron is not None:
                deployment_kwargs["cron"] = cron

        if spec.work_pool_name is not None:
            deployment_kwargs["work_pool_name"] = spec.work_pool_name

        deployment = (
            flow_obj.from_source(
                source=source,
                entrypoint=spec.entrypoint,
            ).to_deployment(**deployment_kwargs)
        )

        if spec.apply_separately or spec.work_pool_name is not None:
            standalone.append(deployment)
        else:
            grouped.append(deployment)

    return grouped, standalone
