"""CLI entrypoint for rewrite deployments from paired YAML specifications."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import os
from pathlib import Path
from typing import Any

from prefect import deploy
from yaml import safe_load


from echodataflow.deployment.deployment_engine import (
    discover_all_flows,
    filter_flows_for_deploy,
    build_deploy_specs,
    create_deployments,
    import_selected_flows,
    load_config,
    resolve_deployment_source,
    set_prefect_variables,
    validate_flow_coverage,
)


def _run_concurrency_setup(filtered_flows: dict[str, dict[str, Any]]) -> None:
    """Run set_concurrency_limit for modules with flows in filtered_flows."""
    seen_modules = set()
    
    for flow_info in filtered_flows.values():
        flow_module = flow_info["flow_module"]
        module_name = id(flow_module)  # use object id to track unique modules
        if module_name in seen_modules:
            continue
        seen_modules.add(module_name)
        
        setup_fn = getattr(flow_module, "set_concurrency_limit", None)
        if not callable(setup_fn):
            continue

        if inspect.iscoroutinefunction(setup_fn):
            asyncio.run(setup_fn())
        else:
            setup_fn()


def _run_from_specs(
    *,
    param_cfg_path: Path,
    deploy_cfg_path: Path,
    source_mode: str | None,
    run_concurrency_setup: bool,
    default_work_pool_name: str = "local",
) -> None:
    # Load configs
    param_cfg = load_config(param_cfg_path)
    deploy_cfg = load_config(deploy_cfg_path)

    # Validate the pair of configs contain the same flows
    validate_flow_coverage(param_cfg, deploy_cfg)

    # Set prefect variables
    set_prefect_variables(deploy_cfg, param_cfg)

    # Discover all flows and filter to those in deploy config
    all_flows = discover_all_flows()
    filtered_flows = filter_flows_for_deploy(all_flows, deploy_cfg)
    import_selected_flows(filtered_flows)
    if run_concurrency_setup:
        _run_concurrency_setup(filtered_flows)

    # Set up deployment source: git or local
    source = resolve_deployment_source(
        deploy_cfg=deploy_cfg,
        source_mode_override=source_mode,
        log_context="deploy_cli",
    )

    # Use deploy config default work pool name if specified,
    # unless specified for individual flow
    default_work_pool_name = deploy_cfg.get("default_work_pool_name", default_work_pool_name)

    specs = build_deploy_specs(
        deploy_cfg=deploy_cfg,
        filtered_flows=filtered_flows,
    )
    grouped, standalone = create_deployments(
        specs=specs,
        param_cfg=param_cfg,
        deploy_cfg=deploy_cfg,
        source=source,
    )

    deploy(*grouped, work_pool_name=default_work_pool_name)
    for deployment in standalone:
        deployment.apply()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="deploy_cli",
        description="Create deployments from paired param/deploy YAML specifications.",
    )
    subparsers = parser.add_subparsers(dest="target", required=True)

    run_parser = subparsers.add_parser(
        "run",
        help="Run deployments from explicit YAML file paths.",
    )
    run_parser.add_argument(
        "--source-mode",
        choices=("local", "git"),
        default=None,
        help=(
            "Temporarily override source selection for this run. "
            "Maps to PREFECT_SOURCE_MODE."
        ),
    )
    run_parser.add_argument(
        "--default-work-pool-name",
        required=True,
        default="local",
        help=(
            "Default work pool name for deployments."
        ),
    )
    run_parser.add_argument(
        "--param-config",
        required=True,
        type=Path,
        help="Path to config_*.yaml (parameter config).",
    )
    run_parser.add_argument(
        "--deploy-spec",
        required=True,
        type=Path,
        help="Path to deploy_*.yaml (deployment spec).",
    )
    run_parser.add_argument(
        "--use-concurrency",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run concurrency-limit setup before creating deployments (default: enabled).",
    )

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    source_mode = args.source_mode
    if source_mode is not None:
        os.environ["PREFECT_SOURCE_MODE"] = source_mode

    if args.target == "run":
        _run_from_specs(
            param_cfg_path=args.param_config,
            deploy_cfg_path=args.deploy_spec,
            source_mode=source_mode,
            run_concurrency_setup=args.use_concurrency,
            default_work_pool_name=args.default_work_pool_name,
        )
        return

    raise ValueError(f"Unsupported target: {args.target}")


if __name__ == "__main__":
    main()
