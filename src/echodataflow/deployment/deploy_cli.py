"""CLI entrypoint for rewrite deployments from paired YAML specifications."""

from __future__ import annotations

import argparse
from pathlib import Path

from prefect import deploy

from echodataflow.deployment.deployment_engine import (
    build_deploy_specs,
    configure_concurrency_groups,
    create_deployments,
    load_config,
    resolve_registered_flows,
    resolve_deployment_source,
    validate_deploy_config,
    validate_flow_coverage,
)


def _run_from_specs(
    *,
    param_cfg_path: Path,
    deploy_cfg_path: Path,
    default_work_pool_name: str = "local",
) -> None:
    # Load configs
    param_cfg = load_config(param_cfg_path)
    deploy_cfg = load_config(deploy_cfg_path)

    # Validate the deployment schema and paired flow coverage.
    validate_deploy_config(deploy_cfg)
    validate_flow_coverage(param_cfg, deploy_cfg)
    if deploy_cfg.get("flow_start_time") is not None:
        print(f"Time travel mode: flow start time is {deploy_cfg['flow_start_time']}")

    # Validate registry keys and import only the flows requested by this recipe.
    resolved_flows = resolve_registered_flows(deploy_cfg)

    # Set up deployment source: git or local
    source = resolve_deployment_source(
        deploy_cfg=deploy_cfg,
        log_context="deploy_cli",
    )

    # Use deploy config default work pool name if specified,
    # unless specified for individual flow
    default_work_pool_name = deploy_cfg.get("default_work_pool_name", default_work_pool_name)

    specs = build_deploy_specs(
        param_cfg=param_cfg,
        deploy_cfg=deploy_cfg,
        resolved_flows=resolved_flows,
    )
    configure_concurrency_groups(
        specs=specs,
        concurrency_groups=deploy_cfg.get("concurrency_groups", {}),
        default_work_pool_name=default_work_pool_name,
    )
    grouped, standalone = create_deployments(
        specs=specs,
        source=source,
        default_work_pool_name=default_work_pool_name,
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
        "--default-work-pool-name",
        required=True,
        default="local",
        help="Default work pool name for deployments.",
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
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.target == "run":
        _run_from_specs(
            param_cfg_path=args.param_config,
            deploy_cfg_path=args.deploy_spec,
            default_work_pool_name=args.default_work_pool_name,
        )
        return

    raise ValueError(f"Unsupported target: {args.target}")


if __name__ == "__main__":
    main()
