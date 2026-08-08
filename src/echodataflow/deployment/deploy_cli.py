"""CLI entrypoint for rewrite deployments from paired YAML specifications."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any

from prefect import deploy
from prefect.variables import Variable


from echodataflow.deployment.deployment_engine import (
    discover_all_flows,
    filter_flows_for_deploy,
    build_deploy_specs,
    create_deployments,
    load_config,
    resolve_deployment_source,
    validate_flow_coverage,
)


def _run_concurrency_setup(deploy_cfg: dict[str, Any]) -> None:
    """Create Prefect concurrency limits declared on individual flow configs."""
    concurrency_by_tag: dict[str, int] = {}

    for flow_key, deploy_meta in deploy_cfg.get("flows", {}).items():
        if not isinstance(deploy_meta, dict):
            continue

        concurrency_limit = deploy_meta.get("concurrency_limit")
        if concurrency_limit is None:
            continue
        if not isinstance(concurrency_limit, int) or concurrency_limit < 1:
            raise ValueError(
                f"deploy_cfg.flows.{flow_key}.concurrency_limit must be an integer >= 1"
            )

        concurrency_tag = deploy_meta.get("concurrency_tag", flow_key)
        if not isinstance(concurrency_tag, str) or not concurrency_tag.strip():
            raise ValueError(
                f"deploy_cfg.flows.{flow_key}.concurrency_tag must be a non-empty string"
            )
        concurrency_tag = concurrency_tag.strip()

        existing_limit = concurrency_by_tag.get(concurrency_tag)
        if existing_limit is not None and existing_limit != concurrency_limit:
            raise ValueError(
                f"Conflicting concurrency_limit for tag {concurrency_tag!r}: "
                f"{existing_limit} != {concurrency_limit}"
            )
        concurrency_by_tag[concurrency_tag] = concurrency_limit

    if not concurrency_by_tag:
        return

    from prefect import get_client
    from prefect.exceptions import ObjectAlreadyExists, ObjectNotFound

    async def ensure_concurrency_limits() -> None:
        async with get_client() as client:
            for concurrency_tag, concurrency_limit in concurrency_by_tag.items():
                try:
                    await client.read_concurrency_limit_by_tag(concurrency_tag)
                except ObjectNotFound:
                    try:
                        await client.create_concurrency_limit(
                            tag=concurrency_tag,
                            concurrency_limit=concurrency_limit,
                        )
                    except ObjectAlreadyExists:
                        pass

    asyncio.run(ensure_concurrency_limits())


def _run_from_specs(
    *,
    param_cfg_path: Path,
    deploy_cfg_path: Path,
    run_concurrency_setup: bool,
    default_work_pool_name: str = "local",
) -> None:
    # Load configs
    param_cfg = load_config(param_cfg_path)
    deploy_cfg = load_config(deploy_cfg_path)

    # Validate the pair of configs contain the same flows
    validate_flow_coverage(param_cfg, deploy_cfg)

    # Set "flow_start_time" as a Prefect variable
    Variable.set("flow_start_time", deploy_cfg.get("flow_start_time"), overwrite=True)

    # Discover all flows and filter to those in deploy config
    all_flows = discover_all_flows()
    filtered_flows = filter_flows_for_deploy(all_flows, deploy_cfg)
    if run_concurrency_setup:
        _run_concurrency_setup(deploy_cfg)

    # Set up deployment source: git or local
    source = resolve_deployment_source(
        deploy_cfg=deploy_cfg,
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

    if args.target == "run":
        _run_from_specs(
            param_cfg_path=args.param_config,
            deploy_cfg_path=args.deploy_spec,
            run_concurrency_setup=args.use_concurrency,
            default_work_pool_name=args.default_work_pool_name,
        )
        return

    raise ValueError(f"Unsupported target: {args.target}")


if __name__ == "__main__":
    main()
