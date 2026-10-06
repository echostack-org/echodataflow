import importlib.util
import sys
from pathlib import Path

import pytest


def import_module_from_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_deploy_cli_module(install_prefect_stubs):
    install_prefect_stubs()
    module_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "echodataflow"
        / "deployment"
        / "deploy_cli.py"
    )
    return import_module_from_path("deploy_cli_test_mod", module_path)


def test_build_parser_run(install_prefect_stubs):
    module = _load_deploy_cli_module(install_prefect_stubs=install_prefect_stubs)

    parser = module._build_parser()
    args = parser.parse_args(
        [
            "run",
            "--default-work-pool-name",
            "local",
            "--param-config",
            "config_ship.yaml",
            "--deploy-spec",
            "deploy_ship.yaml",
        ]
    )

    assert args.target == "run"
    assert args.param_config == Path("config_ship.yaml")
    assert args.deploy_spec == Path("deploy_ship.yaml")


@pytest.mark.parametrize(
    ("flow_start_time", "expected_output"),
    [
        (
            "2025-06-19T00:30:00+00:00",
            "Time travel mode: flow start time is 2025-06-19T00:30:00+00:00\n",
        ),
        (None, ""),
    ],
)
def test_run_from_specs_prints_time_travel_mode_only_when_configured(
    monkeypatch,
    install_prefect_stubs,
    capsys,
    flow_start_time,
    expected_output,
):
    module = _load_deploy_cli_module(install_prefect_stubs=install_prefect_stubs)
    param_path = Path("params.yaml")
    deploy_path = Path("deploy.yaml")
    configs = {
        param_path: {"flows": {}},
        deploy_path: {"flows": {}, "flow_start_time": flow_start_time},
    }

    monkeypatch.setattr(module, "load_config", configs.__getitem__)
    monkeypatch.setattr(module, "resolve_registered_flows", lambda _config: {})
    monkeypatch.setattr(module, "resolve_deployment_source", lambda **_kwargs: "source")
    monkeypatch.setattr(module, "build_deploy_specs", lambda **_kwargs: [])
    monkeypatch.setattr(module, "configure_concurrency_groups", lambda **_kwargs: None)
    monkeypatch.setattr(module, "create_deployments", lambda **_kwargs: ([], []))

    module._run_from_specs(
        param_cfg_path=param_path,
        deploy_cfg_path=deploy_path,
    )

    assert capsys.readouterr().out == expected_output


def test_main_dispatches_run_args(monkeypatch, install_prefect_stubs):
    module = _load_deploy_cli_module(install_prefect_stubs=install_prefect_stubs)

    captured = {}

    def fake_run_from_specs(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(module, "_run_from_specs", fake_run_from_specs)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "deploy_cli",
            "run",
            "--default-work-pool-name",
            "local",
            "--param-config",
            "recipe/params/config_ship.yaml",
            "--deploy-spec",
            "recipe/deploy/deploy_ship.yaml",
        ],
    )

    module.main()

    assert captured["param_cfg_path"] == Path("recipe/params/config_ship.yaml")
    assert captured["deploy_cfg_path"] == Path("recipe/deploy/deploy_ship.yaml")
