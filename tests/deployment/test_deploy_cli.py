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
    module_path = Path(__file__).resolve().parents[2] / "src" / "echodataflow" / "deployment" / "deploy_cli.py"
    return import_module_from_path("deploy_cli_test_mod", module_path)


def test_build_parser_run_boolean_optional(install_prefect_stubs):
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
            "--no-use-concurrency",
        ]
    )

    assert args.target == "run"
    assert args.use_concurrency is False
    assert args.param_config == Path("config_ship.yaml")
    assert args.deploy_spec == Path("deploy_ship.yaml")


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
            "--no-use-concurrency",
        ],
    )

    module.main()

    assert captured["param_cfg_path"] == Path("recipe/params/config_ship.yaml")
    assert captured["deploy_cfg_path"] == Path("recipe/deploy/deploy_ship.yaml")
    assert captured["run_concurrency_setup"] is False


def test_run_concurrency_setup_rejects_conflicting_tag_limits(monkeypatch, install_prefect_stubs):
    module = _load_deploy_cli_module(install_prefect_stubs=install_prefect_stubs)

    deploy_cfg = {
        "flows": {
            "raw2Sv": {
                "concurrency_limit": 4,
                "concurrency_tag": "shared-tag",
            },
            "predict_hake": {
                "concurrency_limit": 2,
                "concurrency_tag": "shared-tag",
            },
        }
    }

    with pytest.raises(ValueError, match="Conflicting concurrency_limit"):
        module._run_concurrency_setup(deploy_cfg)
