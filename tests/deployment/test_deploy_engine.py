import importlib
import types
from pathlib import Path
from types import SimpleNamespace

import pytest


def test_validate_flow_coverage(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    param_cfg = {"flows": {"flow_a": {}, "flow_b": {}}}
    deploy_cfg = {"flows": {"flow_a": {}, "flow_b": {}}}

    # Exact match — should not raise
    engine.validate_flow_coverage(param_cfg, deploy_cfg)

    # flow_b missing from deploy — should raise
    deploy_cfg_missing = {"flows": {"flow_a": {}}}
    with pytest.raises(ValueError, match="flow_b"):
        engine.validate_flow_coverage(param_cfg, deploy_cfg_missing)

    # flow_c in deploy but missing from config — should raise
    deploy_cfg_extra = {"flows": {"flow_a": {}, "flow_b": {}, "flow_c": {}}}
    with pytest.raises(ValueError, match="flow_c"):
        engine.validate_flow_coverage(param_cfg, deploy_cfg_extra)


def test_filter_flows_for_deploy_uses_flow_alias_fallback(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    all_flows = {
        "copy_raw": {
            "flow_obj": object(),
            "module_name": "flows_helper",
            "entrypoint": "echodataflow/flows/flows_helper.py:flow_copy_raw",
        },
        "file_upload": {
            "flow_obj": object(),
            "module_name": "flows_helper",
            "entrypoint": "echodataflow/flows/flows_helper.py:flow_file_upload",
        },
    }
    deploy_cfg = {
        "flows": {
            "copy_raw": {"module": "flows_helper"},
            "file_upload_acoustics": {
                "module": "flows_helper",
                "flow_alias": "file_upload",
            },
        }
    }

    filtered = engine.filter_flows_for_deploy(all_flows, deploy_cfg)

    assert set(filtered) == {"copy_raw", "file_upload_acoustics"}
    assert filtered["copy_raw"] is all_flows["copy_raw"]
    assert filtered["file_upload_acoustics"] is all_flows["file_upload"]


def test_filter_flows_for_deploy_raises_when_key_and_alias_missing(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    all_flows = {
        "copy_raw": {
            "flow_obj": object(),
            "module_name": "flows_helper",
            "entrypoint": "echodataflow/flows/flows_helper.py:flow_copy_raw",
        }
    }
    deploy_cfg = {
        "flows": {
            "file_upload_acoustics": {
                "module": "flows_helper",
                "flow_alias": "file_upload",
            }
        }
    }

    with pytest.raises(KeyError, match="file_upload_acoustics"):
        engine.filter_flows_for_deploy(all_flows, deploy_cfg)


def test_discover_all_flows_does_not_import_unfiltered_modules(
    monkeypatch,
    tmp_path,
    install_prefect_stubs,
):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    flows_dir = tmp_path / "echodataflow" / "flows"
    flows_dir.mkdir(parents=True)
    init_path = flows_dir / "__init__.py"
    init_path.write_text("", encoding="utf-8")
    (flows_dir / "good_module.py").write_text(
        "def flow_good():\n    return 'ok'\n",
        encoding="utf-8",
    )
    (flows_dir / "broken_module.py").write_text(
        "raise RuntimeError('should not import during discovery')\n\n"
        "def flow_broken():\n    return 'broken'\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        engine.importlib.util,
        "find_spec",
        lambda name: SimpleNamespace(origin=str(init_path)) if name == "echodataflow.flows" else None,
    )

    imported = []

    def fake_import_module(name):
        imported.append(name)
        if name == "echodataflow.flows.good_module":
            return types.SimpleNamespace(flow_good=object())
        if name == "echodataflow.flows.broken_module":
            raise AssertionError("broken module should not be imported")
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(engine.importlib, "import_module", fake_import_module)

    all_flows = engine.discover_all_flows()

    assert set(all_flows) == {"good", "broken"}
    assert imported == []

    filtered = engine.filter_flows_for_deploy(
        all_flows,
        {"flows": {"good": {"module": "good_module"}}},
    )
    engine.import_selected_flows(filtered)

    assert imported == ["echodataflow.flows.good_module"]
    assert "flow_obj" in filtered["good"]
    assert "flow_module" in filtered["good"]


def test_local_deploy_specs_generate_current_flow_entrypoints(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    repo_root = Path(__file__).resolve().parents[2]
    deploy_ship = engine.load_config(repo_root / "recipe" / "deploy" / "deploy_ship.yaml")
    deploy_cloud = engine.load_config(repo_root / "recipe" / "deploy" / "deploy_cloud.yaml")

    # Build filtered flows mappings with mock flow objects
    ship_flows = {}
    for flow_key, flow_meta in deploy_ship["flows"].items():
        module_name = flow_meta["module"]
        flow_alias = flow_meta.get("flow_alias") or flow_key
        entrypoint = f"echodataflow/flows/{module_name}.py:flow_{flow_alias}"
        ship_flows[flow_key] = {
            "flow_obj": object(),
            "module_name": module_name,
            "entrypoint": entrypoint,
        }

    cloud_flows = {}
    for flow_key, flow_meta in deploy_cloud["flows"].items():
        module_name = flow_meta["module"]
        flow_alias = flow_meta.get("flow_alias") or flow_key
        entrypoint = f"echodataflow/flows/{module_name}.py:flow_{flow_alias}"
        cloud_flows[flow_key] = {
            "flow_obj": object(),
            "module_name": module_name,
            "entrypoint": entrypoint,
        }

    ship_specs = engine.build_deploy_specs(
        deploy_cfg=deploy_ship,
        filtered_flows=ship_flows,
    )
    cloud_specs = engine.build_deploy_specs(
        deploy_cfg=deploy_cloud,
        filtered_flows=cloud_flows,
    )

    ship_entrypoints = {spec.flow_key: spec.entrypoint for spec in ship_specs}
    cloud_entrypoints = {spec.flow_key: spec.entrypoint for spec in cloud_specs}

    assert ship_entrypoints == {
        "copy_raw": "echodataflow/flows/flows_helper.py:flow_copy_raw",
        "raw2Sv": "echodataflow/flows/flows_acoustics.py:flow_raw2Sv",
        "create_MVBS": "echodataflow/flows/flows_acoustics.py:flow_create_MVBS",
        "predict_hake": "echodataflow/flows/flows_acoustics.py:flow_predict_hake",
        "file_upload_acoustics": "echodataflow/flows/flows_helper.py:flow_file_upload",
        "file_upload_trawl": "echodataflow/flows/flows_helper.py:flow_file_upload",
    }
    assert cloud_entrypoints == {
        "ingest_haul": "echodataflow/flows/flows_biology.py:flow_ingest_haul",
        "ingest_NASC": "echodataflow/flows/flows_integration.py:flow_ingest_NASC",
        "update_grid": "echodataflow/flows/flows_integration.py:flow_update_grid",
        "update_cache_MVBS": "echodataflow/flows/flows_viz_cloud.py:flow_update_cache_MVBS",
    }
