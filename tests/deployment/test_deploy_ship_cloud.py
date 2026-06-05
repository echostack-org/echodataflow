import importlib.util
import sys
import types
from pathlib import Path

class FakeFlowSource:
    def __init__(self, flow_name, sink):
        self.flow_name = flow_name
        self.sink = sink

    def to_deployment(self, **kwargs):
        dep = {"flow_name": self.flow_name, **kwargs}
        self.sink["deployments"].append(dep)
        return FakeDeployment(dep, self.sink)


class FakeDeployment:
    def __init__(self, data, sink):
        self.data = data
        self.sink = sink

    def apply(self):
        self.sink["applied"].append(self.data)


class FakeFlow:
    def __init__(self, flow_name, sink):
        self.flow_name = flow_name
        self.sink = sink

    def from_source(self, source, entrypoint):
        self.sink["from_source"].append(
            {"flow_name": self.flow_name, "source": source, "entrypoint": entrypoint}
        )
        return FakeFlowSource(self.flow_name, self.sink)


def import_module_from_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


REPO_ROOT = Path(__file__).resolve().parents[2]


def clone_config(config):
    return {
        key: (value.copy() if isinstance(value, dict) else value)
        for key, value in config.items()
    }


def test_deploy_cli_cloud_characterization(monkeypatch, tmp_path, install_prefect_stubs):
    sink = {"from_source": [], "deployments": [], "applied": []}
    stubs = install_prefect_stubs(sink=sink)
    (tmp_path / "echodataflow" / "flows").mkdir(parents=True)
    monkeypatch.setitem(sys.modules, "pandas", types.ModuleType("pandas"))
    monkeypatch.setitem(sys.modules, "s3fs", types.ModuleType("s3fs"))

    flows_biology = types.ModuleType("flows_biology")
    flows_biology.flow_ingest_haul = FakeFlow("flow_ingest_haul", sink)
    flows_integration = types.ModuleType("flows_integration")
    flows_integration.flow_ingest_NASC = FakeFlow("flow_ingest_NASC", sink)
    flows_integration.flow_update_grid = FakeFlow("flow_update_grid", sink)
    flows_viz_cloud = types.ModuleType("flows_viz_cloud")
    flows_viz_cloud.flow_update_cache_MVBS = FakeFlow("flow_update_cache_MVBS", sink)

    monkeypatch.setitem(sys.modules, "flows_biology", flows_biology)
    monkeypatch.setitem(sys.modules, "flows_integration", flows_integration)
    monkeypatch.setitem(sys.modules, "flows_viz_cloud", flows_viz_cloud)
    monkeypatch.setitem(sys.modules, "echodataflow.flows.flows_biology", flows_biology)
    monkeypatch.setitem(sys.modules, "echodataflow.flows.flows_integration", flows_integration)
    monkeypatch.setitem(sys.modules, "echodataflow.flows.flows_viz_cloud", flows_viz_cloud)

    param_cfg = {
        "init": {"counter_raw_copy": 0},
        "flows": {
            "ingest_haul": {"x": 1},
            "ingest_NASC": {"y": 2},
            "update_grid": {"z": 3},
            "update_cache_MVBS": {"w": 4},
        },
    }
    deploy_cfg = {
        "flow_start_time": None,
        "default_work_pool_name": "local",
        "flows": {
            "ingest_haul": {
                "module": "flows_biology",
                "deployment_name": "ingest_haul",
                "interval": 5,
            },
            "ingest_NASC": {
                "module": "flows_integration",
                "deployment_name": "ingest_NASC",
                "interval": 7,
            },
            "update_grid": {
                "module": "flows_integration",
                "deployment_name": "update_grid",
                "triggers": [
                    {"expect": "haul.ingested", "resource_name": "ingest_haul"},
                    {"expect": "nasc.ingested", "resource_name": "ingest_NASC"},
                ],
            },
            "update_cache_MVBS": {
                "module": "flows_viz_cloud",
                "deployment_name": "update_cache_MVBS",
                "interval": 10,
                "cron_offset": 3,
                "inject_time_offset": True,
            },
        },
    }

    module = import_module_from_path(
        "deploy_cli_characterization_mod",
        REPO_ROOT / "src" / "echodataflow" / "deployment" / "deploy_cli.py",
    )

    def fake_load_config(path):
        path_str = str(path)
        if "config_" in path_str:
            return clone_config(param_cfg)
        return clone_config(deploy_cfg)

    monkeypatch.setattr(module, "load_config", fake_load_config)
    monkeypatch.setattr(module, "resolve_deployment_source", lambda **_kwargs: "local-source")

    # Create the filtered flows mapping
    cloud_deploy_cfg = clone_config(deploy_cfg)
    filtered = {}
    for flow_key in cloud_deploy_cfg["flows"].keys():
        module_name = cloud_deploy_cfg["flows"][flow_key]["module"]
        flow_alias = cloud_deploy_cfg["flows"][flow_key].get("flow_alias") or flow_key
        flow_name = f"flow_{flow_alias}"
        flow_module = sys.modules[f"echodataflow.flows.{module_name}"]
        flow_obj = getattr(flow_module, flow_name)
        entrypoint = f"echodataflow/flows/{module_name}.py:{flow_name}"
        filtered[flow_key] = {
            "flow_obj": flow_obj,
            "module_name": module_name,
            "flow_module": flow_module,
            "entrypoint": entrypoint,
        }

    # Mock the discovery functions in the deploy_cli module
    monkeypatch.setattr(module, "discover_all_flows", lambda: filtered)
    monkeypatch.setattr(module, "filter_flows_for_deploy", lambda all_flows, cfg: {k: filtered[k] for k in cfg["flows"].keys()})

    stubs["FakeVariable"].calls = []
    module._run_from_specs(
        param_cfg_path=Path("config_cloud.yaml"),
        deploy_cfg_path=Path("deploy_cloud.yaml"),
        source_mode="local",
        run_concurrency_setup=False,
        default_work_pool_name="local",
    )

    expected_entrypoints = {
        "ingest_haul": "echodataflow/flows/flows_biology.py:flow_ingest_haul",
        "ingest_NASC": "echodataflow/flows/flows_integration.py:flow_ingest_NASC",
        "update_grid": "echodataflow/flows/flows_integration.py:flow_update_grid",
        "update_cache_MVBS": "echodataflow/flows/flows_viz_cloud.py:flow_update_cache_MVBS",
    }
    actual_entrypoints = {
        item["flow_name"].removeprefix("flow_"): item["entrypoint"]
        for item in sink["from_source"]
    }
    assert actual_entrypoints == expected_entrypoints

    assert sink["deploy_call"]["kwargs"]["work_pool_name"] == "local"
    assert len(sink["deployments"]) == 4
    names = {d["name"] for d in sink["deployments"]}
    assert names == {"ingest_haul", "ingest_NASC", "update_grid", "update_cache_MVBS"}

    update_cache = next(d for d in sink["deployments"] if d["name"] == "update_cache_MVBS")
    assert update_cache["cron"] == "3-59/10 * * * *"
    assert update_cache["parameters"]["time_offset_seconds"] == 0

    update_grid = next(d for d in sink["deployments"] if d["name"] == "update_grid")
    assert len(update_grid["triggers"]) == 2

    ingest_haul = next(d for d in sink["deployments"] if d["name"] == "ingest_haul")
    ingest_nasc = next(d for d in sink["deployments"] if d["name"] == "ingest_NASC")
    assert ingest_haul["cron"] == "*/5 * * * *"
    assert ingest_nasc["cron"] == "*/7 * * * *"



def test_deploy_cli_ship_characterization(monkeypatch, tmp_path, install_prefect_stubs):
    sink = {"from_source": [], "deployments": [], "applied": []}
    stubs = install_prefect_stubs(sink=sink)
    (tmp_path / "echodataflow" / "flows").mkdir(parents=True)

    flows_acoustics = types.ModuleType("flows_acoustics")
    flows_acoustics.flow_raw2Sv = FakeFlow("flow_raw2Sv", sink)
    flows_acoustics.flow_create_MVBS = FakeFlow("flow_create_MVBS", sink)
    flows_acoustics.flow_predict_hake = FakeFlow("flow_predict_hake", sink)

    flows_helper_mod = types.ModuleType("flows_helper")
    flows_helper_mod.flow_file_upload = FakeFlow("flow_file_upload", sink)

    monkeypatch.setitem(sys.modules, "flows_acoustics", flows_acoustics)
    monkeypatch.setitem(sys.modules, "flows_helper", flows_helper_mod)
    monkeypatch.setitem(sys.modules, "echodataflow.flows.flows_acoustics", flows_acoustics)
    monkeypatch.setitem(sys.modules, "echodataflow.flows.flows_helper", flows_helper_mod)

    param_cfg = {
        "init": {"counter_raw_copy": 0},
        "flows": {
            "raw2Sv": {"a": 1},
            "create_MVBS": {"b": 2},
            "predict_hake": {"c": 3},
            "file_upload_acoustics": {"d": 4},
            "file_upload_trawl": {"e": 5},
        },
    }
    deploy_cfg = {
        "flow_start_time": None,
        "default_work_pool_name": "local",
        "flows": {
            "raw2Sv": {
                "module": "flows_acoustics",
                "deployment_name": "raw2Sv_leg2",
                "interval": 5,
            },
            "create_MVBS": {
                "module": "flows_acoustics",
                "deployment_name": "create-MVBS_leg2",
                "interval": 10,
                "cron_offset": 3,
                "inject_time_offset": True,
            },
            "predict_hake": {
                "module": "flows_acoustics",
                "deployment_name": "predict-hake_leg2",
                "interval": 20,
                "inject_time_offset": True,
            },
            "file_upload_acoustics": {
                "module": "flows_helper",
                "deployment_name": "file-upload-acoustics_leg2",
                "flow_alias": "file_upload",
                "interval": 10,
                "apply_separately": True,
                "work_pool_name": "local",
            },
            "file_upload_trawl": {
                "module": "flows_helper",
                "deployment_name": "file-upload-trawl_20250902",
                "flow_alias": "file_upload",
                "interval": 10,
                "apply_separately": True,
                "work_pool_name": "local",
            },
        },
    }

    module = import_module_from_path(
        "deploy_cli_characterization_mod_ship",
        REPO_ROOT / "src" / "echodataflow" / "deployment" / "deploy_cli.py",
    )

    def fake_load_config(path):
        path_str = str(path)
        if "config_" in path_str:
            return clone_config(param_cfg)
        return clone_config(deploy_cfg)

    monkeypatch.setattr(module, "load_config", fake_load_config)
    monkeypatch.setattr(module, "resolve_deployment_source", lambda **_kwargs: "local-source")

    # Create the filtered flows mapping
    ship_deploy_cfg = clone_config(deploy_cfg)
    filtered = {}
    for flow_key in ship_deploy_cfg["flows"].keys():
        module_name = ship_deploy_cfg["flows"][flow_key]["module"]
        flow_alias = ship_deploy_cfg["flows"][flow_key].get("flow_alias") or flow_key
        flow_name = f"flow_{flow_alias}"
        flow_module = sys.modules[f"echodataflow.flows.{module_name}"]
        flow_obj = getattr(flow_module, flow_name)
        entrypoint = f"echodataflow/flows/{module_name}.py:{flow_name}"
        filtered[flow_key] = {
            "flow_obj": flow_obj,
            "module_name": module_name,
            "flow_module": flow_module,
            "entrypoint": entrypoint,
        }

    # Mock the discovery functions in the deploy_cli module
    monkeypatch.setattr(module, "discover_all_flows", lambda: filtered)
    monkeypatch.setattr(module, "filter_flows_for_deploy", lambda all_flows, cfg: {k: filtered[k] for k in cfg["flows"].keys()})

    stubs["FakeVariable"].calls = []
    module._run_from_specs(
        param_cfg_path=Path("config_ship.yaml"),
        deploy_cfg_path=Path("deploy_ship.yaml"),
        source_mode="local",
        run_concurrency_setup=False,
        default_work_pool_name="local",
    )

    expected_entrypoints = {
        "raw2Sv": "echodataflow/flows/flows_acoustics.py:flow_raw2Sv",
        "create_MVBS": "echodataflow/flows/flows_acoustics.py:flow_create_MVBS",
        "predict_hake": "echodataflow/flows/flows_acoustics.py:flow_predict_hake",
        "file_upload": "echodataflow/flows/flows_helper.py:flow_file_upload",
    }
    actual_entrypoints = {
        item["flow_name"].removeprefix("flow_"): item["entrypoint"]
        for item in sink["from_source"]
    }
    assert actual_entrypoints == expected_entrypoints

    assert sink["deploy_call"]["kwargs"]["work_pool_name"] == "local"
    assert len(sink["deployments"]) == 5
    assert len(sink["applied"]) == 2
