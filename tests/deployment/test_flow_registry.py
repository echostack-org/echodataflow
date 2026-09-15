import importlib
import sys
import types

import pytest


def test_resolve_registered_flows_loads_only_recipe_flows(
    monkeypatch,
    install_prefect_stubs,
):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")
    registry = importlib.import_module("echodataflow.deployment.flow_registry")

    fake_flow = object()
    fake_module = types.ModuleType("echodataflow.flows.example")
    fake_module.flow_example = fake_flow
    monkeypatch.setitem(sys.modules, "echodataflow.flows.example", fake_module)
    monkeypatch.setattr(
        registry,
        "FLOW_REGISTRY",
        {
            "example": registry.FlowRegistration(
                entrypoint="echodataflow/flows/example.py:flow_example",
            )
        },
    )

    assert engine.resolve_registered_flows(
        {"flows": {"recipe_name": {"flow": "example"}}}
    ) == {
        "recipe_name": {
            "entrypoint": "echodataflow/flows/example.py:flow_example",
            "flow_obj": fake_flow,
        }
    }


def test_flow_registry_contains_current_raw2sv_entrypoint():
    registry = importlib.import_module("echodataflow.deployment.flow_registry")

    registration = registry.FLOW_REGISTRY["raw2Sv"]

    assert registration.entrypoint == (
        "echodataflow/flows/flows_acoustics.py:flow_raw2Sv"
    )


def test_postprocessing_flows_are_registered_with_realtime_modules():
    registry = importlib.import_module("echodataflow.deployment.flow_registry")

    assert registry.FLOW_REGISTRY["raw2Sv_postprocessing"].entrypoint == (
        "echodataflow/flows/flows_acoustics.py:flow_raw2Sv_postprocessing"
    )
    assert registry.FLOW_REGISTRY["create_MVBS_postprocessing"].entrypoint == (
        "echodataflow/flows/flows_acoustics.py:flow_create_MVBS_postprocessing"
    )
    assert registry.FLOW_REGISTRY["predict_hake_postprocessing"].entrypoint == (
        "echodataflow/flows/flows_predict_hake.py:flow_predict_hake_postprocessing"
    )


def test_flow_registry_contains_process_cps_entrypoints():
    registry = importlib.import_module(
        "echodataflow.deployment.flow_registry"
    )

    sv_registration = registry.FLOW_REGISTRY[
        "process_Sv_CPS"
    ]
    transect_registration = registry.FLOW_REGISTRY[
        "process_transect_CPS"
    ]

    assert sv_registration.entrypoint == (
        "echodataflow/flows/flows_CPS_sv.py:"
        "flow_process_Sv_CPS"
    )

    assert transect_registration.entrypoint == (
        "echodataflow/flows/flows_CPS_transect.py:"
        "flow_process_transect_CPS"
    )

def test_resolve_registered_flows_defaults_to_recipe_key(
    monkeypatch,
    install_prefect_stubs,
):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")
    registry = importlib.import_module("echodataflow.deployment.flow_registry")

    fake_flow = object()
    fake_module = types.ModuleType("echodataflow.flows.example")
    fake_module.flow_example = fake_flow
    monkeypatch.setitem(sys.modules, "echodataflow.flows.example", fake_module)
    monkeypatch.setattr(
        registry,
        "FLOW_REGISTRY",
        {
            "example": registry.FlowRegistration(
                entrypoint="echodataflow/flows/example.py:flow_example",
            )
        },
    )

    resolved = engine.resolve_registered_flows({"flows": {"example": {}}})

    assert resolved["example"]["flow_obj"] is fake_flow


def test_resolve_registered_flows_rejects_unregistered_key(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    with pytest.raises(KeyError, match="not registered"):
        engine.resolve_registered_flows({"flows": {"missing": {}}})
