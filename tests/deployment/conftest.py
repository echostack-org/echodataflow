import sys
import types

import pytest


class FakeVariable:
    calls: list[dict[str, object]] = []

    @classmethod
    def set(cls, key, value, overwrite):
        cls.calls.append({"key": key, "value": value, "overwrite": overwrite})
        return None


class FakeTrigger:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class FakeRunnerDeployment:
    pass


class FakePrefectFlowGeneric:
    @classmethod
    def __class_getitem__(cls, _item):
        return cls


@pytest.fixture
def install_prefect_stubs(monkeypatch):
    def _install(*, sink=None):
        prefect_mod = types.ModuleType("prefect")
        if sink is None:
            prefect_mod.deploy = lambda *args, **kwargs: None
        else:
            def fake_deploy(*deployments, **kwargs):
                sink["deploy_call"] = {
                    "deployments": list(deployments),
                    "kwargs": kwargs,
                }

            prefect_mod.deploy = fake_deploy

        deployments_mod = types.ModuleType("prefect.deployments")
        runner_mod = types.ModuleType("prefect.deployments.runner")
        runner_mod.RunnerDeployment = FakeRunnerDeployment

        flows_mod = types.ModuleType("prefect.flows")
        flows_mod.Flow = FakePrefectFlowGeneric

        variables_mod = types.ModuleType("prefect.variables")
        variables_mod.Variable = FakeVariable

        events_mod = types.ModuleType("prefect.events")
        events_mod.DeploymentEventTrigger = FakeTrigger

        monkeypatch.setitem(sys.modules, "prefect", prefect_mod)
        monkeypatch.setitem(sys.modules, "prefect.deployments", deployments_mod)
        monkeypatch.setitem(sys.modules, "prefect.deployments.runner", runner_mod)
        monkeypatch.setitem(sys.modules, "prefect.flows", flows_mod)
        monkeypatch.setitem(sys.modules, "prefect.variables", variables_mod)
        monkeypatch.setitem(sys.modules, "prefect.events", events_mod)

        return {
            "FakeVariable": FakeVariable,
            "FakeTrigger": FakeTrigger,
            "FakeRunnerDeployment": FakeRunnerDeployment,
            "FakePrefectFlowGeneric": FakePrefectFlowGeneric,
        }

    return _install
