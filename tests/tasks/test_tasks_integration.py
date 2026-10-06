import pandas as pd

from echodataflow.tasks import tasks_integration


def test_read_NASC_task_delegates_to_operation(monkeypatch):
    expected = pd.DataFrame({"NASC": [1.0]})
    calls = {}

    def fake_read(item, settings):
        calls["arguments"] = (item, settings)
        return expected

    monkeypatch.setattr(tasks_integration, "read_NASC", fake_read)
    item = object()
    settings = object()

    result = tasks_integration.task_read_NASC.fn(item, settings)

    assert result is expected
    assert calls["arguments"] == (item, settings)
