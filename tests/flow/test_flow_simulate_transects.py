import pandas as pd
import pytest

from echodataflow.flows import flows_simulation


class FakeVariable:
    stored = {}

    @classmethod
    def get(cls, key, default=None):
        return cls.stored.get(key, default)

    @classmethod
    def set(cls, key, value, overwrite=False):
        assert overwrite is True
        cls.stored[key] = value


@pytest.mark.filterwarnings(
    "error:The behavior of DataFrame concatenation with empty or all-NA entries:FutureWarning"
)
def test_flow_simulate_transects_opens_closes_and_advances(monkeypatch, tmp_path):
    FakeVariable.stored = {}
    monkeypatch.setattr(flows_simulation, "Variable", FakeVariable)

    transect_csv = tmp_path / "transect_start_end_time.csv"

    kwargs = {
        "path_transect_csv": str(transect_csv),
        "survey_start": "2024-07-07T00:00:00Z",
        "transect_duration_minutes": 10,
        "start_transect_num": 1,
        "max_transects": 2,
    }

    # First run: open transect 001.
    flows_simulation.flow_simulate_transects.fn(**kwargs)

    df = pd.read_csv(transect_csv, dtype="string")

    assert df["transectPart"].tolist() == ["001"]
    assert df.loc[0, "transectStart"] == "2024-07-07T00:00:00+00:00"
    assert pd.isna(df.loc[0, "transectEnd"])

    # Second run: close transect 001.
    flows_simulation.flow_simulate_transects.fn(**kwargs)

    df = pd.read_csv(transect_csv, dtype="string")

    assert df["transectPart"].tolist() == ["001"]
    assert df.loc[0, "transectStart"] == "2024-07-07T00:00:00+00:00"
    assert df.loc[0, "transectEnd"] == "2024-07-07T00:10:00+00:00"


def test_flow_simulate_transects_stops_after_maximum(monkeypatch, tmp_path, capsys):
    FakeVariable.stored = {}
    monkeypatch.setattr(flows_simulation, "Variable", FakeVariable)

    transect_csv = tmp_path / "transect_start_end_time.csv"

    kwargs = {
        "path_transect_csv": str(transect_csv),
        "survey_start": "2024-07-07T00:00:00Z",
        "transect_duration_minutes": 10,
        "start_transect_num": 1,
        "max_transects": 1,
    }

    # Open 001, close 001, then attempt to advance beyond max_transects.
    flows_simulation.flow_simulate_transects.fn(**kwargs)
    flows_simulation.flow_simulate_transects.fn(**kwargs)
    flows_simulation.flow_simulate_transects.fn(**kwargs)

    output = capsys.readouterr().out
    assert "All simulated transects have been generated." in output

    df = pd.read_csv(transect_csv, dtype="string")
    assert df["transectPart"].tolist() == ["001"]
