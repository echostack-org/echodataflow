import pandas as pd
import pytest

from echodataflow.flows import flows_CPS_sv as flows


@pytest.mark.parametrize("manifest", ["missing", "empty"])
def test_no_manifest_data_does_not_start_tasks(monkeypatch, tmp_path, manifest):
    if manifest == "empty":
        pd.DataFrame(columns=["Sv_filename"]).to_csv(
            tmp_path / "Sv_files.csv", index=False
        )
    calls = []
    monkeypatch.setattr(flows, "task_process_sv_cps", lambda **kw: calls.append(kw))
    flows.flow_process_Sv_CPS.fn(path_main=str(tmp_path))
    assert calls == []


def test_flow_selects_one_file_and_passes_recipe_parameters(monkeypatch, tmp_path):
    path = tmp_path / "Sv" / "example_Sv.zarr"
    path.mkdir(parents=True)
    (path / ".zmetadata").write_text("{}")
    pd.DataFrame({"Sv_filename": [path.name]}).to_csv(
        tmp_path / "Sv_files.csv", index=False
    )
    calls = []
    monkeypatch.setattr(flows, "task_process_sv_cps", lambda **kw: calls.append(kw))
    flows.flow_process_Sv_CPS.fn(
        path_main=str(tmp_path),
        new_file_num_limit=1,
        target_frequency=38000,
        min_depth=12,
        mask_mode="threshold",
        fallback_sv_threshold=-65,
    )
    assert len(calls) == 1
    call = calls[0]
    assert call["sv_path"] == str(path.resolve())
    assert call["sv_filename"] == path.name
    assert call["target_frequency"] == 38000
    assert call["min_depth"] == 12
    assert call["mask_mode"] == "threshold"
    assert call["fallback_sv_threshold"] == -65
    assert call["path_cps_sv"] == str(tmp_path / "CPS_Sv")
    assert call["db_path"] == str(tmp_path / "processing.db")


def test_flow_enforces_batch_limit_with_real_ledger(monkeypatch, tmp_path):
    names = ["a_Sv.zarr", "b_Sv.zarr"]
    for name in names:
        path = tmp_path / "Sv" / name
        path.mkdir(parents=True)
        (path / ".zmetadata").write_text("{}")
    pd.DataFrame({"Sv_filename": names}).to_csv(tmp_path / "Sv_files.csv", index=False)
    calls = []
    monkeypatch.setattr(flows, "task_process_sv_cps", lambda **kw: calls.append(kw))
    flows.flow_process_Sv_CPS.fn(path_main=str(tmp_path), new_file_num_limit=1)
    assert len(calls) == 1
