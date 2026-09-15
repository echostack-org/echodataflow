import pandas as pd

def test_process_cps_retries_completed_transect_missing_outputs(
    monkeypatch,
    tmp_path,
    capsys,
):
    path_main = tmp_path / "output"
    path_main.mkdir()

    transect_csv = tmp_path / "transects.csv"
    snapshot_csv = tmp_path / "snapshot.csv"

    transect = pd.DataFrame(
        {
            "transectPart": ["001"],
            "transectNumber": ["001"],
            "transectStart": ["2024-07-07T00:30:00Z"],
            "transectEnd": ["2024-07-07T00:35:00Z"],
        }
    )

    # The transect is already present in the snapshot.
    # Previously this meant CPS would never reconsider it.
    transect.to_csv(transect_csv, index=False)
    transect.to_csv(snapshot_csv, index=False)

    # flow_process_CPS requires the processing ledger to exist.
    (path_main / "processing.db").touch()

    calls = []

    def fake_get_completed_sv_files(
        db_path,
        start_time=None,
        end_time=None,
    ):
        calls.append(
            (
                db_path,
                start_time,
                end_time,
            )
        )
        return []

    monkeypatch.setattr(
        flows_CPS,
        "get_completed_sv_files",
        fake_get_completed_sv_files,
    )

    flows_CPS.flow_process_CPS.fn(
        path_transect_csv=str(transect_csv),
        path_snapshot_csv=str(snapshot_csv),
        path_main=str(path_main),
    )

    output = capsys.readouterr().out

    # Even though the snapshot already contains transect 001,
    # it must still be checked because CPS/NASC outputs are missing.
    assert len(calls) == 1
    assert "No Sv data for transect_001" in output