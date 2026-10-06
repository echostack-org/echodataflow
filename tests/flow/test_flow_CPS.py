import pandas as pd

from echodataflow.flows import flows_CPS_transect
from echodataflow.utils.processing_ledger import (
    initialize_ledger,
    register_transect,
)

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
    # It must still be reconsidered if CPS/NASC outputs are missing.
    transect.to_csv(transect_csv, index=False)
    transect.to_csv(snapshot_csv, index=False)

    db_path = path_main / "processing.db"
    initialize_ledger(db_path)

    register_transect(
        db_path=db_path,
        transect_part="001",
        transect_number="001",
        start_time="2024-07-07T00:30:00Z",
        end_time="2024-07-07T00:35:00Z",
    )

    calls = []

    def fake_get_completed_cps_files(
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
        flows_CPS_transect,
        "get_completed_cps_files",
        fake_get_completed_cps_files,
    )

    flows_CPS_transect.flow_process_transect_CPS.fn(
        path_transect_csv=str(transect_csv),
        path_snapshot_csv=str(snapshot_csv),
        path_main=str(path_main),
    )

    output = capsys.readouterr().out

    assert len(calls) == 1
    assert "No CPS-ready Sv data for transect_001" in output