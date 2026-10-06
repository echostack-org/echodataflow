import pandas as pd

from echodataflow.flows.flows_transect import (
    flow_transect_update,
    get_changed_transects,
)
from echodataflow.utils.processing_ledger import (
    initialize_ledger,
    mark_raw_completed,
    register_raw_file,
)


def test_flow_transect_update_initializes_snapshot(tmp_path):
    transect_csv = tmp_path / "transects.csv"
    snapshot_csv = tmp_path / "snapshot.csv"
    path_main = tmp_path / "output"
    path_main.mkdir()

    pd.DataFrame(
        {
            "transectPart": ["001"],
            "transectNumber": ["001"],
            "transectStart": ["2024-07-07T00:00:00Z"],
            "transectEnd": ["2024-07-07T00:10:00Z"],
        }
    ).to_csv(transect_csv, index=False)

    flow_transect_update.fn(
        path_transect_csv=str(transect_csv),
        path_snapshot_csv=str(snapshot_csv),
        path_main=str(path_main),
    )

    assert snapshot_csv.exists()

    snapshot = pd.read_csv(
        snapshot_csv,
        dtype={
            "transectPart": str,
            "transectNumber": str,
        },
    )

    assert len(snapshot) == 1
    assert snapshot.loc[0, "transectPart"] == "001"


def test_flow_transect_update_finds_overlapping_sv(tmp_path, capsys):
    transect_csv = tmp_path / "transects.csv"
    snapshot_csv = tmp_path / "snapshot.csv"
    path_main = tmp_path / "output"
    path_main.mkdir()

    previous = pd.DataFrame(
        {
            "transectPart": ["001"],
            "transectNumber": ["001"],
            "transectStart": ["2024-07-07T00:00:00Z"],
            "transectEnd": ["2024-07-07T00:10:00Z"],
        }
    )

    current = pd.concat(
        [
            previous,
            pd.DataFrame(
                {
                    "transectPart": ["002"],
                    "transectNumber": ["002"],
                    "transectStart": ["2024-07-07T00:20:00Z"],
                    "transectEnd": ["2024-07-07T00:30:00Z"],
                }
            ),
        ],
        ignore_index=True,
    )

    previous.to_csv(snapshot_csv, index=False)
    current.to_csv(transect_csv, index=False)

    db_path = path_main / "processing.db"
    initialize_ledger(db_path)

    raw_before = tmp_path / "before.raw"
    raw_overlap = tmp_path / "overlap.raw"
    raw_after = tmp_path / "after.raw"

    raw_before.touch()
    raw_overlap.touch()
    raw_after.touch()

    register_raw_file(db_path, raw_before)
    register_raw_file(db_path, raw_overlap)
    register_raw_file(db_path, raw_after)

    mark_raw_completed(
        db_path,
        raw_before,
        "before_Sv.zarr",
        "2024-07-07T00:00:00Z",
        "2024-07-07T00:05:00Z",
    )

    mark_raw_completed(
        db_path,
        raw_overlap,
        "overlap_Sv.zarr",
        "2024-07-07T00:15:00Z",
        "2024-07-07T00:25:00Z",
    )

    mark_raw_completed(
        db_path,
        raw_after,
        "after_Sv.zarr",
        "2024-07-07T00:40:00Z",
        "2024-07-07T00:50:00Z",
    )

    flow_transect_update.fn(
        path_transect_csv=str(transect_csv),
        path_snapshot_csv=str(snapshot_csv),
        path_main=str(path_main),
    )

    output = capsys.readouterr().out

    assert "Found 1 new or updated transect segment(s)" in output
    assert "Transect 002" in output
    assert "Found 1 overlapping Sv file(s)" in output
    assert "overlap_Sv.zarr" in output
    assert "before_Sv.zarr" not in output
    assert "after_Sv.zarr" not in output


def test_get_changed_transects():
    previous = pd.DataFrame(
        {
            "transectPart": ["001"],
            "transectNumber": ["001"],
            "transectStart": ["2024-07-07T00:00:00Z"],
            "transectEnd": ["2024-07-07T00:10:00Z"],
        }
    )

    current = pd.concat(
        [
            previous,
            pd.DataFrame(
                {
                    "transectPart": ["002"],
                    "transectNumber": ["002"],
                    "transectStart": ["2024-07-07T00:20:00Z"],
                    "transectEnd": ["2024-07-07T00:30:00Z"],
                }
            ),
        ],
        ignore_index=True,
    )

    changed = get_changed_transects(current, previous)

    assert len(changed) == 1
    assert changed.iloc[0]["transectPart"] == "002"


def test_flow_transect_update_ignores_open_transect(tmp_path, capsys):
    transect_csv = tmp_path / "transects.csv"
    snapshot_csv = tmp_path / "snapshot.csv"
    path_main = tmp_path / "output"
    path_main.mkdir()

    previous = pd.DataFrame(
        {
            "transectPart": ["001"],
            "transectNumber": ["001"],
            "transectStart": ["2024-07-07T00:00:00Z"],
            "transectEnd": ["2024-07-07T00:10:00Z"],
        }
    )

    current = pd.concat(
        [
            previous,
            pd.DataFrame(
                {
                    "transectPart": ["002"],
                    "transectNumber": ["002"],
                    "transectStart": ["2024-07-07T00:20:00Z"],
                    "transectEnd": [pd.NA],
                }
            ),
        ],
        ignore_index=True,
    )

    previous.to_csv(snapshot_csv, index=False)
    current.to_csv(transect_csv, index=False)

    flow_transect_update.fn(
        path_transect_csv=str(transect_csv),
        path_snapshot_csv=str(snapshot_csv),
        path_main=str(path_main),
    )

    output = capsys.readouterr().out

    assert "No new or updated transect segments." in output

def test_get_changed_transects_detects_open_to_closed_update():
    previous = pd.DataFrame(
        {
            "transectPart": ["001"],
            "transectNumber": ["001"],
            "transectStart": ["2024-07-07T00:00:00Z"],
            "transectEnd": [pd.NA],
        }
    )

    current = pd.DataFrame(
        {
            "transectPart": ["001"],
            "transectNumber": ["001"],
            "transectStart": ["2024-07-07T00:00:00Z"],
            "transectEnd": ["2024-07-07T00:10:00Z"],
        }
    )

    changed = get_changed_transects(current, previous)

    assert len(changed) == 1
    assert changed.iloc[0]["transectPart"] == "001"
    assert changed.iloc[0]["transectEnd"] == "2024-07-07T00:10:00Z"

def test_flow_transect_update_handles_header_only_csv(
    tmp_path,
    capsys,
):
    transect_csv = tmp_path / "transects.csv"
    snapshot_csv = tmp_path / "snapshot.csv"
    path_main = tmp_path / "output"
    path_main.mkdir()

    transect_csv.write_text(
        "transectPart,transectNumber,transectStart,transectEnd\n"
    )

    flow_transect_update.fn(
        path_transect_csv=str(transect_csv),
        path_snapshot_csv=str(snapshot_csv),
        path_main=str(path_main),
    )

    output = capsys.readouterr().out

    assert snapshot_csv.exists()
    assert "No previous transect snapshot found. Initializing snapshot." in output


def test_flow_transect_update_handles_zero_byte_csv(
    tmp_path,
):
    transect_csv = tmp_path / "transects.csv"
    snapshot_csv = tmp_path / "snapshot.csv"
    path_main = tmp_path / "output"
    path_main.mkdir()

    transect_csv.touch()

    flow_transect_update.fn(
        path_transect_csv=str(transect_csv),
        path_snapshot_csv=str(snapshot_csv),
        path_main=str(path_main),
    )

    assert snapshot_csv.exists()