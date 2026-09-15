from pathlib import Path

import pandas as pd
import pytest

from echodataflow.utils.manifests import (
    MVBS_COLUMNS_POSTPROCESSING,
    PREDICTION_COLUMNS_POSTPROCESSING,
    SV_COLUMNS_POSTPROCESSING,
    filter_time_range,
    manifest_signature_changed,
    read_manifest,
    write_manifest,
)


@pytest.mark.parametrize(
    ("columns", "date_columns", "status_column", "blank_filename"),
    [
        (
            SV_COLUMNS_POSTPROCESSING,
            ["timestamp", "first_ping_time", "last_ping_time", "Sv_deleted_at"],
            "raw2Sv_status",
            "Sv_filename",
        ),
        (
            MVBS_COLUMNS_POSTPROCESSING,
            ["slice_start", "slice_end", "first_ping_time", "last_ping_time"],
            "MVBS_status",
            None,
        ),
        (
            PREDICTION_COLUMNS_POSTPROCESSING,
            ["slice_start", "slice_end", "first_ping_time", "last_ping_time"],
            "prediction_status",
            "prediction_filename",
        ),
    ],
)
def test_postprocessing_manifest_restores_blank_text_columns(
    tmp_path, columns, date_columns, status_column, blank_filename
):
    path = tmp_path / "ledger.csv"
    row = {column: pd.NA for column in columns}
    row[status_column] = "failed"
    row["attempt_count"] = 1
    pd.DataFrame([row], columns=columns).to_csv(path)

    manifest = read_manifest(path, columns, date_columns)
    manifest.loc[0, [status_column, "error"]] = ["pending", ""]

    assert manifest.loc[0, "error"] == ""
    assert str(manifest["error"].dtype) == "string"
    if blank_filename:
        manifest.loc[0, blank_filename] = "result.zarr"
        assert manifest.loc[0, blank_filename] == "result.zarr"
    if "is_partial" in manifest:
        manifest.loc[0, "is_partial"] = True
        assert manifest.loc[0, "is_partial"]


def test_manifest_signature_detects_missing_and_changed_outputs():
    manifest = pd.DataFrame(
        {
            "filename": ["output.zarr"],
            "input_signature": ["current"],
        }
    )

    assert not manifest_signature_changed(manifest, "filename", "output.zarr", "current")
    assert manifest_signature_changed(manifest, "filename", "output.zarr", "changed")
    assert manifest_signature_changed(manifest, "filename", "missing.zarr", "current")


def test_manifest_round_trip_normalizes_utc(tmp_path):
    path = tmp_path / "manifest.csv"
    pd.DataFrame(
        {
            "filename": ["example.zarr"],
            "first_ping_time": ["2025-06-19T00:00:00"],
            "last_ping_time": ["2025-06-19T00:01:00"],
        }
    ).to_csv(path)

    manifest = read_manifest(
        path,
        ["filename", "first_ping_time", "last_ping_time"],
        ["first_ping_time", "last_ping_time"],
    )

    assert manifest.columns.tolist() == [
        "filename",
        "first_ping_time",
        "last_ping_time",
    ]
    assert manifest.loc[0, "first_ping_time"] == pd.Timestamp("2025-06-19T00:00:00Z")
    assert manifest.loc[0, "last_ping_time"] == pd.Timestamp("2025-06-19T00:01:00Z")

    write_manifest(manifest, path)

    assert path.exists()
    assert not Path(f"{path}.tmp").exists()


def test_read_manifest_accepts_mixed_naive_and_utc_timestamps(tmp_path):
    path = tmp_path / "manifest.csv"
    pd.DataFrame(
        {
            "filename": ["first.zarr", "second.zarr"],
            "first_ping_time": [
                "2025-06-19 00:00:00",
                "2025-06-19 00:20:00+00:00",
            ],
        }
    ).to_csv(path)

    manifest = read_manifest(
        path,
        ["filename", "first_ping_time"],
        ["first_ping_time"],
    )

    assert manifest["first_ping_time"].tolist() == [
        pd.Timestamp("2025-06-19T00:00:00Z"),
        pd.Timestamp("2025-06-19T00:20:00Z"),
    ]


@pytest.mark.parametrize(
    ("stored_columns", "error_text"),
    [
        (["filename"], "missing columns=['first_ping_time']"),
        (
            ["filename", "first_ping_time", "legacy_column"],
            "unexpected columns=['legacy_column']",
        ),
    ],
)
def test_read_manifest_rejects_schema_drift(tmp_path, stored_columns, error_text):
    path = tmp_path / "manifest.csv"
    pd.DataFrame([{column: "value" for column in stored_columns}]).to_csv(path)

    with pytest.raises(ValueError, match=error_text.replace("[", r"\[").replace("]", r"\]")):
        read_manifest(path, ["filename", "first_ping_time"], ["first_ping_time"])


def test_filter_time_range_with_start_only_includes_preceding_file():
    frame = pd.DataFrame(
        {
            "s3_path": ["before.raw", "first.raw", "last.raw", "after.raw", "later.raw"],
            "timestamp": pd.to_datetime(
                [
                    "2025-06-18T23:54:00Z",
                    "2025-06-19T00:02:00Z",
                    "2025-06-19T01:54:00Z",
                    "2025-06-19T02:02:00Z",
                    "2025-06-19T02:10:00Z",
                ]
            ),
        }
    )

    selected = filter_time_range(
        frame,
        "timestamp",
        None,
        "2025-06-19T00:00:00Z",
        "2025-06-19T02:00:00Z",
    )

    assert selected["s3_path"].tolist() == [
        "before.raw",
        "first.raw",
        "last.raw",
    ]


def test_filter_time_range_remains_half_open_without_neighbors():
    frame = pd.DataFrame(
        {
            "s3_path": ["start.raw", "end.raw"],
            "timestamp": pd.to_datetime(["2025-06-19T00:00:00Z", "2025-06-19T02:00:00Z"]),
        }
    )

    selected = filter_time_range(
        frame,
        "timestamp",
        None,
        "2025-06-19T00:00:00Z",
        "2025-06-19T02:00:00Z",
    )

    assert selected["s3_path"].tolist() == ["start.raw"]


def test_filter_time_range_with_start_and_end_selects_overlapping_records():
    frame = pd.DataFrame(
        {
            "name": ["before", "crosses-start", "inside", "crosses-end", "after"],
            "first_ping_time": pd.to_datetime(
                [
                    "2025-06-18T23:40:00Z",
                    "2025-06-18T23:55:00Z",
                    "2025-06-19T00:20:00Z",
                    "2025-06-19T01:55:00Z",
                    "2025-06-19T02:00:00Z",
                ]
            ),
            "last_ping_time": pd.to_datetime(
                [
                    "2025-06-18T23:50:00Z",
                    "2025-06-19T00:05:00Z",
                    "2025-06-19T00:30:00Z",
                    "2025-06-19T02:05:00Z",
                    "2025-06-19T02:10:00Z",
                ]
            ),
        }
    )

    selected = filter_time_range(
        frame,
        "first_ping_time",
        "last_ping_time",
        "2025-06-19T00:00:00Z",
        "2025-06-19T02:00:00Z",
    )

    assert selected["name"].tolist() == ["crosses-start", "inside", "crosses-end"]


def test_filter_time_range_can_exclude_interval_ending_at_exact_start():
    frame = pd.DataFrame(
        {
            "name": ["touches-start", "overlaps"],
            "start": pd.to_datetime(["2025-06-18T23:40:00Z", "2025-06-18T23:50:00Z"]),
            "end": pd.to_datetime(["2025-06-19T00:00:00Z", "2025-06-19T00:05:00Z"]),
        }
    )

    selected = filter_time_range(
        frame,
        "start",
        "end",
        "2025-06-19T00:00:00Z",
        "2025-06-19T00:40:00Z",
        include_exact_start_time=False,
    )

    assert selected["name"].tolist() == ["overlaps"]
