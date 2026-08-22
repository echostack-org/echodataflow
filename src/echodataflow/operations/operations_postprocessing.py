"""Pure planning helpers for batch acoustic post-processing."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

from echodataflow.utils.manifests import (
    MVBS_COLUMNS_POSTPROCESSING,
    PREDICTION_COLUMNS_POSTPROCESSING,
    filter_time_range,
    read_manifest,
    write_manifest,
)
from echodataflow.utils.utils import extract_datetime_from_filename


@dataclass(frozen=True)
class PlannedSlice:
    """A time window and the files that overlap it."""

    start_time: pd.Timestamp
    end_time: pd.Timestamp
    filenames: tuple[str, ...]
    is_partial: bool = False
    input_signature: str = ""


@dataclass(frozen=True)
class TimeWindow:
    """One aligned half-open time interval."""

    start_time: pd.Timestamp
    end_time: pd.Timestamp


def generate_aligned_windows(
    first_time,
    last_time,
    window_mins: int,
) -> list[TimeWindow]:
    """Generate complete UTC-aligned windows between two timestamps."""
    if window_mins <= 0:
        raise ValueError("slice length must be greater than zero")
    duration = pd.Timedelta(minutes=window_mins)
    start = pd.to_datetime(first_time, utc=True).floor(f"{window_mins}min")
    final_end = pd.to_datetime(last_time, utc=True).floor(f"{window_mins}min")

    windows: list[TimeWindow] = []
    while start + duration <= final_end:
        windows.append(TimeWindow(start_time=start, end_time=start + duration))
        start += duration
    return windows


def select_overlapping_records(
    records: pd.DataFrame,
    window: TimeWindow,
    start_column: str,
    end_column: str,
) -> pd.DataFrame:
    """Select records whose time coverage overlaps a window."""
    return records[
        (records[end_column] >= window.start_time) & (records[start_column] < window.end_time)
    ]


def select_contained_records(
    records: pd.DataFrame,
    window: TimeWindow,
    start_column: str,
    end_column: str,
) -> pd.DataFrame:
    """Select records fully contained within a window."""
    return records[
        (records[start_column] >= window.start_time) & (records[end_column] <= window.end_time)
    ]


def build_input_signature(
    records: pd.DataFrame,
    columns: list[str],
    window: TimeWindow,
) -> str:
    """Create a deterministic fingerprint of window inputs and their metadata."""

    def fix_value(value):
        if pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        return str(value)

    rows = [
        {column: fix_value(row[column]) for column in columns}
        for _, row in records.sort_values(columns[0]).iterrows()
    ]
    payload = {
        "window_start": window.start_time.isoformat(),
        "window_end": window.end_time.isoformat(),
        "inputs": rows,
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()


def build_Sv_ledger(raw_files: pd.DataFrame) -> pd.DataFrame:
    """Build the fixed post-processing Sv ledger from the complete raw list."""
    required = {"s3_path"}
    if not required.issubset(raw_files.columns):
        raise ValueError(f"raw list must contain columns: {sorted(required)}")

    ledger = raw_files[["s3_path"]].copy()
    ledger["s3_path"] = ledger["s3_path"].astype(str)
    ledger["raw_filename"] = ledger["s3_path"].map(lambda value: Path(value).name)
    if ledger["s3_path"].duplicated().any():
        raise ValueError("raw list contains duplicate S3 paths")
    if ledger["raw_filename"].duplicated().any():
        raise ValueError("raw list contains duplicate basenames")

    ledger["timestamp"] = ledger["raw_filename"].map(extract_datetime_from_filename)
    invalid = ledger.loc[ledger["timestamp"].isna(), "raw_filename"].tolist()
    if invalid:
        raise ValueError(f"Could not parse timestamps from raw filenames: {invalid}")
    ledger["timestamp"] = pd.to_datetime(ledger["timestamp"], utc=True)

    ledger["Sv_filename"] = pd.NA
    ledger["raw2Sv_status"] = "pending"
    ledger["attempt_count"] = 0
    ledger["first_ping_time"] = pd.NaT
    ledger["last_ping_time"] = pd.NaT
    ledger["error"] = ""
    ledger["Sv_cleanup_status"] = "pending"
    ledger["Sv_deleted_at"] = pd.NaT
    ledger["Sv_cleanup_error"] = ""
    return ledger.sort_values("timestamp").reset_index(drop=True)


def build_MVBS_ledger(
    ledger_Sv: pd.DataFrame,
    slice_mins: int = 20,
    no_data_gap_hours: float = 3.0,
) -> pd.DataFrame:
    """Preplan every MVBS slice and its required raw files."""
    if no_data_gap_hours <= 0:
        raise ValueError("no_data_gap_hours must be greater than zero")
    if ledger_Sv.empty:
        return pd.DataFrame(columns=MVBS_COLUMNS_POSTPROCESSING)

    df_Sv = ledger_Sv.copy()
    df_Sv["timestamp"] = pd.to_datetime(df_Sv["timestamp"], utc=True)
    duration = pd.Timedelta(minutes=slice_mins)
    final_end = df_Sv["timestamp"].max().floor(f"{slice_mins}min") + duration
    windows = generate_aligned_windows(
        df_Sv["timestamp"].min(),
        final_end,
        slice_mins,
    )

    records = []
    no_data_gap = pd.Timedelta(hours=no_data_gap_hours)
    for window in windows:
        # Record the raw files required by this slice, including its predecessor
        required = filter_time_range(
            df=df_Sv,
            column_start_time="timestamp",
            column_end_time=None,
            start_time=window.start_time,
            end_time=window.end_time,
        )

        # Set the slice status to no_data if there is a long gap
        # between the last Sv timestamp and the slice start
        latest_Sv_timestamp = required["timestamp"].max()
        is_long_gap = (
            pd.notna(latest_Sv_timestamp) and window.start_time - latest_Sv_timestamp > no_data_gap
        )
        records.append(
            {
                "MVBS_filename": f"MVBS_{window.start_time:%Y%m%dT%H%M%S}.zarr",
                "slice_start": window.start_time,
                "slice_end": window.end_time,
                "raw_filenames": json.dumps(required["raw_filename"].tolist()),
                "first_ping_time": pd.NaT,
                "last_ping_time": pd.NaT,
                "is_partial": pd.NA,
                "MVBS_status": "no_data" if is_long_gap else "pending",
                "attempt_count": 0,
                "error": (
                    f"No Sv file timestamp within {no_data_gap_hours} hours before the slice start"
                    if is_long_gap
                    else ""
                ),
            }
        )
    return pd.DataFrame.from_records(records, columns=MVBS_COLUMNS_POSTPROCESSING)


def failure_state(attempt_count: int, max_flow_run_attempts: int) -> tuple[int, str]:
    """Increment a failure count and return its retryable or terminal status."""
    if max_flow_run_attempts < 1:
        raise ValueError("max_flow_run_attempts must be a positive integer")

    attempt_count += 1
    status = "always_failed" if attempt_count >= max_flow_run_attempts else "failed"
    return attempt_count, status


def propagate_status(
    upstream_ledger: pd.DataFrame,
    downstream_ledger: pd.DataFrame,
    *,
    upstream_filename_column: str,
    upstream_status_column: str,
    downstream_filenames_column: str,
    downstream_status_column: str,
    upstream_label: str,
) -> pd.DataFrame:
    """Propagate terminal blocked and all-no-data upstream states downstream."""
    updated = downstream_ledger.copy()
    if upstream_ledger.empty or downstream_ledger.empty:
        return updated

    # A terminal failure in any dependency takes precedence over no-data.
    blocked_inputs = set(
        upstream_ledger.loc[
            upstream_ledger[upstream_status_column].isin(["blocked", "always_failed"]),
            upstream_filename_column,
        ].astype(str)
    )
    for idx, row in updated.iterrows():
        if row[downstream_status_column] not in {"pending", "failed"}:
            continue
        required_names = json.loads(row[downstream_filenames_column])
        blocked_by = sorted(blocked_inputs.intersection(required_names))
        if blocked_by:
            updated.loc[idx, [downstream_status_column, "error"]] = [
                "blocked",
                f"Required {upstream_label} cannot be processed: {blocked_by}",
            ]
            continue

        required_upstream = upstream_ledger[
            upstream_ledger[upstream_filename_column].isin(required_names)
        ]
        # All dependencies must be known before propagating no-data.
        if len(required_upstream) != len(required_names):
            continue
        if (
            not required_upstream.empty
            and required_upstream[upstream_status_column].eq("no_data").all()
        ):
            updated.loc[idx, [downstream_status_column, "error"]] = [
                "no_data",
                f"All required {upstream_label} have no data",
            ]
    return updated


def build_prediction_ledger(
    ledger_MVBS: pd.DataFrame,
    prediction_slice_mins: int = 40,
) -> pd.DataFrame:
    """Preplan every prediction window and its required MVBS slices."""
    if ledger_MVBS.empty:
        return pd.DataFrame(columns=PREDICTION_COLUMNS_POSTPROCESSING)

    df_MVBS = ledger_MVBS.copy()
    df_MVBS["slice_start"] = pd.to_datetime(df_MVBS["slice_start"], utc=True)
    df_MVBS["slice_end"] = pd.to_datetime(df_MVBS["slice_end"], utc=True)
    windows = generate_aligned_windows(
        df_MVBS["slice_start"].min(),
        df_MVBS["slice_end"].max(),
        prediction_slice_mins,
    )

    records = []
    for window in windows:
        required = filter_time_range(
            df=df_MVBS,
            column_start_time="slice_start",
            column_end_time="slice_end",
            start_time=window.start_time,
            end_time=window.end_time,
            include_exact_start_time=False,
        )
        is_no_data = not required.empty and required["MVBS_status"].eq("no_data").all()
        records.append(
            {
                "prediction_filename_postfix": window.start_time.strftime("%Y%m%dT%H%M%S"),
                "slice_start": window.start_time,
                "slice_end": window.end_time,
                "MVBS_filenames": json.dumps(required["MVBS_filename"].tolist()),
                "score_filename": pd.NA,
                "softmax_filename": pd.NA,
                "prediction_filename": pd.NA,
                "evr_filename": pd.NA,
                "first_ping_time": pd.NaT,
                "last_ping_time": pd.NaT,
                "prediction_status": "no_data" if is_no_data else "pending",
                "attempt_count": 0,
                "error": "No MVBS data in the prediction window" if is_no_data else "",
            }
        )
    return pd.DataFrame.from_records(records, columns=PREDICTION_COLUMNS_POSTPROCESSING)


def read_or_create_ledger(
    ledger_path: Path,
    columns: list[str],
    date_columns: list[str],
    builder: Callable[[], pd.DataFrame],
) -> pd.DataFrame:
    """Load a ledger or create it with the supplied builder."""
    if ledger_path.exists():
        return read_manifest(path=ledger_path, columns=columns, date_columns=date_columns)

    ledger = builder()
    write_manifest(ledger, ledger_path)
    return ledger


def plan_mvbs_slices(
    ledger_Sv: pd.DataFrame,
    ledger_MVBS: pd.DataFrame,
) -> list[PlannedSlice]:
    """Return pending MVBS slices whose required raw conversions are complete."""
    if ledger_Sv.empty or ledger_MVBS.empty:
        return []

    df_Sv = ledger_Sv.copy()
    df_MVBS = ledger_MVBS.copy()

    # Ensure datetime columns are in UTC
    df_Sv["first_ping_time"] = pd.to_datetime(df_Sv["first_ping_time"], utc=True)
    df_Sv["last_ping_time"] = pd.to_datetime(df_Sv["last_ping_time"], utc=True)
    df_MVBS["slice_start"] = pd.to_datetime(df_MVBS["slice_start"], utc=True)
    df_MVBS["slice_end"] = pd.to_datetime(df_MVBS["slice_end"], utc=True)

    slices: list[PlannedSlice] = []
    for row in df_MVBS.itertuples(index=False):
        # Skip slices that have already been created successfully
        if row.MVBS_status not in {"pending", "failed"}:
            continue

        required_raw = json.loads(row.raw_filenames)
        required_Sv = df_Sv[df_Sv["raw_filename"].isin(required_raw)]
        if len(required_Sv) != len(required_raw):
            raise ValueError(f"MVBS ledger references unknown raw files: {required_raw}")

        # Skip if any required Sv conversions are not yet completed
        if not required_Sv["raw2Sv_status"].eq("completed").all():
            continue

        slices.append(
            PlannedSlice(
                start_time=row.slice_start,
                end_time=row.slice_end,
                filenames=tuple(required_Sv.sort_values("timestamp")["Sv_filename"].astype(str)),
                is_partial=(
                    required_Sv["first_ping_time"].min() > row.slice_start
                    or required_Sv["last_ping_time"].max() < row.slice_end
                ),
            )
        )
    return slices


def plan_Sv_cleanup(
    ledger_Sv: pd.DataFrame,
    ledger_MVBS: pd.DataFrame,
) -> list[str]:
    """Return Sv filenames safe to delete after all dependent MVBS work succeeds."""
    if ledger_Sv.empty or ledger_MVBS.empty:
        return []

    dependencies: dict[str, list[str]] = {}
    for row in ledger_MVBS.itertuples(index=False):
        for raw_filename in json.loads(row.raw_filenames):
            dependencies.setdefault(raw_filename, []).append(row.MVBS_status)

    successful_statuses = {"completed", "no_data"}
    cleanup_filenames = []
    for _, row in ledger_Sv.iterrows():
        if row["Sv_cleanup_status"] not in {"pending", "failed"}:
            continue
        if pd.isna(row["Sv_filename"]):
            continue
        statuses = dependencies.get(str(row["raw_filename"]), [])
        if statuses and all(status in successful_statuses for status in statuses):
            cleanup_filenames.append(str(row["Sv_filename"]))
    return cleanup_filenames


def plan_prediction_slices(
    ledger_MVBS: pd.DataFrame,
    ledger_prediction: pd.DataFrame,
) -> list[PlannedSlice]:
    """Return pending prediction windows whose required MVBS slices are ready."""
    if ledger_MVBS.empty or ledger_prediction.empty:
        return []

    df_MVBS = ledger_MVBS.copy()
    df_prediction = ledger_prediction.copy()

    # Ensure datetime columns are in UTC
    df_prediction["slice_start"] = pd.to_datetime(df_prediction["slice_start"], utc=True)
    df_prediction["slice_end"] = pd.to_datetime(df_prediction["slice_end"], utc=True)

    slices: list[PlannedSlice] = []
    for row in df_prediction.itertuples(index=False):
        if row.prediction_status not in {"pending", "failed"}:
            continue

        required_names = json.loads(row.MVBS_filenames)
        required_MVBS = df_MVBS[df_MVBS["MVBS_filename"].isin(required_names)]
        if len(required_MVBS) != len(required_names):
            raise ValueError(f"Prediction ledger references unknown MVBS files: {required_names}")

        # No-data slices are terminal and do not need to be passed to prediction.
        if not required_MVBS["MVBS_status"].isin(["completed", "no_data"]).all():
            continue

        completed_MVBS = required_MVBS[required_MVBS["MVBS_status"] == "completed"]
        if completed_MVBS.empty:
            continue

        partial = (
            completed_MVBS["is_partial"]
            .map(lambda value: str(value).strip().lower() in {"true", "1", "yes"})
            .any()
            or required_MVBS["MVBS_status"].eq("no_data").any()
        )

        slices.append(
            PlannedSlice(
                start_time=row.slice_start,
                end_time=row.slice_end,
                filenames=tuple(
                    completed_MVBS.sort_values("slice_start")["MVBS_filename"].astype(str)
                ),
                is_partial=bool(partial),
            )
        )
    return slices
