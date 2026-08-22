"""Utilities for reading, writing, and filtering processing manifests."""

from __future__ import annotations

from pathlib import Path
import pandas as pd

SV_COLUMNS_REALTIME = [
    "raw_filename",
    "Sv_filename",
    "first_ping_time",
    "last_ping_time",
]
MVBS_COLUMNS_REALTIME = ["MVBS_filename", "first_ping_time", "last_ping_time"]
PREDICTION_COLUMNS_REALTIME = [
    "prediction_filename_postfix",
    "score_filename",
    "softmax_filename",
    "prediction_filename",
    "evr_filename",
    "first_ping_time",
    "last_ping_time",
]
SV_COLUMNS_POSTPROCESSING = [
    "s3_path",
    "timestamp",
    "raw_filename",
    "Sv_filename",
    "raw2Sv_status",
    "attempt_count",
    "first_ping_time",
    "last_ping_time",
    "error",
    "Sv_cleanup_status",
    "Sv_deleted_at",
    "Sv_cleanup_error",
]
MVBS_COLUMNS_POSTPROCESSING = [
    "MVBS_filename",
    "slice_start",
    "slice_end",
    "raw_filenames",
    "first_ping_time",
    "last_ping_time",
    "is_partial",
    "MVBS_status",
    "attempt_count",
    "error",
]
PREDICTION_COLUMNS_POSTPROCESSING = [
    "prediction_filename_postfix",
    "slice_start",
    "slice_end",
    "MVBS_filenames",
    "score_filename",
    "softmax_filename",
    "prediction_filename",
    "evr_filename",
    "first_ping_time",
    "last_ping_time",
    "prediction_status",
    "attempt_count",
    "error",
]


def read_manifest(path: Path, columns: list[str], date_columns: list[str]) -> pd.DataFrame:
    # Return a schema-correct empty manifest on the first run
    if not path.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(path, index_col=0)

    # Validation: catch missing or unexpected columns
    missing = [column for column in columns if column not in df.columns]
    unexpected = [column for column in df.columns if column not in columns]
    if missing or unexpected:
        raise ValueError(
            f"Invalid manifest schema for {path}: "
            f"missing columns={missing}, unexpected columns={unexpected}"
        )
    df = df[columns]

    # Set datetime columns to UTC
    for column in date_columns:
        if column in df:
            # Ledger updates can mix legacy naive values with UTC-qualified values
            df[column] = pd.to_datetime(df[column], format="mixed", utc=True)
    return df


def write_manifest(df: pd.DataFrame, path: Path) -> None:
    """Replace a manifest atomically; callers must still enforce one writer."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # Never expose a partially written CSV to a polling downstream flow
    temporary = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(temporary, date_format="%Y-%m-%dT%H:%M:%S.%f%z")
    temporary.replace(path)


def manifest_signature_changed(
    df: pd.DataFrame,
    identity_column: str,
    identity: str,
    input_signature: str,
) -> bool:
    """Return whether an output is missing or its recorded inputs changed."""
    matches = df[df[identity_column] == identity]
    if matches.empty:
        return True
    stored = matches.iloc[-1]["input_signature"]
    return pd.isna(stored) or str(stored) != input_signature


def filter_time_range(
    df: pd.DataFrame,
    column_start_time: str,
    column_end_time: str | None,
    start_time: str | None,
    end_time: str | None,
    include_exact_start_time: bool = True,
) -> pd.DataFrame:
    # Treat requested ranges as half-open: [start_time, end_time)
    ordered = df.sort_values(column_start_time)
    selected = ordered.copy()

    # When only column_start_time is provided (e.g. raw filename that only marks start time)
    if column_end_time is None:
        if start_time is not None:
            lower = pd.to_datetime(start_time, utc=True)
            if include_exact_start_time:
                selected = selected[selected[column_start_time] >= lower]
            else:
                selected = selected[selected[column_start_time] > lower]
            # Include the last file starting before the requested interval
            prev = ordered[ordered[column_start_time] < lower].tail(1)
            selected = pd.concat([selected, prev]).drop_duplicates()
        if end_time is not None:
            selected = selected[selected[column_start_time] < pd.to_datetime(end_time, utc=True)]

    # When both column_start_time and column_end_time are provided (e.g. slices that mark start and end time)
    else:
        if start_time is not None:
            lower = pd.to_datetime(start_time, utc=True)
            if include_exact_start_time:
                selected = selected[selected[column_end_time] >= lower]
            else:
                selected = selected[selected[column_end_time] > lower]
        if end_time is not None:
            selected = selected[selected[column_start_time] < pd.to_datetime(end_time, utc=True)]

    return selected.sort_values(column_start_time)
