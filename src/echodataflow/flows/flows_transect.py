from pathlib import Path

import pandas as pd
from prefect import flow

from echodataflow.utils.processing_ledger import (
    get_completed_sv_files,
    resolve_database,
)


TRANSECT_COLUMNS = [
    "transectPart",
    "transectNumber",
    "transectStart",
    "transectEnd",
]


def get_changed_transects(
    current: pd.DataFrame,
    previous: pd.DataFrame,
) -> pd.DataFrame:
    """Return transect segments that are new or have been updated."""

    key_columns = TRANSECT_COLUMNS

    return (
        current.drop_duplicates(subset=key_columns).merge(
            previous[key_columns],
            on=key_columns,
            how="left",
            indicator=True,
        )
        .query("_merge == 'left_only'")
        .drop(columns="_merge")
    )


@flow(log_prints=True)
def flow_transect_update(
    path_transect_csv: str,
    path_snapshot_csv: str,
    path_main: str,
    processing_db: str = "processing.db",
):
    """Identify updated transects and find overlapping Sv files."""

    path_transect = Path(path_transect_csv)
    path_snapshot = Path(path_snapshot_csv)
    db_path = resolve_database(path_main, processing_db)

    # Read the current transect information, preserving transect identifiers
    # as strings so values with leading zeros (e.g., "002") are not converted
    # to integers by pandas
    try:
        current = pd.read_csv(
            path_transect,
            dtype="string",
        )
    except pd.errors.EmptyDataError:
        current = pd.DataFrame(
            columns=TRANSECT_COLUMNS
        )

    if not path_snapshot.exists():
        print("No previous transect snapshot found. Initializing snapshot.")
        current.to_csv(path_snapshot, index=False)
        return

    previous = pd.read_csv(path_snapshot, dtype="string")

    changed = get_changed_transects(current, previous)

    # Only process completed transects
    changed = changed.dropna(subset=["transectEnd"])

    if changed.empty:
        print("No new or updated transect segments.")
        current.to_csv(path_snapshot, index=False)
        return

    print(f"Found {len(changed)} new or updated transect segment(s):")
    print(changed)

    # Find Sv files overlapping each changed transect
    for _, transect in changed.iterrows():
        start_time = pd.to_datetime(transect["transectStart"], utc=True)
        end_time = pd.to_datetime(transect["transectEnd"], utc=True)

        sv_filenames = get_completed_sv_files(
            db_path,
            start_time=start_time,
            end_time=end_time,
        )

        print(f"\nTransect {transect['transectPart']}: {start_time} to {end_time}")
        print(f"Found {len(sv_filenames)} overlapping Sv file(s):")

        for filename in sv_filenames:
            print(f"- {filename}")

    # Save snapshot only after processing the current CSV
    current.to_csv(path_snapshot, index=False)
