from pathlib import Path

import pandas as pd
from prefect.events import emit_event
from prefect.events.worker import EventsWorker
from watchdog.observers import Observer

from echodataflow.utils.file_watcher import (
    watch_directory,
    watch_file,
)
from echodataflow.utils.processing_ledger import (
    initialize_ledger,
    register_raw_file,
    register_transect,
)


TRANSECT_UPDATE_EVENT = "echodataflow.transect.updated"
TRANSECT_RESOURCE_ID = "transect-start-end-time"
TRANSECT_RELATED_RESOURCE_ID = "transect-monitor"

TRANSECT_COLUMNS = [
    "transectPart",
    "transectNumber",
    "transectStart",
    "transectEnd",
]


def _flush_events() -> None:
    """Wait until queued Prefect events have been sent."""
    EventsWorker.instance().wait_until_empty()


def register_raw_update(
    path: Path,
    db_path: str | Path,
) -> None:
    """Register a RAW file change in the processing ledger."""
    register_raw_file(db_path, path)


def watch_raw_directory(
    path: str | Path,
    db_path: str | Path,
) -> Observer:
    """Watch a directory for RAW files and keep the processing ledger updated."""

    raw_directory = Path(path).resolve()

    initialize_ledger(db_path)

    # Reconcile files that already exist when the watcher starts.
    for raw_path in raw_directory.glob("*.raw"):
        register_raw_file(db_path, raw_path)

    return watch_directory(
        directory=raw_directory,
        callback=lambda raw_path: register_raw_update(
            raw_path,
            db_path,
        ),
        pattern="*.raw",
    )


def emit_transect_update_event(path: Path) -> None:
    """Emit a Prefect event when transect definitions change."""

    event = emit_event(
        event=TRANSECT_UPDATE_EVENT,
        resource={
            "prefect.resource.id": TRANSECT_RESOURCE_ID,
            "path": str(path),
        },
        related=[
            {
                "prefect.resource.id": TRANSECT_RELATED_RESOURCE_ID,
                "prefect.resource.name": TRANSECT_RELATED_RESOURCE_ID,
                "prefect.resource.role": "deployment",
            }
        ],
    )

    if event is not None:
        _flush_events()


def register_transect_update(
    path: Path,
    db_path: str | Path,
) -> None:
    """Register completed transects from the CSV in the processing ledger."""

    try:
        df = pd.read_csv(
            path,
            dtype="string",
        )
    except pd.errors.EmptyDataError:
        return

    missing_columns = [
        column
        for column in TRANSECT_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Transect CSV is missing required columns: "
            + ", ".join(missing_columns)
        )

    # Only register transects with both a start and end time.
    completed = df.dropna(
        subset=[
            "transectPart",
            "transectNumber",
            "transectStart",
            "transectEnd",
        ]
    )

    ledger_changed = False

    for _, transect in completed.iterrows():

        start_time = pd.to_datetime(
            transect["transectStart"],
            utc=True,
        )

        end_time = pd.to_datetime(
            transect["transectEnd"],
            utc=True,
        )

        changed = register_transect(
            db_path=db_path,
            transect_part=transect["transectPart"],
            transect_number=transect["transectNumber"],
            start_time=start_time,
            end_time=end_time,
        )

        ledger_changed = ledger_changed or changed

    # Keep the existing Prefect event for now so the current
    # event-driven setup continues to work during the transition.
    if ledger_changed:
        emit_transect_update_event(path)


def watch_transect_file(
    path: str | Path,
    db_path: str | Path,
) -> Observer:
    """Watch the transect CSV and keep the processing ledger updated."""

    transect_path = Path(path).resolve()

    initialize_ledger(db_path)

    # Reconcile transects already present when the watcher starts.
    register_transect_update(
        transect_path,
        db_path,
    )

    return watch_file(
        target_file=transect_path,
        callback=lambda changed_path: register_transect_update(
            changed_path,
            db_path,
        ),
    )