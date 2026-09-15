from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from sqlalchemy import (
    BigInteger,
    Column,
    Index,
    MetaData,
    Table,
    Text,
    create_engine,
    insert,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from sqlalchemy.sql import func


metadata = MetaData()

raw_sv = Table(
    "raw_sv",
    metadata,
    Column("raw_path", Text, primary_key=True),
    Column("raw_filename", Text, nullable=False),
    Column("file_size", BigInteger),
    Column("file_mtime_ns", BigInteger),
    Column("status", Text, nullable=False, server_default="pending"),
    Column("sv_filename", Text),
    Column("first_ping_time", Text),
    Column("last_ping_time", Text),
    Column("error", Text, nullable=False, server_default=""),
    Column(
        "created_at",
        Text,
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    Column(
        "updated_at",
        Text,
        nullable=False,
        server_default=func.current_timestamp(),
    ),
)

Index("idx_raw_sv_status", raw_sv.c.status)
Index("idx_raw_sv_first_ping_time", raw_sv.c.first_ping_time)

sv_cps = Table(
    "sv_cps",
    metadata,
    Column("sv_path", Text, primary_key=True),
    Column("sv_filename", Text, nullable=False),
    Column("source_mtime_ns", BigInteger),
    Column("status", Text, nullable=False, server_default="pending"),
    Column("cps_filename", Text),
    Column("first_ping_time", Text),
    Column("last_ping_time", Text),
    Column("error", Text, nullable=False, server_default=""),
    Column(
        "created_at",
        Text,
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    Column(
        "updated_at",
        Text,
        nullable=False,
        server_default=func.current_timestamp(),
    ),
)

Index("idx_sv_cps_status", sv_cps.c.status)
Index("idx_sv_cps_first_ping_time", sv_cps.c.first_ping_time)

def _timestamp_string(value) -> str:
    """Return timestamps in a consistent ISO-8601 representation."""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)

def _zarr_store_mtime_ns(path: str | Path) -> int:
    """Return a stable modification timestamp for a Zarr store."""
    path = Path(path)

    # Consolidated Zarr v2 metadata is rewritten when the store is rewritten.
    metadata_candidates = [
        path / ".zmetadata",
        path / "zarr.json",
        path / ".zgroup",
    ]

    for metadata_path in metadata_candidates:
        if metadata_path.exists():
            return metadata_path.stat().st_mtime_ns

    return path.stat().st_mtime_ns

def _database_url(db_path: str | Path) -> str:
    """Convert a local database path to a SQLAlchemy URL."""

    value = str(db_path)

    # Already a SQLAlchemy database URL, e.g. PostgreSQL.
    if "://" in value:
        return value

    path = Path(value).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    return f"sqlite:///{path.as_posix()}"


@lru_cache
def _get_engine(database: str) -> Engine:
    """Create and cache a SQLAlchemy engine."""

    url = _database_url(database)

    kwargs = {}

    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"timeout": 30}

    return create_engine(url, **kwargs)


def _engine(db_path: str | Path) -> Engine:
    return _get_engine(str(db_path))

def resolve_database(
    path_main: str | Path,
    processing_db: str,
) -> str | Path:
    """Resolve a local database filename or preserve a database URL."""
    if "://" in processing_db:
        return processing_db

    return Path(path_main) / processing_db

def initialize_ledger(db_path: str | Path) -> None:
    """Create the processing ledger database and required tables."""

    engine = _engine(db_path)

    # Keep the existing SQLite concurrency settings.
    if engine.dialect.name == "sqlite":
        with engine.connect() as conn:
            conn.exec_driver_sql("PRAGMA journal_mode=WAL")
            conn.exec_driver_sql("PRAGMA busy_timeout=30000")

    metadata.create_all(engine)


def register_raw_file(
    db_path: str | Path,
    raw_path: str | Path,
) -> bool:
    """Register a RAW file, re-queueing it only when its contents changed.

    Returns True when the ledger changed and downstream processing should
    be notified, otherwise False.
    """

    raw_path = Path(raw_path)
    stat = raw_path.stat()

    engine = _engine(db_path)

    with engine.begin() as conn:
        existing = conn.execute(
            select(
                raw_sv.c.file_size,
                raw_sv.c.file_mtime_ns,
            ).where(raw_sv.c.raw_path == str(raw_path))
        ).first()

        if existing is None:
            values = {
                "raw_path": str(raw_path),
                "raw_filename": raw_path.name,
                "file_size": stat.st_size,
                "file_mtime_ns": stat.st_mtime_ns,
                "status": "pending",
            }

            if engine.dialect.name == "postgresql":
                stmt = (
                    postgresql_insert(raw_sv)
                    .values(**values)
                    .on_conflict_do_nothing(
                        index_elements=[raw_sv.c.raw_path]
                    )
                )
            elif engine.dialect.name == "sqlite":
                stmt = (
                    sqlite_insert(raw_sv)
                    .values(**values)
                    .on_conflict_do_nothing(
                        index_elements=[raw_sv.c.raw_path]
                    )
                )
            else:
                stmt = insert(raw_sv).values(**values)

            result = conn.execute(stmt)

            if result.rowcount == 1:
                return True

            # Another callback registered the same RAW between our
            # SELECT and INSERT. Re-read it instead of failing.
            existing = conn.execute(
                select(
                    raw_sv.c.file_size,
                    raw_sv.c.file_mtime_ns,
                ).where(raw_sv.c.raw_path == str(raw_path))
            ).one()

        if (
            existing.file_size != stat.st_size
            or existing.file_mtime_ns != stat.st_mtime_ns
        ):
            conn.execute(
                update(raw_sv)
                .where(raw_sv.c.raw_path == str(raw_path))
                .values(
                    file_size=stat.st_size,
                    file_mtime_ns=stat.st_mtime_ns,
                    status="pending",
                    sv_filename=None,
                    first_ping_time=None,
                    last_ping_time=None,
                    error="",
                    updated_at=func.current_timestamp(),
                )
            )
            return True

        return False

def get_raw_files_to_process(
    db_path: str | Path,
    limit: int = -1,
) -> list[Path]:
    """Return RAW files that are pending or failed."""

    stmt = (
        select(raw_sv.c.raw_path)
        .where(raw_sv.c.status.in_(("pending", "failed")))
        .order_by(raw_sv.c.created_at, raw_sv.c.raw_path)
    )

    if limit != -1:
        stmt = stmt.limit(limit)

    engine = _engine(db_path)

    with engine.connect() as conn:
        rows = conn.execute(stmt).all()

    return [Path(row.raw_path) for row in rows]


def mark_raw_processing(
    db_path: str | Path,
    raw_path: str | Path,
) -> None:
    """Mark a RAW file as currently being processed."""

    engine = _engine(db_path)

    with engine.begin() as conn:
        conn.execute(
            update(raw_sv)
            .where(raw_sv.c.raw_path == str(Path(raw_path)))
            .values(
                status="processing",
                error="",
                updated_at=func.current_timestamp(),
            )
        )


def mark_raw_completed(
    db_path: str | Path,
    raw_path: str | Path,
    sv_filename: str,
    first_ping_time,
    last_ping_time,
) -> None:
    """Mark a RAW file as successfully converted to Sv."""

    engine = _engine(db_path)

    with engine.begin() as conn:
        conn.execute(
            update(raw_sv)
            .where(raw_sv.c.raw_path == str(Path(raw_path)))
            .values(
                status="completed",
                sv_filename=sv_filename,
                first_ping_time=_timestamp_string(first_ping_time),
                last_ping_time=_timestamp_string(last_ping_time),
                error="",
                updated_at=func.current_timestamp(),
            )
        )


def mark_raw_failed(
    db_path: str | Path,
    raw_path: str | Path,
    error: str,
) -> None:
    """Mark a RAW file as failed."""

    engine = _engine(db_path)

    with engine.begin() as conn:
        conn.execute(
            update(raw_sv)
            .where(raw_sv.c.raw_path == str(Path(raw_path)))
            .values(
                status="failed",
                error=error,
                updated_at=func.current_timestamp(),
            )
        )


def get_completed_sv_files(
    db_path: str | Path,
    start_time=None,
    end_time=None,
) -> list[str]:
    """Return completed Sv files, optionally overlapping a time range."""

    stmt = select(raw_sv.c.sv_filename).where(
        raw_sv.c.status == "completed",
        raw_sv.c.sv_filename.is_not(None),
    )

    if start_time is not None:
        stmt = stmt.where(
            raw_sv.c.last_ping_time >= _timestamp_string(start_time)
        )

    if end_time is not None:
        stmt = stmt.where(
            raw_sv.c.first_ping_time <= _timestamp_string(end_time)
        )

    stmt = stmt.order_by(raw_sv.c.first_ping_time)

    engine = _engine(db_path)

    with engine.connect() as conn:
        rows = conn.execute(stmt).all()

    return [row.sv_filename for row in rows]

def register_sv_file(
    db_path: str | Path,
    sv_path: str | Path,
) -> bool:
    """Register an Sv Zarr store for CPS processing.

    A previously completed Sv store is re-queued when its source Zarr
    metadata modification time changes.

    Returns True when the ledger changed and CPS processing is required.
    """
    sv_path = Path(sv_path).resolve()
    source_mtime_ns = _zarr_store_mtime_ns(sv_path)

    engine = _engine(db_path)

    with engine.begin() as conn:
        existing = conn.execute(
            select(
                sv_cps.c.source_mtime_ns,
            ).where(
                sv_cps.c.sv_path == str(sv_path)
            )
        ).first()

        if existing is None:
            values = {
                "sv_path": str(sv_path),
                "sv_filename": sv_path.name,
                "source_mtime_ns": source_mtime_ns,
                "status": "pending",
            }

            if engine.dialect.name == "postgresql":
                stmt = (
                    postgresql_insert(sv_cps)
                    .values(**values)
                    .on_conflict_do_nothing(
                        index_elements=[sv_cps.c.sv_path]
                    )
                )
            elif engine.dialect.name == "sqlite":
                stmt = (
                    sqlite_insert(sv_cps)
                    .values(**values)
                    .on_conflict_do_nothing(
                        index_elements=[sv_cps.c.sv_path]
                    )
                )
            else:
                stmt = insert(sv_cps).values(**values)

            result = conn.execute(stmt)

            if result.rowcount == 1:
                return True

            existing = conn.execute(
                select(
                    sv_cps.c.source_mtime_ns,
                ).where(
                    sv_cps.c.sv_path == str(sv_path)
                )
            ).one()

        if existing.source_mtime_ns != source_mtime_ns:
            conn.execute(
                update(sv_cps)
                .where(
                    sv_cps.c.sv_path == str(sv_path)
                )
                .values(
                    source_mtime_ns=source_mtime_ns,
                    status="pending",
                    cps_filename=None,
                    first_ping_time=None,
                    last_ping_time=None,
                    error="",
                    updated_at=func.current_timestamp(),
                )
            )
            return True

        return False


def get_sv_files_to_process(
    db_path: str | Path,
    limit: int = -1,
) -> list[Path]:
    """Return Sv stores that are pending or failed CPS processing."""
    stmt = (
        select(sv_cps.c.sv_path)
        .where(
            sv_cps.c.status.in_(("pending", "failed"))
        )
        .order_by(
            sv_cps.c.created_at,
            sv_cps.c.sv_path,
        )
    )

    if limit != -1:
        stmt = stmt.limit(limit)

    engine = _engine(db_path)

    with engine.connect() as conn:
        rows = conn.execute(stmt).all()

    return [Path(row.sv_path) for row in rows]


def mark_sv_cps_processing(
    db_path: str | Path,
    sv_path: str | Path,
) -> None:
    """Mark an Sv store as currently undergoing CPS processing."""
    sv_path = Path(sv_path).resolve()

    engine = _engine(db_path)

    with engine.begin() as conn:
        conn.execute(
            update(sv_cps)
            .where(
                sv_cps.c.sv_path == str(sv_path)
            )
            .values(
                status="processing",
                error="",
                updated_at=func.current_timestamp(),
            )
        )


def mark_sv_cps_completed(
    db_path: str | Path,
    sv_path: str | Path,
    cps_filename: str,
    first_ping_time,
    last_ping_time,
) -> None:
    """Mark CPS processing of an Sv store as completed."""
    sv_path = Path(sv_path).resolve()

    engine = _engine(db_path)

    with engine.begin() as conn:
        conn.execute(
            update(sv_cps)
            .where(
                sv_cps.c.sv_path == str(sv_path)
            )
            .values(
                status="completed",
                cps_filename=cps_filename,
                first_ping_time=_timestamp_string(first_ping_time),
                last_ping_time=_timestamp_string(last_ping_time),
                error="",
                updated_at=func.current_timestamp(),
            )
        )


def mark_sv_cps_failed(
    db_path: str | Path,
    sv_path: str | Path,
    error: str,
) -> None:
    """Mark CPS processing of an Sv store as failed."""
    sv_path = Path(sv_path).resolve()

    engine = _engine(db_path)

    with engine.begin() as conn:
        conn.execute(
            update(sv_cps)
            .where(
                sv_cps.c.sv_path == str(sv_path)
            )
            .values(
                status="failed",
                error=error,
                updated_at=func.current_timestamp(),
            )
        )


def get_completed_cps_files(
    db_path: str | Path,
    start_time=None,
    end_time=None,
) -> list[str]:
    """Return completed CPS products, optionally overlapping a time range."""
    stmt = select(
        sv_cps.c.cps_filename
    ).where(
        sv_cps.c.status == "completed",
        sv_cps.c.cps_filename.is_not(None),
    )

    if start_time is not None:
        stmt = stmt.where(
            sv_cps.c.last_ping_time
            >= _timestamp_string(start_time)
        )

    if end_time is not None:
        stmt = stmt.where(
            sv_cps.c.first_ping_time
            <= _timestamp_string(end_time)
        )

    stmt = stmt.order_by(
        sv_cps.c.first_ping_time
    )

    engine = _engine(db_path)

    with engine.connect() as conn:
        rows = conn.execute(stmt).all()

    return [row.cps_filename for row in rows]