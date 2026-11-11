import sqlite3

from echodataflow.utils.processing_ledger import (
    _database_url,
    get_completed_sv_files,
    get_raw_files_to_process,
    initialize_ledger,
    mark_raw_completed,
    mark_raw_failed,
    mark_raw_processing,
    register_raw_file,
    resolve_database,
)
from concurrent.futures import ThreadPoolExecutor


def test_database_url_from_path(tmp_path):
    db_path = tmp_path / "processing.db"

    url = _database_url(db_path)

    assert url.startswith("sqlite:///")
    assert url.endswith("processing.db")


def test_database_url_preserves_database_url():
    url = "postgresql+psycopg://user:password@localhost/test"

    assert _database_url(url) == url

def test_resolve_database_local_path(tmp_path):
    result = resolve_database(tmp_path, "processing.db")

    assert result == tmp_path / "processing.db"


def test_resolve_database_preserves_database_url():
    url = "postgresql+psycopg://user:password@localhost/test"

    assert resolve_database("/some/path", url) == url

def test_initialize_ledger(tmp_path):
    db_path = tmp_path / "processing.db"

    initialize_ledger(db_path)

    assert db_path.exists()

    with sqlite3.connect(db_path) as conn:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(raw_sv)").fetchall()
        }

    assert columns == {
        "raw_path",
        "raw_filename",
        "file_size",
        "file_mtime_ns",
        "status",
        "sv_filename",
        "first_ping_time",
        "last_ping_time",
        "error",
        "created_at",
        "updated_at",
    }


def test_register_raw_file(tmp_path):
    db_path = tmp_path / "processing.db"
    raw_path = tmp_path / "test.raw"

    raw_path.touch()

    initialize_ledger(db_path)

    assert register_raw_file(db_path, raw_path) is True
    assert register_raw_file(db_path, raw_path) is False

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT raw_path, raw_filename, status
            FROM raw_sv
            """
        ).fetchall()

    assert rows == [
        (str(raw_path), "test.raw", "pending"),
    ]


def test_get_raw_files_to_process(tmp_path):
    db_path = tmp_path / "processing.db"

    raw_a = tmp_path / "a.raw"
    raw_b = tmp_path / "b.raw"

    raw_a.touch()
    raw_b.touch()

    initialize_ledger(db_path)
    register_raw_file(db_path, raw_a)
    register_raw_file(db_path, raw_b)

    result = get_raw_files_to_process(db_path)

    assert result == [raw_a, raw_b]


def test_get_raw_files_to_process_limit(tmp_path):
    db_path = tmp_path / "processing.db"

    raw_a = tmp_path / "a.raw"
    raw_b = tmp_path / "b.raw"

    raw_a.touch()
    raw_b.touch()

    initialize_ledger(db_path)
    register_raw_file(db_path, raw_a)
    register_raw_file(db_path, raw_b)

    result = get_raw_files_to_process(db_path, limit=1)

    assert result == [raw_a]


def test_raw_processing_state_transitions(tmp_path):
    db_path = tmp_path / "processing.db"
    raw_path = tmp_path / "test.raw"

    raw_path.touch()

    initialize_ledger(db_path)
    register_raw_file(db_path, raw_path)

    mark_raw_processing(db_path, raw_path)

    with sqlite3.connect(db_path) as conn:
        status = conn.execute(
            "SELECT status FROM raw_sv WHERE raw_path = ?",
            (str(raw_path),),
        ).fetchone()[0]

    assert status == "processing"

    mark_raw_completed(
        db_path,
        raw_path,
        "test_Sv.zarr",
        "2026-08-14T12:00:00Z",
        "2026-08-14T12:05:00Z",
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT status, sv_filename, first_ping_time, last_ping_time, error
            FROM raw_sv
            WHERE raw_path = ?
            """,
            (str(raw_path),),
        ).fetchone()

    assert row == (
        "completed",
        "test_Sv.zarr",
        "2026-08-14T12:00:00Z",
        "2026-08-14T12:05:00Z",
        "",
    )

    mark_raw_failed(db_path, raw_path, "boom")

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT status, error
            FROM raw_sv
            WHERE raw_path = ?
            """,
            (str(raw_path),),
        ).fetchone()

    assert row == ("failed", "boom")


def test_get_completed_sv_files_by_time_range(tmp_path):
    db_path = tmp_path / "processing.db"

    raw_a = tmp_path / "a.raw"
    raw_b = tmp_path / "b.raw"

    raw_a.touch()
    raw_b.touch()

    initialize_ledger(db_path)

    register_raw_file(db_path, raw_a)
    register_raw_file(db_path, raw_b)

    mark_raw_completed(
        db_path,
        raw_a,
        "a_Sv.zarr",
        "2026-08-14T12:00:00",
        "2026-08-14T12:05:00",
    )

    mark_raw_completed(
        db_path,
        raw_b,
        "b_Sv.zarr",
        "2026-08-14T12:05:00",
        "2026-08-14T12:10:00",
    )

    result = get_completed_sv_files(
        db_path,
        start_time="2026-08-14T12:04:00",
        end_time="2026-08-14T12:06:00",
    )

    assert result == [
        "a_Sv.zarr",
        "b_Sv.zarr",
    ]


def test_register_raw_file_requeues_changed_file(tmp_path):
    db_path = tmp_path / "processing.db"
    raw_path = tmp_path / "test.raw"

    raw_path.write_bytes(b"first")

    initialize_ledger(db_path)

    assert register_raw_file(db_path, raw_path) is True

    mark_raw_completed(
        db_path,
        raw_path,
        "test_Sv.zarr",
        "2026-08-14T12:00:00Z",
        "2026-08-14T12:05:00Z",
    )

    raw_path.write_bytes(b"first plus more data")

    assert register_raw_file(db_path, raw_path) is True

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT status, sv_filename, first_ping_time, last_ping_time
            FROM raw_sv
            WHERE raw_path = ?
            """,
            (str(raw_path),),
        ).fetchone()

    assert row == ("pending", None, None, None)

def test_register_raw_file_large_mtime(tmp_path):
    db_path = tmp_path / "processing.db"
    raw_path = tmp_path / "test.raw"
    raw_path.touch()

    initialize_ledger(db_path)
    register_raw_file(db_path, raw_path)

    # A nanosecond mtime is much larger than a 32-bit integer.
    assert raw_path.stat().st_mtime_ns > 2**31

def test_register_raw_file_concurrently_is_idempotent(tmp_path):
    db_path = tmp_path / "processing.db"
    raw_path = tmp_path / "test.raw"

    raw_path.write_bytes(b"raw-data")
    initialize_ledger(db_path)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(register_raw_file, db_path, raw_path)
            for _ in range(2)
        ]

        results = [future.result() for future in futures]

    assert sorted(results) == [False, True]

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT raw_path, raw_filename, status
            FROM raw_sv
            """
        ).fetchall()

    assert rows == [
        (str(raw_path), "test.raw", "pending"),
    ]