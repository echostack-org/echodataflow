from echodataflow.operations import operations_watchdog


def test_watch_raw_directory(monkeypatch, tmp_path):
    called = {}
    db_path = tmp_path / "processing.db"

    def fake_initialize_ledger(path):
        called["db_path"] = path

    def fake_watch_directory(
        directory,
        callback,
        pattern,
    ):
        called["directory"] = directory
        called["callback"] = callback
        called["pattern"] = pattern
        return "observer"

    monkeypatch.setattr(
        operations_watchdog,
        "initialize_ledger",
        fake_initialize_ledger,
    )

    monkeypatch.setattr(
        operations_watchdog,
        "watch_directory",
        fake_watch_directory,
    )

    result = operations_watchdog.watch_raw_directory(
        tmp_path,
        db_path,
    )

    assert result == "observer"
    assert called["directory"] == tmp_path.resolve()
    assert called["db_path"] == db_path
    assert called["pattern"] == "*.raw"


def test_register_raw_update(monkeypatch, tmp_path):
    raw_file = tmp_path / "example.raw"
    db_path = tmp_path / "processing.db"

    called = {}

    def fake_register_raw_file(db, path):
        called["registered"] = (db, path)

    monkeypatch.setattr(
        operations_watchdog,
        "register_raw_file",
        fake_register_raw_file,
    )

    operations_watchdog.register_raw_update(
        raw_file,
        db_path,
    )

    assert called["registered"] == (db_path, raw_file)


def test_watch_raw_directory_reconciles_existing_raw_files(
    monkeypatch,
    tmp_path,
):
    raw_a = tmp_path / "a.raw"
    raw_b = tmp_path / "b.raw"

    raw_a.touch()
    raw_b.touch()

    db_path = tmp_path / "processing.db"

    registered = []

    monkeypatch.setattr(
        operations_watchdog,
        "register_raw_file",
        lambda db, path: registered.append((db, path)),
    )

    monkeypatch.setattr(
        operations_watchdog,
        "watch_directory",
        lambda **kwargs: "observer",
    )

    result = operations_watchdog.watch_raw_directory(
        tmp_path,
        db_path,
    )

    assert result == "observer"

    assert {path for _, path in registered} == {
        raw_a.resolve(),
        raw_b.resolve(),
    }


def test_emit_transect_update_event(monkeypatch, tmp_path):
    target = tmp_path / "transect_start_end_time.csv"
    target.touch()

    emitted = {}

    def fake_emit_event(**kwargs):
        emitted.update(kwargs)

    monkeypatch.setattr(
        operations_watchdog,
        "emit_event",
        fake_emit_event,
    )

    operations_watchdog.emit_transect_update_event(target)

    assert emitted["event"] == "echodataflow.transect.updated"
    assert (
        emitted["resource"]["prefect.resource.id"]
        == "transect-start-end-time"
    )
    assert emitted["resource"]["path"] == str(target)