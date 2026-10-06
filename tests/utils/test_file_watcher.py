from pathlib import Path

from echodataflow.utils.file_watcher import FileChangeHandler


class FakeEvent:
    is_directory = False

    def __init__(
        self,
        src_path: Path,
        dest_path: Path | None = None,
    ):
        self.src_path = str(src_path)
        self.dest_path = str(dest_path) if dest_path else str(src_path)


def test_file_change_handler_calls_callback_for_target(tmp_path):
    target = tmp_path / "transect_start_end_time.csv"
    target.touch()

    detected = []

    handler = FileChangeHandler(
        callback=detected.append,
        matches=lambda path: path == target.resolve(),
    )

    handler.on_modified(FakeEvent(target))

    assert detected == [target.resolve()]


def test_file_change_handler_ignores_other_files(tmp_path):
    target = tmp_path / "transect_start_end_time.csv"
    other = tmp_path / "other.csv"

    target.touch()
    other.touch()

    detected = []

    handler = FileChangeHandler(
        callback=detected.append,
        matches=lambda path: path == target.resolve(),
    )

    handler.on_modified(FakeEvent(other))

    assert detected == []


def test_file_change_handler_calls_callback_for_created_target(tmp_path):
    target = tmp_path / "transect_start_end_time.csv"
    detected = []

    handler = FileChangeHandler(
        callback=detected.append,
        matches=lambda path: path == target.resolve(),
    )

    handler.on_created(FakeEvent(target))

    assert detected == [target.resolve()]


def test_file_change_handler_calls_callback_for_moved_target(tmp_path):
    target = tmp_path / "transect_start_end_time.csv"
    temporary = tmp_path / "temporary.csv"
    detected = []

    handler = FileChangeHandler(
        callback=detected.append,
        matches=lambda path: path == target.resolve(),
    )

    handler.on_moved(
        FakeEvent(
            src_path=temporary,
            dest_path=target,
        )
    )

    assert detected == [target.resolve()]


def test_file_change_handler_calls_callback_for_matching_file(tmp_path):
    raw_file = tmp_path / "example.raw"
    detected = []

    handler = FileChangeHandler(
        callback=detected.append,
        matches=lambda path: path.match("*.raw"),
    )

    handler.on_created(FakeEvent(raw_file))

    assert detected == [raw_file.resolve()]


def test_file_change_handler_ignores_nonmatching_file(tmp_path):
    other_file = tmp_path / "example.txt"
    detected = []

    handler = FileChangeHandler(
        callback=detected.append,
        matches=lambda path: path.match("*.raw"),
    )

    handler.on_created(FakeEvent(other_file))

    assert detected == []


def test_file_change_handler_calls_callback_for_moved_matching_file(tmp_path):
    temporary = tmp_path / "temporary.tmp"
    raw_file = tmp_path / "example.raw"
    detected = []

    handler = FileChangeHandler(
        callback=detected.append,
        matches=lambda path: path.match("*.raw"),
    )

    handler.on_moved(
        FakeEvent(
            src_path=temporary,
            dest_path=raw_file,
        )
    )

    assert detected == [raw_file.resolve()]


def test_file_change_handler_logs_callback_exception(tmp_path, caplog):
    target = tmp_path / "transect_start_end_time.csv"

    def failing_callback(_path):
        raise RuntimeError("callback failed")

    handler = FileChangeHandler(
        callback=failing_callback,
        matches=lambda path: path == target.resolve(),
    )

    handler.on_modified(FakeEvent(target))

    assert "Error handling filesystem update" in caplog.text
    assert "callback failed" in caplog.text
