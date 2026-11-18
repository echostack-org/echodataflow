import os
import subprocess
import threading
from pathlib import Path
from unittest.mock import Mock

import pytest

from echodataflow.flows import flows_helper


@pytest.fixture
def fake_rclone(tmp_path, monkeypatch):
    """Provide a controllable fake rclone executable."""
    executable = tmp_path / "rclone"
    executable.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$@" > "$RCLONE_ARGS"\n'
        'echo "upload started" >&2\n'
        'if [ "$RCLONE_MODE" = "slow" ]; then exec sleep 10; fi\n'
        'if [ "$RCLONE_MODE" = "fail" ]; then exit 7; fi\n'
    )
    executable.chmod(0o755)
    args_file = tmp_path / "args.txt"
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("RCLONE_ARGS", str(args_file))
    monkeypatch.setenv("RCLONE_MODE", "success")
    return args_file


@pytest.fixture
def logger(monkeypatch):
    """Replace the Prefect run logger with a mock."""
    logger = Mock()
    monkeypatch.setattr(flows_helper, "get_run_logger", lambda: logger)
    return logger


def test_upload_passes_arguments_and_cleans_exclusions(tmp_path, fake_rclone, logger):
    """Pass upload arguments, log output, and remove the exclusion file."""
    source = tmp_path / "source with spaces"
    source.mkdir()
    # Inspect the exclusion file while rclone is running.
    original_popen = subprocess.Popen
    exclusions = []

    def start(command, **kwargs):
        exclusions.append(Path(command[-1]).read_text())
        return original_popen(command, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(flows_helper.subprocess, "Popen", start)
        flows_helper.flow_file_upload.fn(str(source), "remote:folder with spaces", ["skip"], max_age=3)

    args = fake_rclone.read_text().splitlines()
    assert args[:6] == ["copy", "-v", "--s3-no-check-bucket", "--no-traverse", "--max-age", "3h"]
    assert args[6:9] == [str(source), "remote:folder with spaces", "--exclude-from"]
    assert exclusions == [".DS_Store\nupload_exclude_folders_*.txt\n/skip/**\n"]
    assert not Path(args[-1]).exists()
    logger.info.assert_any_call("%s", "upload started")


def test_upload_logs_before_timeout_and_cleans_up(tmp_path, fake_rclone, logger, monkeypatch):
    """Log output while rclone runs and clean up after a timeout."""
    source = tmp_path / "source"
    source.mkdir()
    monkeypatch.setenv("RCLONE_MODE", "slow")
    logged = threading.Event()
    logger.info.side_effect = lambda message, *args: logged.set() if args == ("upload started",) else None
    errors = []

    def upload():
        try:
            flows_helper.flow_file_upload.fn(str(source), "remote:dest", [], timeout_seconds=1)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=upload)
    thread.start()
    try:
        assert logged.wait(0.8), "rclone output was not logged while it was running"
        assert thread.is_alive()
    finally:
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert len(errors) == 1 and isinstance(errors[0], subprocess.TimeoutExpired)
    assert not list(source.glob("upload_exclude_folders_*.txt"))


def test_upload_failure_raises_and_cleans_up(tmp_path, fake_rclone, logger, monkeypatch):
    """Raise on rclone failure and remove the exclusion file."""
    source = tmp_path / "source"
    source.mkdir()
    monkeypatch.setenv("RCLONE_MODE", "fail")
    with pytest.raises(subprocess.CalledProcessError) as exc:
        flows_helper.flow_file_upload.fn(str(source), "remote:dest", [])
    assert exc.value.returncode == 7
    assert not list(source.glob("upload_exclude_folders_*.txt"))
    assert "--max-age" not in fake_rclone.read_text().splitlines()


def test_invalid_timeout_is_rejected(tmp_path):
    """Reject a nonpositive timeout before starting rclone."""
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        flows_helper.flow_file_upload.fn(str(tmp_path), "remote:dest", [], timeout_seconds=0)
