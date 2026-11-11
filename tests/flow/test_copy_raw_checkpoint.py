"""Replay logic with isolated Prefect/S3 boundaries; no network or server needed."""

import ast
import datetime
import hashlib
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest


@pytest.fixture
def replay(tmp_path):
    path = Path(__file__).resolve().parents[2] / "src/echodataflow/flows/flows_simulation.py"
    tree = ast.parse(path.read_text())
    tree.body = [node for node in tree.body if not isinstance(node, (ast.Import, ast.ImportFrom))]
    calls = []

    class Variables:
        @staticmethod
        def get(*args, **kwargs):
            raise AssertionError("Batch replay must not use Prefect variables")

        set = get

    class Task:
        def with_options(self, **kwargs):
            return self

        def __call__(self, work, settings):
            calls.append(work.s3_path)
            Path(work.local_path).write_bytes(b"raw")
            return work.local_path

    scope = dict(
        datetime=datetime,
        hashlib=hashlib,
        json=json,
        re=re,
        Path=Path,
        pd=pd,
        runtime=SimpleNamespace(deployment=SimpleNamespace(id="demo")),
        flow=lambda **kw: lambda fn: fn,
        Variable=Variables,
        S3CopyResult=str,
        S3CopySettings=SimpleNamespace,
        S3CopyWorkItem=SimpleNamespace,
        task_copy_s3_file=Task(),
    )
    exec(compile(tree, str(path), "exec"), scope)

    def run(count=10, size=True):
        rows = [
            {
                "s3_path": "data/" + "long-prefix/" * 12 + f"file_{i:03}.raw",
                "timestamp": f"2024-01-01T00:{i // 60:02}:{i % 60:02}Z",
                **({"size": 3} if size else {}),
            }
            for i in range(count)
        ]
        manifest = tmp_path / "raw.csv"
        pd.DataFrame(rows).to_csv(manifest, index=False)
        return rows, dict(
            path_raw_list=str(manifest),
            path_copy=str(tmp_path / "raw"),
            files_per_run=10,
            endpoint_url="https://s3.amazonaws.com",
        )

    return scope, calls, run, tmp_path


def test_93_files_resume_without_variable_size_limit(replay):
    scope, calls, setup, root = replay
    rows, options = setup(93)
    out = root / "raw"
    out.mkdir()
    for row in rows[:76]:
        (out / Path(row["s3_path"]).name).write_bytes(b"raw")
    assert len(scope["flow_copy_raw"](**options)) == 10
    assert len(scope["flow_copy_raw"](**options)) == 7
    assert scope["flow_copy_raw"](**options) == []
    assert calls == [row["s3_path"] for row in rows[76:]]
    checkpoint = next(out.glob(".copy_raw_progress_*.json"))
    assert len(json.loads(checkpoint.read_text())) == 17
    # A checkpoint larger than Prefect's limit is also supported.
    state = {row["s3_path"]: 3 for row in rows}
    scope["_save_batch_progress"](checkpoint, state)
    assert checkpoint.stat().st_size > 5000
    assert scope["flow_copy_raw"](**options) == []


def test_wrong_size_redownloaded_and_missing_file_not_trusted(replay):
    scope, calls, setup, root = replay
    rows, options = setup(2)
    scope["flow_copy_raw"](**options)
    out = root / "raw"
    (out / "file_000.raw").write_bytes(b"partial")
    (out / "file_001.raw").unlink()
    scope["flow_copy_raw"](**options)
    assert len(calls) == 4


def test_manifests_without_size_resume_from_local_checkpoint(replay):
    scope, calls, setup, root = replay
    rows, options = setup(2, size=False)
    scope["flow_copy_raw"](**options)
    assert scope["flow_copy_raw"](**options) == []
    assert len(calls) == 2


def test_download_survives_checkpoint_write_failure(replay):
    scope, calls, setup, root = replay
    rows, options = setup(2)
    save = scope["_save_batch_progress"]

    def failed(*args):
        raise OSError("checkpoint unavailable")

    scope["_save_batch_progress"] = failed
    with pytest.raises(OSError):
        scope["flow_copy_raw"](**options)
    scope["_save_batch_progress"] = save
    scope["flow_copy_raw"](**options)
    assert calls == [row["s3_path"] for row in rows]


def test_timestamp_mode_keeps_existing_watermark_behavior(replay):
    scope, calls, setup, root = replay
    rows, options = setup(2)
    updates = []
    scope["Variable"] = SimpleNamespace(
        get=lambda *a, **k: "2024-01-01T00:00:00Z",
        set=lambda key, value, **kw: updates.append((key, value)),
    )
    options["files_per_run"] = None
    scope["flow_copy_raw"](**options)
    assert calls == [rows[1]["s3_path"]]
    assert updates[0][0] == "prev_start_time_demo"
    assert not list((root / "raw").glob(".copy_raw_progress_*.json"))
