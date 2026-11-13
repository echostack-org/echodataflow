"""Exercise orchestration without starting a Prefect server."""

import logging

import pytest
from echodataflow.flows import flows_display
from echoshader.display import prepare


def test_only_completed_sources_are_offered(tmp_path, monkeypatch):
    source, output = tmp_path / "sources", tmp_path / "display"
    source.mkdir()
    for name in [
        "transect_001.zarr",
        "transect_001_preview.zarr",
        "transect_002.zarr.tmp-abc",
        "transect_003.zarr",
    ]:
        (source / name).mkdir()
    calls = []
    monkeypatch.setattr(
        flows_display, "get_run_logger", lambda: logging.getLogger(__name__)
    )
    monkeypatch.setattr(
        prepare,
        "prepare_display",
        lambda path, destination, settings: (
            calls.append((path.name, destination.name)) or True
        ),
    )
    assert flows_display.flow_update_display.fn(str(source), str(output), {}) == 2
    assert calls == [
        ("transect_001.zarr", "transect_001.display"),
        ("transect_003.zarr", "transect_003.display"),
    ]


def test_source_and_display_must_be_separate(tmp_path):
    with pytest.raises(ValueError, match="separate"):
        flows_display.flow_update_display.fn(
            str(tmp_path), str(tmp_path / "display"), {}
        )
