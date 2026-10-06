from types import SimpleNamespace

from echodataflow.flows import flows_integration


def test_flow_ingest_NASC_waits_for_stratum_estimates(monkeypatch, tmp_path):
    messages = []
    monkeypatch.setattr(
        flows_integration,
        "get_run_logger",
        lambda: SimpleNamespace(info=messages.append),
    )
    monkeypatch.setattr(
        flows_integration.s3fs,
        "S3FileSystem",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("S3 should not be accessed")),
    )

    result = flows_integration.flow_ingest_NASC.fn(path_vm_local=str(tmp_path))

    assert result is None
    assert messages == [
        f"Upstream stratum estimates are not ready: {tmp_path / 'stratum_mean.csv'}"
    ]


def test_flow_update_grid_waits_for_all_upstream_inputs(monkeypatch, tmp_path):
    messages = []
    monkeypatch.setattr(
        flows_integration,
        "get_run_logger",
        lambda: SimpleNamespace(info=messages.append),
    )
    monkeypatch.setattr(
        flows_integration.gpd,
        "read_file",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("GeoJSON should not be read")
        ),
    )

    result = flows_integration.flow_update_grid.fn(path_vm_local=str(tmp_path))

    assert result is None
    assert len(messages) == 1
    assert "NASC_all_griddify.geojson" in messages[0]
    assert "stratum_mean.csv" in messages[0]
