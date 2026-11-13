from unittest.mock import Mock

from echodataflow.flows import flows_viz_cloud


def test_cps_cache_flow_delegates_to_transect_preparation(tmp_path, monkeypatch):
    source = tmp_path / "CPS_Sv"
    destination = tmp_path / "viz_cache_CPS"
    database = tmp_path / "processing.db"

    prepare = Mock(return_value=["001"])
    resolve = Mock(return_value=database)
    monkeypatch.setattr(flows_viz_cloud, "prepare_transect_caches", prepare)
    monkeypatch.setattr(flows_viz_cloud, "resolve_database", resolve)

    result = flows_viz_cloud.flow_update_cache_CPS.fn(
        path_CPS=str(source),
        path_cache=str(destination),
    )

    resolve.assert_called_once_with(source.parent, "processing.db")
    prepare.assert_called_once_with(source, destination, database)
    assert result == ["001"]
