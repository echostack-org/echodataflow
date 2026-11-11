from unittest.mock import MagicMock

import pytest

import echodataflow.flows.flows_spasso as flows_spasso
from echodataflow.operations.operations_spasso import (
    SpassoDownloadResult,
    SpassoRemoteFile,
)


def remote_file(
    filename: str,
    size: int = 100,
) -> SpassoRemoteFile:
    """Create a remote SPASSO file for testing."""

    return SpassoRemoteFile(
        filename=filename,
        remote_path=f"remote/{filename}",
        size_bytes=size,
        mtime=1000,
    )


@pytest.fixture
def mock_prefect(monkeypatch):
    """Mock Prefect runtime dependencies."""

    secret = MagicMock()
    secret.get.return_value = "test-value"

    monkeypatch.setattr(
        flows_spasso.Secret,
        "load",
        MagicMock(return_value=secret),
    )

    logger = MagicMock()

    monkeypatch.setattr(
        flows_spasso,
        "get_run_logger",
        MagicMock(return_value=logger),
    )

    return logger


def configure_remote_listing(
    monkeypatch,
    files,
):
    """Mock the Prefect task used to list remote files."""

    list_task = MagicMock()
    configured_task = MagicMock()

    list_task.with_options.return_value = configured_task
    configured_task.return_value = files

    monkeypatch.setattr(
        flows_spasso,
        "task_list_spasso_files",
        list_task,
    )

    return list_task


def test_requires_product_patterns(
    monkeypatch,
    mock_prefect,
    tmp_path,
):
    """At least one requested product family is required."""

    configure_remote_listing(
        monkeypatch,
        [],
    )

    with pytest.raises(
        ValueError,
        match="At least one SPASSO product",
    ):
        flows_spasso.flow_fetch_spasso.fn(
            host="example.org",
            path_main=str(tmp_path),
            product_patterns=[],
        )


def test_selects_latest_file_for_each_product(
    monkeypatch,
    mock_prefect,
    tmp_path,
):
    """Latest products are selected independently by date."""

    files = [
        remote_file(
            "20260915_Copernicus_PHY.nc"
        ),
        remote_file(
            "20260917_Copernicus_PHY.nc"
        ),
        remote_file(
            "20260914_Copernicus_SST_L4.nc"
        ),
        remote_file(
            "20260916_Copernicus_SST_L4.nc"
        ),
    ]

    configure_remote_listing(
        monkeypatch,
        files,
    )

    submitted = []

    download_task = MagicMock()
    configured_download = MagicMock()

    download_task.with_options.return_value = (
        configured_download
    )

    def fake_submit(connection, item, settings):
        submitted.append(item)

        future = MagicMock()
        future.result.return_value = SpassoDownloadResult(
            filename=item.filename,
            remote_path=item.remote_path,
            local_path=str(
                tmp_path / item.filename
            ),
            size_bytes=item.size_bytes,
        )

        return future

    configured_download.submit.side_effect = fake_submit

    monkeypatch.setattr(
        flows_spasso,
        "task_download_spasso_file",
        download_task,
    )

    monkeypatch.setattr(
        flows_spasso,
        "as_completed",
        lambda futures: list(futures),
    )

    flows_spasso.flow_fetch_spasso.fn(
        host="example.org",
        path_main=str(tmp_path),
        product_patterns=[
            "Copernicus_PHY",
            "Copernicus_SST_L4",
        ],
        task_retries=0,
    )

    assert {
        item.filename
        for item in submitted
    } == {
        "20260917_Copernicus_PHY.nc",
        "20260916_Copernicus_SST_L4.nc",
    }


def test_product_matching_is_exact(
    monkeypatch,
    mock_prefect,
    tmp_path,
):
    """
    Copernicus_PHY must not match FTLE, KE, OW,
    or LLADV products.
    """

    files = [
        remote_file(
            "20260915_Copernicus_PHY.nc"
        ),
        remote_file(
            "20260917_FTLE_Copernicus_PHY.nc"
        ),
        remote_file(
            "20260917_KE_Copernicus_PHY.nc"
        ),
        remote_file(
            "20260917_OW_Copernicus_PHY.nc"
        ),
        remote_file(
            "20260917_LLADV_Copernicus_PHY.nc"
        ),
    ]

    configure_remote_listing(
        monkeypatch,
        files,
    )

    submitted = []

    download_task = MagicMock()
    configured_download = MagicMock()

    download_task.with_options.return_value = (
        configured_download
    )

    def fake_submit(connection, item, settings):
        submitted.append(item)

        future = MagicMock()
        future.result.return_value = SpassoDownloadResult(
            filename=item.filename,
            remote_path=item.remote_path,
            local_path=str(
                tmp_path / item.filename
            ),
            size_bytes=item.size_bytes,
        )

        return future

    configured_download.submit.side_effect = fake_submit

    monkeypatch.setattr(
        flows_spasso,
        "task_download_spasso_file",
        download_task,
    )

    monkeypatch.setattr(
        flows_spasso,
        "as_completed",
        lambda futures: list(futures),
    )

    flows_spasso.flow_fetch_spasso.fn(
        host="example.org",
        path_main=str(tmp_path),
        product_patterns=["Copernicus_PHY"],
        task_retries=0,
    )

    assert len(submitted) == 1
    assert (
        submitted[0].filename
        == "20260915_Copernicus_PHY.nc"
    )


def test_existing_file_with_same_size_is_skipped(
    monkeypatch,
    mock_prefect,
    tmp_path,
):
    """Do not download a file already present at the same size."""

    filename = "20260917_Copernicus_PHY.nc"

    local_file = tmp_path / filename
    local_file.write_bytes(b"12345")

    files = [
        remote_file(
            filename,
            size=5,
        )
    ]

    configure_remote_listing(
        monkeypatch,
        files,
    )

    download_task = MagicMock()

    monkeypatch.setattr(
        flows_spasso,
        "task_download_spasso_file",
        download_task,
    )

    flows_spasso.flow_fetch_spasso.fn(
        host="example.org",
        path_main=str(tmp_path),
        product_patterns=["Copernicus_PHY"],
        task_retries=0,
    )

    download_task.with_options.assert_not_called()


def test_existing_file_with_different_size_is_downloaded(
    monkeypatch,
    mock_prefect,
    tmp_path,
):
    """Redownload an existing file when its size differs."""

    filename = "20260917_Copernicus_PHY.nc"

    local_file = tmp_path / filename
    local_file.write_bytes(b"old")

    files = [
        remote_file(
            filename,
            size=100,
        )
    ]

    configure_remote_listing(
        monkeypatch,
        files,
    )

    download_task = MagicMock()
    configured_download = MagicMock()

    download_task.with_options.return_value = (
        configured_download
    )

    future = MagicMock()
    future.result.return_value = SpassoDownloadResult(
        filename=filename,
        remote_path=f"remote/{filename}",
        local_path=str(local_file),
        size_bytes=100,
    )

    configured_download.submit.return_value = future

    monkeypatch.setattr(
        flows_spasso,
        "task_download_spasso_file",
        download_task,
    )

    monkeypatch.setattr(
        flows_spasso,
        "as_completed",
        lambda futures: list(futures),
    )

    flows_spasso.flow_fetch_spasso.fn(
        host="example.org",
        path_main=str(tmp_path),
        product_patterns=["Copernicus_PHY"],
        task_retries=0,
    )

    configured_download.submit.assert_called_once()


def test_missing_product_does_not_fail_flow(
    monkeypatch,
    mock_prefect,
    tmp_path,
):
    """A product not yet available remotely is skipped."""

    configure_remote_listing(
        monkeypatch,
        [
            remote_file(
                "20260917_Copernicus_PHY.nc"
            )
        ],
    )

    download_task = MagicMock()
    configured_download = MagicMock()

    download_task.with_options.return_value = (
        configured_download
    )

    future = MagicMock()
    future.result.return_value = SpassoDownloadResult(
        filename="20260917_Copernicus_PHY.nc",
        remote_path=(
            "remote/20260917_Copernicus_PHY.nc"
        ),
        local_path=str(
            tmp_path / "20260917_Copernicus_PHY.nc"
        ),
        size_bytes=100,
    )

    configured_download.submit.return_value = future

    monkeypatch.setattr(
        flows_spasso,
        "task_download_spasso_file",
        download_task,
    )

    monkeypatch.setattr(
        flows_spasso,
        "as_completed",
        lambda futures: list(futures),
    )

    flows_spasso.flow_fetch_spasso.fn(
        host="example.org",
        path_main=str(tmp_path),
        product_patterns=[
            "Copernicus_PHY",
            "Copernicus_SSS_L4",
        ],
        task_retries=0,
    )

    configured_download.submit.assert_called_once()


def test_no_requested_products_found_returns_cleanly(
    monkeypatch,
    mock_prefect,
    tmp_path,
):
    """No matching remote products is a valid no-op."""

    configure_remote_listing(
        monkeypatch,
        [
            remote_file(
                "20260917_SOMETHING_ELSE.nc"
            )
        ],
    )

    download_task = MagicMock()

    monkeypatch.setattr(
        flows_spasso,
        "task_download_spasso_file",
        download_task,
    )

    result = flows_spasso.flow_fetch_spasso.fn(
        host="example.org",
        path_main=str(tmp_path),
        product_patterns=["Copernicus_PHY"],
        task_retries=0,
    )

    assert result is None
    download_task.with_options.assert_not_called()


def test_download_failure_fails_flow(
    monkeypatch,
    mock_prefect,
    tmp_path,
):
    """A failed download must make the flow fail."""

    filename = "20260917_Copernicus_PHY.nc"

    configure_remote_listing(
        monkeypatch,
        [
            remote_file(filename)
        ],
    )

    download_task = MagicMock()
    configured_download = MagicMock()

    download_task.with_options.return_value = (
        configured_download
    )

    future = MagicMock()
    future.result.side_effect = RuntimeError(
        "download failed"
    )

    configured_download.submit.return_value = future

    monkeypatch.setattr(
        flows_spasso,
        "task_download_spasso_file",
        download_task,
    )

    monkeypatch.setattr(
        flows_spasso,
        "as_completed",
        lambda futures: list(futures),
    )

    with pytest.raises(
        RuntimeError,
        match="1 SPASSO download",
    ):
        flows_spasso.flow_fetch_spasso.fn(
            host="example.org",
            path_main=str(tmp_path),
            product_patterns=["Copernicus_PHY"],
            task_retries=0,
        )
