from types import SimpleNamespace
from unittest.mock import MagicMock

from echodataflow.operations.operations_spasso import (
    SpassoConnectionSettings,
    SpassoDownloadSettings,
    SpassoRemoteFile,
    download_spasso_file,
    list_spasso_files,
)


def test_list_spasso_files(monkeypatch):
    """Remote SFTP entries are converted to SpassoRemoteFile objects."""

    transport = MagicMock()
    sftp = MagicMock()

    sftp.listdir_attr.return_value = [
        SimpleNamespace(
            filename="20260917_Copernicus_PHY.nc",
            st_size=100,
            st_mtime=1000,
        ),
        SimpleNamespace(
            filename="20260916_Copernicus_SST_L4.nc",
            st_size=200,
            st_mtime=2000,
        ),
    ]

    monkeypatch.setattr(
        "echodataflow.operations.operations_spasso._open_sftp",
        lambda connection: (transport, sftp),
    )

    connection = SpassoConnectionSettings(
        host="example.org",
        username="user",
        password="password",
    )

    files = list_spasso_files(
        connection,
        "data/Cruises/test/Processed",
    )

    assert len(files) == 2

    assert files[0] == SpassoRemoteFile(
        filename="20260917_Copernicus_PHY.nc",
        remote_path=(
            "data/Cruises/test/Processed/"
            "20260917_Copernicus_PHY.nc"
        ),
        size_bytes=100,
        mtime=1000,
    )

    assert files[1].filename == "20260916_Copernicus_SST_L4.nc"
    assert files[1].size_bytes == 200
    assert files[1].mtime == 2000

    sftp.listdir_attr.assert_called_once_with(
        "data/Cruises/test/Processed"
    )

    sftp.close.assert_called_once()
    transport.close.assert_called_once()


def test_list_spasso_files_closes_connection_on_error(monkeypatch):
    """SFTP resources are closed even when listing fails."""

    transport = MagicMock()
    sftp = MagicMock()

    sftp.listdir_attr.side_effect = RuntimeError("SFTP listing failed")

    monkeypatch.setattr(
        "echodataflow.operations.operations_spasso._open_sftp",
        lambda connection: (transport, sftp),
    )

    connection = SpassoConnectionSettings(
        host="example.org",
        username="user",
        password="password",
    )

    try:
        list_spasso_files(connection, "remote")
    except RuntimeError as exc:
        assert str(exc) == "SFTP listing failed"
    else:
        raise AssertionError("Expected RuntimeError")

    sftp.close.assert_called_once()
    transport.close.assert_called_once()


def test_download_spasso_file(monkeypatch, tmp_path):
    """A remote SPASSO file is downloaded to the configured directory."""

    transport = MagicMock()
    sftp = MagicMock()

    monkeypatch.setattr(
        "echodataflow.operations.operations_spasso._open_sftp",
        lambda connection: (transport, sftp),
    )

    def fake_get(remote_path, local_path):
        assert remote_path == (
            "remote/20260917_Copernicus_PHY.nc"
        )

        with open(local_path, "wb") as file:
            file.write(b"spasso-data")

    sftp.get.side_effect = fake_get

    connection = SpassoConnectionSettings(
        host="example.org",
        username="user",
        password="password",
    )

    item = SpassoRemoteFile(
        filename="20260917_Copernicus_PHY.nc",
        remote_path="remote/20260917_Copernicus_PHY.nc",
        size_bytes=11,
        mtime=1000,
    )

    settings = SpassoDownloadSettings(
        remote_directory="remote",
        local_directory=str(tmp_path),
    )

    result = download_spasso_file(
        connection,
        item,
        settings,
    )

    expected_path = (
        tmp_path / "20260917_Copernicus_PHY.nc"
    )

    assert expected_path.exists()
    assert expected_path.read_bytes() == b"spasso-data"

    assert result.filename == item.filename
    assert result.remote_path == item.remote_path
    assert result.local_path == str(expected_path)
    assert result.size_bytes == 11

    sftp.get.assert_called_once_with(
        item.remote_path,
        str(expected_path),
    )

    sftp.close.assert_called_once()
    transport.close.assert_called_once()


def test_download_spasso_file_closes_connection_on_error(
    monkeypatch,
    tmp_path,
):
    """SFTP resources are closed even when a download fails."""

    transport = MagicMock()
    sftp = MagicMock()

    sftp.get.side_effect = RuntimeError("download failed")

    monkeypatch.setattr(
        "echodataflow.operations.operations_spasso._open_sftp",
        lambda connection: (transport, sftp),
    )

    connection = SpassoConnectionSettings(
        host="example.org",
        username="user",
        password="password",
    )

    item = SpassoRemoteFile(
        filename="20260917_Copernicus_PHY.nc",
        remote_path="remote/20260917_Copernicus_PHY.nc",
        size_bytes=100,
        mtime=1000,
    )

    settings = SpassoDownloadSettings(
        remote_directory="remote",
        local_directory=str(tmp_path),
    )

    try:
        download_spasso_file(
            connection,
            item,
            settings,
        )
    except RuntimeError as exc:
        assert str(exc) == "download failed"
    else:
        raise AssertionError("Expected RuntimeError")

    sftp.close.assert_called_once()
    transport.close.assert_called_once()