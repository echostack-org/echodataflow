"""SPASSO data-access operations and data contracts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import paramiko


@dataclass(frozen=True)
class SpassoConnectionSettings:
    """Connection settings shared by SPASSO SFTP operations."""

    host: str
    port: int = 2221
    username: str = ""
    password: str = ""


@dataclass(frozen=True)
class SpassoDownloadSettings:
    """Local settings for SPASSO downloads."""

    local_directory: str


@dataclass(frozen=True)
class SpassoRemoteFile:
    """Metadata for one remote SPASSO file."""

    filename: str
    remote_path: str
    size_bytes: int
    mtime: int


@dataclass(frozen=True)
class SpassoDownloadResult:
    """Metadata describing one successfully downloaded SPASSO file."""

    filename: str
    remote_path: str
    local_path: str
    size_bytes: int


def _open_sftp(
    connection: SpassoConnectionSettings,
) -> tuple[paramiko.Transport, paramiko.SFTPClient]:
    """Open an authenticated SFTP connection."""
    transport = paramiko.Transport((connection.host, connection.port))
    transport.connect(
        username=connection.username,
        password=connection.password,
    )
    sftp = paramiko.SFTPClient.from_transport(transport)
    return transport, sftp


def list_spasso_files(
    connection: SpassoConnectionSettings,
    remote_directory: str,
) -> list[SpassoRemoteFile]:
    """List regular files in one SPASSO remote directory."""
    transport, sftp = _open_sftp(connection)
    try:
        rows: list[SpassoRemoteFile] = []
        for entry in sftp.listdir_attr(remote_directory):
            remote_path = f"{remote_directory.rstrip('/')}/{entry.filename}"
            rows.append(
                SpassoRemoteFile(
                    filename=entry.filename,
                    remote_path=remote_path,
                    size_bytes=int(entry.st_size),
                    mtime=int(entry.st_mtime),
                )
            )
        return rows
    finally:
        sftp.close()
        transport.close()


def download_spasso_file(
    connection: SpassoConnectionSettings,
    item: SpassoRemoteFile,
    settings: SpassoDownloadSettings,
) -> SpassoDownloadResult:
    """Download one SPASSO file to the configured local directory."""
    local_dir = Path(settings.local_directory)
    local_dir.mkdir(parents=True, exist_ok=True)

    local_path = local_dir / item.filename

    transport, sftp = _open_sftp(connection)
    try:
        sftp.get(item.remote_path, str(local_path))
    finally:
        sftp.close()
        transport.close()

    return SpassoDownloadResult(
        filename=item.filename,
        remote_path=item.remote_path,
        local_path=str(local_path),
        size_bytes=local_path.stat().st_size,
    )

def download_spasso_asset(url: str, path_main: str) -> list[str]:
    """Download and extract a public SPASSO ZIP into a local directory."""
    import shutil
    import urllib.request
    import zipfile

    root = Path(path_main)
    root.mkdir(parents=True, exist_ok=True)
    archive_path = root / "spasso-products.zip"

    # reuse the downloaded archive on subsequent runs
    if not archive_path.exists():
        temporary = archive_path.with_suffix(".part")
        try:
            with urllib.request.urlopen(url, timeout=120) as response:
                with temporary.open("wb") as output:
                    shutil.copyfileobj(response, output)

            with zipfile.ZipFile(temporary) as archive:
                if archive.testzip() is not None:
                    raise ValueError("Invalid SPASSO archive")

            temporary.replace(archive_path)
        finally:
            temporary.unlink(missing_ok=True)

    with zipfile.ZipFile(archive_path) as archive:
        members = [item for item in archive.infolist() if not item.is_dir()]
        if not members:
            raise ValueError("Empty SPASSO archive")

        for item in members:
            if (
                "/" in item.filename
                or "\\" in item.filename
                or not item.filename.endswith(".nc")
            ):
                raise ValueError(f"Unexpected archive entry: {item.filename}")

        for item in members:
            target = root / item.filename
            if target.exists() and target.stat().st_size == item.file_size:
                continue

            temporary = target.with_suffix(".part")
            try:
                with archive.open(item) as source, temporary.open("wb") as output:
                    shutil.copyfileobj(source, output)
                temporary.replace(target)
            finally:
                temporary.unlink(missing_ok=True)

    return [str(root / item.filename) for item in members]