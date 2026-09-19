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
