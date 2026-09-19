"""Reusable Prefect tasks for SPASSO data access."""

from prefect import task

from echodataflow.operations.operations_spasso import (
    SpassoConnectionSettings,
    SpassoDownloadResult,
    SpassoDownloadSettings,
    SpassoRemoteFile,
    download_spasso_file,
    list_spasso_files,
)


@task(log_prints=True)
def task_list_spasso_files(
    connection: SpassoConnectionSettings,
    remote_directory: str,
) -> list[SpassoRemoteFile]:
    """List files available in a SPASSO remote directory."""
    return list_spasso_files(
        connection=connection,
        remote_directory=remote_directory,
    )


@task(log_prints=True)
def task_download_spasso_file(
    connection: SpassoConnectionSettings,
    item: SpassoRemoteFile,
    settings: SpassoDownloadSettings,
) -> SpassoDownloadResult:
    """Download one SPASSO file as a Prefect task."""
    return download_spasso_file(
        connection=connection,
        item=item,
        settings=settings,
    )
