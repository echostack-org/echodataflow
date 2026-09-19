"""Prefect flows for retrieving SPASSO products."""

from __future__ import annotations

import re
from pathlib import Path

from prefect import flow, get_run_logger
from prefect.blocks.system import Secret
from prefect.futures import as_completed

from echodataflow.deployment.task_runners import task_runner_from_environment
from echodataflow.operations.operations_spasso import (
    SpassoConnectionSettings,
    SpassoDownloadSettings,
)
from echodataflow.tasks.tasks_spasso import (
    task_download_spasso_file,
    task_list_spasso_files,
)


@flow(log_prints=True, task_runner=task_runner_from_environment())
def flow_fetch_spasso(
    host: str,
    port: int = 2221,
    remote_path: str = "",
    path_main: str = "",
    product_patterns: list[str] | None = None,
    username_secret: str = "spasso-username",
    password_secret: str = "spasso-password",
    task_retries: int = 3,
    task_retry_delay_seconds: int = 30,
) -> None:
    """Download the latest available SPASSO file for each requested product."""

    logger = get_run_logger()

    # ------------------------------------------------------------------
    # Credentials
    # ------------------------------------------------------------------

    username = Secret.load(username_secret).get()
    password = Secret.load(password_secret).get()

    # ------------------------------------------------------------------
    # Local output directory
    # ------------------------------------------------------------------

    local_dir = Path(path_main)
    local_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # SFTP connection settings
    # ------------------------------------------------------------------

    connection = SpassoConnectionSettings(
        host=host,
        port=port,
        username=username,
        password=password,
    )

    # ------------------------------------------------------------------
    # List remote SPASSO files
    # ------------------------------------------------------------------

    remote_files = task_list_spasso_files.with_options(
        retries=task_retries,
        retry_delay_seconds=task_retry_delay_seconds,
    )(
        connection,
        remote_path,
    )

    logger.info(
        "Found %d remote SPASSO file(s).",
        len(remote_files),
    )

    # ------------------------------------------------------------------
    # Select latest file independently for each requested product family
    #
    # Example:
    #
    #   Copernicus_PHY
    #       -> 20260915_Copernicus_PHY.nc
    #
    #   Copernicus_SST_L4
    #       -> 20260914_Copernicus_SST_L4.nc
    #
    #   Copernicus_SSS_L4
    #       -> 20260908_Copernicus_SSS_L4.nc
    #
    # This intentionally allows products to have different dates.
    # ------------------------------------------------------------------

    if not product_patterns:
        raise ValueError(
            "At least one SPASSO product must be provided in "
            "'product_patterns'."
        )

    selected = []

    for product in product_patterns:

        # Exact product-family matching.
        #
        # This is important because "Copernicus_PHY" should match:
        #
        #   20260915_Copernicus_PHY.nc
        #
        # but NOT:
        #
        #   20260915_FTLE_Copernicus_PHY.nc
        #   20260915_OW_Copernicus_PHY.nc
        #   20260915_KE_Copernicus_PHY.nc
        #
        pattern = re.compile(
            rf"^\d{{8}}_{re.escape(product)}\.nc$"
        )

        matches = [
            item
            for item in remote_files
            if pattern.fullmatch(item.filename)
        ]

        if not matches:
            logger.warning(
                "No remote SPASSO file found for product %r.",
                product,
            )
            continue

        # Filenames start with YYYYMMDD, so lexical ordering gives us
        # the newest product date.
        latest = max(
            matches,
            key=lambda item: item.filename[:8],
        )

        logger.info(
            "Latest %-25s -> %s",
            product,
            latest.filename,
        )

        selected.append(latest)

    if not selected:
        logger.warning(
            "None of the requested SPASSO products were found."
        )
        return

    # ------------------------------------------------------------------
    # Remove duplicate selections, just in case.
    # ------------------------------------------------------------------

    selected = list(
        {
            item.filename: item
            for item in selected
        }.values()
    )

    # ------------------------------------------------------------------
    # Check which selected files already exist locally.
    #
    # If filename AND size match, there is nothing to download.
    # If the file is absent or its size differs, download it.
    # ------------------------------------------------------------------

    existing = {
        path.name: path.stat().st_size
        for path in local_dir.iterdir()
        if path.is_file()
    }

    to_download = [
        item
        for item in selected
        if existing.get(item.filename) != item.size_bytes
    ]

    logger.info(
        "Selected %d latest product file(s); "
        "%d require download.",
        len(selected),
        len(to_download),
    )

    if not to_download:
        logger.info(
            "All latest SPASSO products are already available locally."
        )
        return

    # ------------------------------------------------------------------
    # Download settings
    # ------------------------------------------------------------------

    settings = SpassoDownloadSettings(
        remote_directory=remote_path,
        local_directory=str(local_dir),
    )

    # ------------------------------------------------------------------
    # Submit downloads concurrently through Prefect
    # ------------------------------------------------------------------

    futures = {}

    for item in to_download:

        future = task_download_spasso_file.with_options(
            task_run_name=f"spasso_{item.filename}",
            retries=task_retries,
            retry_delay_seconds=task_retry_delay_seconds,
        ).submit(
            connection,
            item,
            settings,
        )

        futures[future] = item

    # ------------------------------------------------------------------
    # Collect results
    # ------------------------------------------------------------------

    errors = []

    for future in as_completed(futures):

        item = futures[future]

        try:
            result = future.result()

            logger.info(
                "Downloaded %s -> %s",
                result.remote_path,
                result.local_path,
            )

        except Exception as exc:
            errors.append(exc)

            logger.error(
                "Failed to download %s: %s",
                item.remote_path,
                exc,
            )

    # ------------------------------------------------------------------
    # Fail the flow if one or more downloads failed.
    # ------------------------------------------------------------------

    if errors:
        raise RuntimeError(
            f"{len(errors)} SPASSO download(s) failed "
            f"out of {len(to_download)}."
        )