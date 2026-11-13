"""Prepare generic Echoshader display products from completed local datasets.

Run in the rendering environment (Echoshader installed). This flow never reads
raw files or changes source datasets. Configure serialized deployment execution.
"""

from pathlib import Path

from prefect import flow, get_run_logger


@flow(name="flow-update-display")
def flow_update_display(
    path_source: str,
    path_display: str,
    display_products: dict,
    pattern: str = "*.zarr",
    path_CPS: str | None = None,
    processing_db: str = "processing.db",
):
    from echoshader.display.prepare import prepare_display

    source, destination = Path(path_source).resolve(), Path(path_display).resolve()
    # Validate roots before discovering files or creating output folders.
    if (
        source == destination
        or source in destination.parents
        or destination in source.parents
    ):
        raise ValueError("Source and display roots must be separate.")
    logger = get_run_logger()
    if path_CPS is not None:
        from echodataflow.operations.cps_transect_cache import prepare_transect_caches
        from echodataflow.utils.processing_ledger import resolve_database

        if source.name != "transects":
            raise ValueError("CPS assembly requires path_source ending in transects.")
        cps = Path(path_CPS).resolve()
        prepare_transect_caches(
            cps, source.parent, resolve_database(cps.parent, processing_db)
        )
    changed = 0
    for path in sorted(source.glob(pattern)):
        if ".tmp-" in path.name or path.name.endswith("_preview.zarr"):
            continue  # Writer staging and legacy previews are never source products.
        logger.info("Preparing display products: %s", path.name)
        changed += prepare_display(
            path, destination / (path.stem + ".display"), display_products
        )
    logger.info("Published %d changed display datasets.", changed)
    return changed
