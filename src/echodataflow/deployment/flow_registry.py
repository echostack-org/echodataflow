"""Curated registry of flows that may be referenced by deployment recipes."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FlowRegistration:
    """Location and optional documentation for one deployable flow."""

    entrypoint: str
    description: str | None = None


FLOW_REGISTRY: dict[str, FlowRegistration] = {
    "raw2Sv": FlowRegistration(
        entrypoint="echodataflow/flows/flows_acoustics.py:flow_raw2Sv",
        description="Incrementally convert newly available raw sonar files to Sv.",
    ),
    "create_MVBS": FlowRegistration(
        entrypoint="echodataflow/flows/flows_acoustics.py:flow_create_MVBS",
    ),
    "predict_hake": FlowRegistration(
        entrypoint="echodataflow/flows/flows_predict_hake.py:flow_predict_hake",
    ),
    "raw2Sv_postprocessing": FlowRegistration(
        entrypoint=(
            "echodataflow/flows/flows_acoustics.py:"
            "flow_raw2Sv_postprocessing"
        ),
        description="Convert a historical S3 raw-file manifest to Sv incrementally.",
    ),
    "create_MVBS_postprocessing": FlowRegistration(
        entrypoint=(
            "echodataflow/flows/flows_acoustics.py:"
            "flow_create_MVBS_postprocessing"
        ),
        description="Create all newly ready historical MVBS slices.",
    ),
    "predict_hake_postprocessing": FlowRegistration(
        entrypoint=(
            "echodataflow/flows/flows_predict_hake.py:"
            "flow_predict_hake_postprocessing"
        ),
        description="Predict all newly ready historical MVBS windows.",
    ),
    "ingest_haul": FlowRegistration(
        entrypoint="echodataflow/flows/flows_biology.py:flow_ingest_haul",
    ),
    "ingest_NASC": FlowRegistration(
        entrypoint="echodataflow/flows/flows_integration.py:flow_ingest_NASC",
    ),
    "update_grid": FlowRegistration(
        entrypoint="echodataflow/flows/flows_integration.py:flow_update_grid",
    ),
    "file_upload": FlowRegistration(
        entrypoint="echodataflow/flows/flows_helper.py:flow_file_upload",
    ),
    "copy_raw": FlowRegistration(
        entrypoint="echodataflow/flows/flows_simulation.py:flow_copy_raw",
    ),
    "copy_trawl": FlowRegistration(
        entrypoint="echodataflow/flows/flows_simulation.py:flow_copy_trawl",
    ),
    "update_cache_MVBS": FlowRegistration(
        entrypoint="echodataflow/flows/flows_viz_cloud.py:flow_update_cache_MVBS",
    ),
    "update_cache_CPS": FlowRegistration(
        entrypoint="echodataflow/flows/flows_viz_cloud.py:flow_update_cache_CPS",
        description="Update the visualization cache from the latest CPS plot product.",
    ),
    "transect_update": FlowRegistration(
        entrypoint="echodataflow/flows/flows_transect.py:flow_transect_update",
        description="Process updates to transect start/end information.",
    ),
    "process_Sv_CPS": FlowRegistration(
        entrypoint=(
            "echodataflow/flows/flows_CPS_sv.py:"
            "flow_process_Sv_CPS"
        ),
        description=(
            "Process generic Sv stores into per-file "
            "CPS-ready Sv products."
        ),
    ),
    "process_transect_CPS": FlowRegistration(
        entrypoint=(
            "echodataflow/flows/flows_CPS_transect.py:"
            "flow_process_transect_CPS"
        ),
        description=(
            "Assemble CPS-ready Sv products by transect, "
            "classify CPS targets, and compute NASC."
        ),
    ),
    "simulate_transects": FlowRegistration(
        entrypoint=(
            "echodataflow/flows/flows_simulation.py:"
            "flow_simulate_transects"
        ),
        description="Simulate realtime transect updates for testing.",
    ),
}