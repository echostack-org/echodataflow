"""Biological data ingestion, transformations, and estimates for trawl hauls."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd

from echodataflow.operations.operations_stratification import assign_stratum
from echodataflow.utils.const import INFO_DATAFRAME_MAPPING, TS_L_PARAMS

BIOLOGY_OUTPUT_KEYS = ("haul_info", "specimens", "lengths", "length_counts")


@dataclass
class BiologyData:
    """The related biological dataframes for one haul or an accumulated dataset."""

    haul_info: pd.DataFrame
    specimens: pd.DataFrame
    lengths: pd.DataFrame
    length_counts: pd.DataFrame

    @classmethod
    def empty(cls) -> BiologyData:
        """Create a dataset containing four empty dataframes."""
        return cls(*(pd.DataFrame() for _ in fields(cls)))


def get_count_from_length_specimen(
    df_length: pd.DataFrame,
    df_specimen: pd.DataFrame,
) -> pd.DataFrame:
    """Combine length-frequency and specimen observations into length counts."""
    # Round specimen lengths to the integer bins used by the length-frequency data
    specimen = df_specimen.copy()
    specimen["length"] = specimen["fork_length"].round(0).astype(int)

    # Count individually measured specimens and aggregate recorded length frequencies
    specimen_counts = (
        specimen.groupby(["sex", "length", "haul"]).size().reset_index(name="frequency")
    )
    length_counts = (
        df_length.groupby(["sex", "length", "haul"]).agg({"frequency": "sum"}).reset_index()
    )

    # A length bin can occur in either source, so retain the union and add both counts
    combined = pd.merge(
        specimen_counts,
        length_counts,
        on=["sex", "length", "haul"],
        how="outer",
        suffixes=("_specimen", "_length"),
    ).fillna(0)
    combined["frequency"] = (combined["frequency_specimen"] + combined["frequency_length"]).astype(
        int
    )
    return combined[["sex", "length", "frequency", "haul"]]


def get_length_weight_regression(df_specimen: pd.DataFrame) -> pd.DataFrame:
    """Get length-weight coefficients by sex and for all fish combined."""
    # Fit male and female relationships separately within each stratum
    df_regres = (
        df_specimen.groupby(["sex", "stratum"], observed=True)
        .apply(
            lambda df: pd.Series(
                np.polyfit(np.log10(df["length"]), np.log10(df["organism_weight"]), 1),
                index=["p1", "p2"],
            ),
            include_groups=False,
        )
        .reset_index()
    )

    # Fit a second relationship using all fish within each stratum
    df_all = (
        df_specimen.groupby("stratum", observed=True)
        .apply(
            lambda df: pd.Series(
                np.polyfit(np.log10(df["length"]), np.log10(df["organism_weight"]), 1),
                index=["p1", "p2"],
            ),
            include_groups=False,
        )
        .reset_index()
    )
    df_all["sex"] = "all"

    # Combine the sex-specific and all-fish coefficients with normalized labels
    df_regres = pd.concat([df_regres, df_all]).reset_index()
    df_regres["sex"] = df_regres["sex"].str.lower()
    return df_regres


def get_sigma_bs_mean_stratum(df_length_count: pd.DataFrame) -> pd.DataFrame:
    """Compute the frequency-weighted mean backscattering cross-section by stratum."""
    length_count = df_length_count.copy()

    # Convert the target-strength relationship into backscattering cross-section
    length_count["sigma_bs"] = 10 ** (
        (TS_L_PARAMS["slope"] * np.log10(length_count["length"]) + TS_L_PARAMS["intercept"]) / 10
    )

    # Weight each length bin by the number of fish observed in that bin
    return (
        length_count.groupby("stratum")
        .apply(
            lambda df: pd.Series(
                (df["sigma_bs"] * df["frequency"]).sum() / df["frequency"].sum(),
                index=["sigma_bs_mean"],
            ),
            include_groups=False,
        )
        .reset_index()
    )


def get_weight_mean_stratum(
    df_length_count: pd.DataFrame,
    df_length_weight_regression: pd.DataFrame,
) -> pd.DataFrame:
    """Compute frequency-weighted mean organism weight by stratum."""
    # Attach the all-fish length-weight coefficients for each stratum
    df_merged = pd.merge(
        df_length_count,
        df_length_weight_regression[df_length_weight_regression["sex"] == "all"][
            ["stratum", "p1", "p2"]
        ],
        on="stratum",
        how="left",
    )

    # Evaluate W = 10^(p1 * log10(L) + p2) for every observed length bin
    df_merged["weight"] = 10 ** (df_merged["p1"] * np.log10(df_merged["length"]) + df_merged["p2"])

    # Weight the modeled value by the number of fish observed in each length bin
    return (
        df_merged.groupby("stratum")
        .apply(
            lambda df: pd.Series(
                (df["weight"] * df["frequency"]).sum() / df["frequency"].sum(),
                index=["weight_mean"],
            ),
            include_groups=False,
        )
        .reset_index()
    )


def read_biology_data(output_paths: Mapping[str, Path]) -> BiologyData:
    """Read accumulated biological outputs, using empty dataframes on the first run."""
    _validate_output_paths(output_paths)
    return BiologyData(
        **{
            key: (
                pd.read_csv(output_paths[key], index_col=0)
                if output_paths[key].exists()
                else pd.DataFrame()
            )
            for key in BIOLOGY_OUTPUT_KEYS
        }
    )


def get_hauls_to_process(
    valid_hauls: Mapping[int, Mapping[str, str]],
    haul_info: pd.DataFrame,
) -> list[int]:
    """Return sorted valid haul numbers absent from accumulated haul information."""
    if haul_info.empty:
        processed: set[int] = set()
    else:
        if "haul" not in haul_info:
            raise ValueError("Existing haul information is missing the 'haul' column")
        processed = set(pd.to_numeric(haul_info["haul"], errors="raise").astype(int))
    return sorted(set(valid_hauls).difference(processed))


def load_haul_data(
    fs: Any,
    haul_num: int,
    haul_files: Mapping[str, str],
) -> BiologyData:
    """Load and normalize the biological workbooks for one complete haul.

    Catch files are required during haul discovery as a completeness signal,
    but their contents are intentionally not ingested yet.
    """
    required = {"length", "specimen", "catch", "info"}
    missing = required.difference(haul_files)
    if missing:
        raise ValueError(f"Haul {haul_num:03d} is missing file types: {sorted(missing)}")

    # Convert the wide length-frequency worksheet to one row per sex and length bin
    with fs.open(haul_files["length"]) as file:
        lengths = (
            pd.read_excel(file, index_col=0, sheet_name="Codend")
            .reset_index()
            .drop("Sum", axis=1)
            .melt(id_vars=["length"], var_name="sex", value_name="frequency")
            .assign(haul=haul_num)
        )
        lengths["frequency"] = lengths["frequency"].fillna(0).astype(int)

    # Load individually measured fish and normalize fork length to the same bins
    with fs.open(haul_files["specimen"]) as file:
        specimens = (
            pd.read_excel(file, index_col=0, sheet_name="Codend")
            .reset_index()
            .assign(haul=haul_num)
        )
        # Preserve the normalized length column in accumulated specimen output
        specimens["length"] = specimens["fork_length"].round(0).astype(int)

    # Keep the net-in-water event as the haul's time and geographic position
    with fs.open(haul_files["info"]) as file:
        haul_info = (
            pd.read_excel(file, index_col=0, sheet_name="ButtonPresses")
            .reset_index()
            .rename(columns=INFO_DATAFRAME_MAPPING)
        )
        haul_info = haul_info[haul_info["button"] == "NIW"][
            ["haul", "timestamp", "latitude", "longitude"]
        ].copy()

    # Combine both sources of length counts before attaching haul location metadata
    length_counts = get_count_from_length_specimen(lengths, specimens)
    locations = haul_info[["haul", "timestamp", "latitude", "longitude"]]
    return BiologyData(
        haul_info=haul_info,
        specimens=pd.merge(specimens, locations, on="haul", how="left"),
        lengths=pd.merge(lengths, locations, on="haul", how="left"),
        length_counts=pd.merge(length_counts, locations, on="haul", how="left"),
    )


def combine_biology_data(datasets: Iterable[BiologyData]) -> BiologyData:
    """Concatenate biological datasets without mutating their dataframes."""
    datasets = list(datasets)
    if not datasets:
        return BiologyData.empty()
    return BiologyData(
        **{
            field.name: pd.concat(
                [getattr(dataset, field.name) for dataset in datasets], ignore_index=True
            )
            for field in fields(BiologyData)
        }
    )


def assign_strata(data: BiologyData, stratum_definitions: pd.DataFrame) -> BiologyData:
    """Return biological data with strata assigned to every dataframe."""
    return BiologyData(
        **{
            field.name: assign_stratum(getattr(data, field.name), stratum_definitions)
            for field in fields(BiologyData)
        }
    )


def compute_stratum_estimates(
    data: BiologyData,
    stratum_definitions: pd.DataFrame,
) -> pd.DataFrame:
    """Recompute biological estimates from the full accumulated dataset."""
    # Refit the relationships from all accumulated specimens whenever data changes
    regression = get_length_weight_regression(data.specimens)

    # Add frequency-weighted backscatter and weight estimates to the definitions
    estimates = pd.merge(
        stratum_definitions.copy(),
        get_sigma_bs_mean_stratum(data.length_counts),
        on="stratum",
        how="outer",
    )
    return pd.merge(
        estimates,
        get_weight_mean_stratum(data.length_counts, regression),
        on="stratum",
        how="outer",
    )


def write_biology_outputs(
    data: BiologyData,
    stratum_mean: pd.DataFrame,
    output_paths: Mapping[str, Path],
    stratum_mean_path: Path,
) -> None:
    """Stage every CSV, then store them as one rollback-protected update."""
    _validate_output_paths(output_paths)
    outputs = {
        **{key: getattr(data, key) for key in BIOLOGY_OUTPUT_KEYS},
        "stratum_mean": stratum_mean,
    }
    paths = {**output_paths, "stratum_mean": stratum_mean_path}
    transaction_id = uuid4().hex
    staged: dict[str, Path] = {}
    backups: dict[str, Path] = {}
    installed: list[str] = []

    try:
        # Serialize everything successfully before changing any published output
        for key, dataframe in outputs.items():
            target = paths[key]
            target.parent.mkdir(parents=True, exist_ok=True)
            stage = target.with_name(f".{target.name}.{transaction_id}.tmp")
            dataframe.to_csv(stage)
            staged[key] = stage

        # Preserve old outputs so a failure during publication can be rolled back
        for key, target in paths.items():
            if target.exists():
                backup = target.with_name(f".{target.name}.{transaction_id}.bak")
                os.replace(target, backup)
                backups[key] = backup
            os.replace(staged[key], target)
            installed.append(key)
    except Exception:
        # Remove newly installed files, then restore every available original
        rollback_errors = []
        for key in reversed(installed):
            try:
                paths[key].unlink(missing_ok=True)
            except OSError as error:
                rollback_errors.append(error)
        for key, backup in backups.items():
            try:
                os.replace(backup, paths[key])
            except OSError as error:
                rollback_errors.append(error)
        if rollback_errors:
            raise RuntimeError(
                "Biology output update failed and could not be fully rolled back"
            ) from rollback_errors[0]
        raise
    else:
        for backup in backups.values():
            backup.unlink(missing_ok=True)
    finally:
        for stage in staged.values():
            stage.unlink(missing_ok=True)


def _validate_output_paths(output_paths: Mapping[str, Path]) -> None:
    missing = set(BIOLOGY_OUTPUT_KEYS).difference(output_paths)
    unexpected = set(output_paths).difference(BIOLOGY_OUTPUT_KEYS)
    if missing or unexpected:
        raise ValueError(
            f"Invalid biology output paths: missing={sorted(missing)}, "
            f"unexpected={sorted(unexpected)}"
        )
