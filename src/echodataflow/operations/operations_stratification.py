"""Shared survey stratification operations."""

from __future__ import annotations

import pandas as pd


def assign_stratum(
    dataframe: pd.DataFrame,
    stratum_definitions: pd.DataFrame,
) -> pd.DataFrame:
    """Return a copy with strata assigned from each observation's latitude."""
    result = dataframe.copy()

    # A missing northern limit denotes the final open-ended stratum
    bounded = stratum_definitions.dropna(subset=["latitude_northern_limit"]).sort_values(
        "latitude_northern_limit"
    )
    open_ended = stratum_definitions[stratum_definitions["latitude_northern_limit"].isna()]
    if len(open_ended) > 1:
        raise ValueError("Stratum definitions contain multiple open-ended strata")

    northern_limits = bounded["latitude_northern_limit"].tolist()
    if len(northern_limits) != len(set(northern_limits)):
        raise ValueError("Stratum northern limits must be unique")

    # Extend the configured limits to cover every possible latitude
    latitude_bins = [-90.0, *northern_limits, 90.0]
    final_stratum = (
        open_ended["stratum"].iloc[0] if not open_ended.empty else bounded["stratum"].max() + 1
    )
    stratum_labels = [*bounded["stratum"].tolist(), final_stratum]
    result["stratum"] = pd.cut(
        result["latitude"],
        bins=latitude_bins,
        labels=stratum_labels,
        include_lowest=True,
    )
    return result
