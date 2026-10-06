"""Configuration loading for SPASSO visualization services."""

from __future__ import annotations

from pathlib import Path

import yaml


def load_spasso_viz_config(
    config_path: str | Path,
) -> dict:
    """Load a SPASSO visualization configuration."""

    config_path = Path(config_path)

    with config_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(
            f"SPASSO visualization config must be a mapping: "
            f"{config_path}"
        )

    if "dashboard" not in config:
        raise ValueError(
            f"Missing 'dashboard' section in {config_path}"
        )

    if "products" not in config:
        raise ValueError(
            f"Missing 'products' section in {config_path}"
        )

    return config