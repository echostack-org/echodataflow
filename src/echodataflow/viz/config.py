"""Configuration for visualization applications."""

import argparse
import os
from pathlib import Path
from typing import Annotated, Literal, Self, overload

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    DirectoryPath,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

ChannelID = Annotated[str, StringConstraints(strict=True, strip_whitespace=True, min_length=1)]


def parse_config_file() -> Path | None:
    """Read the visualization app's optional configuration argument."""
    parser = argparse.ArgumentParser(description="Run an echodataflow visualization app.")
    parser.add_argument(
        "--config-file",
        type=Path,
        help="Visualization YAML path (overrides ECHODATAFLOW_VIZ_CONFIG).",
    )
    return parser.parse_args().config_file


class EchogramConfig(BaseModel):
    """Validated settings shared by the echogram application."""

    model_config = ConfigDict(frozen=True)

    cache_dir: DirectoryPath
    mvbs_file: Path = Path("latest_MVBS.zarr")
    contours_file: Path = Path("latest_contours.csv")
    vertical_dimension: ChannelID = "depth"
    color_limits: tuple[float, float] = (-70, -36)
    channels: Annotated[list[ChannelID], Field(min_length=1)]
    tricolor_channels: Annotated[list[ChannelID], Field(min_length=3, max_length=3)]
    port: Annotated[int, Field(strict=True, ge=1, le=65535)] = 1802
    refresh_seconds: Annotated[int, Field(strict=True, gt=0)] = 600

    @field_validator("color_limits")
    @classmethod
    def check_color_limits(cls, limits: tuple[float, float]) -> tuple[float, float]:
        lower, upper = limits
        if not -float("inf") < lower < upper < float("inf"):
            raise ValueError("Color limits must be finite and increasing")
        return limits

    @field_validator("channels", "tricolor_channels")
    @classmethod
    def unique_channels(cls, values: list[str]) -> list[str]:
        if len(set(values)) != len(values):
            raise ValueError("Channel IDs must be unique")
        return values

    @model_validator(mode="after")
    def check_tricolor_channels(self) -> Self:
        if not set(self.tricolor_channels).issubset(self.channels):
            raise ValueError("tricolor_channels must be selected from channels")
        return self


class BioConfig(BaseModel):
    """Deployment settings for the biological visualization application."""

    model_config = ConfigDict(frozen=True)

    data_dir: DirectoryPath
    grid_file: Path = Path("grid_cells.geojson")
    nasc_file: Path = Path("NASC_all.csv")
    length_count_file: Path = Path("length_count_all.csv")
    specimen_file: Path = Path("specimen_all.csv")
    port: Annotated[int, Field(strict=True, ge=1, le=65535)] = 1804
    refresh_seconds: Annotated[int, Field(strict=True, gt=0)] = 600
    grid_basemap: ChannelID = "EsriNatGeo"
    track_basemap: ChannelID = "EsriOceanBase"
    color_limits: dict[str, tuple[float, float]] = Field(default_factory=dict)

    @field_validator("color_limits")
    @classmethod
    def check_color_limits(cls, limits):
        from echodataflow.viz.viz_core import BIO_VAR_CLIM

        for name, (lower, upper) in limits.items():
            if name not in BIO_VAR_CLIM:
                raise ValueError(f"Unknown biological variable: {name}")
            if not 0 < lower < upper < float("inf"):
                raise ValueError("Color limits must be finite, positive, and increasing")
        return limits


@overload
def load_config(section: Literal["echogram"], path: str | Path | None = None) -> EchogramConfig: ...


@overload
def load_config(section: Literal["bio"], path: str | Path | None = None) -> BioConfig: ...


def load_config(
    section: Literal["echogram", "bio"], path: str | Path | None = None
) -> EchogramConfig | BioConfig:
    """Load one YAML section and resolve its directory against the config file.

    Only the selected application's settings are validated. Supply a path via 
    command line or set ECHODATAFLOW_VIZ_CONFIG (loaded from services.env by 
    system services).
    """
    models = {"echogram": (EchogramConfig, "cache_dir"), "bio": (BioConfig, "data_dir")}
    if section not in models:
        raise ValueError(f"Unknown visualization config section: {section}")
    model, directory_key = models[section]
    configured_path = path or os.environ.get("ECHODATAFLOW_VIZ_CONFIG")
    if not configured_path:
        raise ValueError(
            "Visualization configuration path is required: pass --config-file or set "
            "ECHODATAFLOW_VIZ_CONFIG in services.env for system services."
        )
    config_path = Path(configured_path).expanduser()
    with config_path.open() as stream:
        document = yaml.safe_load(stream)
    data = document.get(section) if isinstance(document, dict) else None
    if isinstance(data, dict) and isinstance(data.get(directory_key), str):
        directory = Path(data[directory_key]).expanduser()
        data[directory_key] = directory if directory.is_absolute() else config_path.parent / directory
    return model.model_validate(data)
