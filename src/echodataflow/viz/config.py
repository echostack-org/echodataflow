"""Configuration for the echogram visualization application."""

import os
from pathlib import Path
from typing import Annotated, Self

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


class EchogramConfig(BaseModel):
    """Validated settings shared by the echogram application."""

    model_config = ConfigDict(frozen=True)

    cache_dir: DirectoryPath
    channels: Annotated[list[ChannelID], Field(min_length=1)]
    tricolor_channels: Annotated[list[ChannelID], Field(min_length=3, max_length=3)]
    port: Annotated[int, Field(strict=True, ge=1, le=65535)] = 1802

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


def load_config(path: str | Path | None = None) -> EchogramConfig:
    """Load YAML; resolve relative cache paths against the configuration file."""
    config_path = Path(
        path or os.environ.get("ECHODATAFLOW_VIZ_CONFIG", "~/.config/echodataflow/viz.yaml")
    ).expanduser()
    with config_path.open() as stream:
        data = yaml.safe_load(stream)
    if isinstance(data, dict) and isinstance(data.get("cache_dir"), str):
        cache_dir = Path(data["cache_dir"]).expanduser()
        data["cache_dir"] = (
            cache_dir if cache_dir.is_absolute() else config_path.parent / cache_dir
        )
    return EchogramConfig.model_validate(data)
