"""Validate visualization configuration before server startup."""

import pytest
import yaml
from pydantic import ValidationError

from echodataflow.viz.config import load_config


def write_config(tmp_path, **overrides):
    data = {
        "cache_dir": ".",
        "channels": ["18", "38", "120"],
        "tricolor_channels": ["120", "38", "18"],
    }
    data.update(overrides)
    path = tmp_path / "viz.yaml"
    path.write_text(yaml.safe_dump({"echogram": data}))
    return path


def test_relative_cache_and_defaults(tmp_path):
    config = load_config("echogram", write_config(tmp_path))
    assert config.cache_dir == tmp_path
    assert config.port == 1802
    assert config.refresh_seconds == 600
    assert config.tricolor_channels == ["120", "38", "18"]


@pytest.mark.parametrize("env_value", [None, ""])
def test_config_path_required(monkeypatch, env_value):
    if env_value is None:
        monkeypatch.delenv("ECHODATAFLOW_VIZ_CONFIG", raising=False)
    else:
        monkeypatch.setenv("ECHODATAFLOW_VIZ_CONFIG", env_value)
    with pytest.raises(ValueError, match="pass --config-file"):
        load_config("echogram")


def test_explicit_path_overrides_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("ECHODATAFLOW_VIZ_CONFIG", str(tmp_path / "missing.yaml"))
    assert load_config("echogram", write_config(tmp_path)).cache_dir == tmp_path


@pytest.mark.parametrize("overrides, message", [
    ({"tricolor_channels": ["18", "38"]}, "at least 3"),
    ({"tricolor_channels": ["18", "38", "200"]}, "selected from"),
    ({"channels": "18,38,120"}, "valid list"),
    ({"channels": ["18", "18", "120"]}, "unique"),
    ({"channels": ["18", "38", " "]}, "at least 1 character"),
    ({"channels": [18, "38", "120"]}, "valid string"),
    ({"tricolor_channels": ["18", "38", "120", "200"]}, "at most 3"),
    ({"refresh_seconds": 0}, "refresh_seconds"),
    ({"refresh_seconds": -1}, "refresh_seconds"),
    ({"port": True}, "port"),
    ({"port": 65536}, "port"),
    ({"cache_dir": "missing"}, "directory"),
])
def test_invalid_config(tmp_path, overrides, message):
    with pytest.raises(ValidationError, match=message):
        load_config("echogram", write_config(tmp_path, **overrides))


def test_bio_config_independent_of_echogram(tmp_path, monkeypatch):
    path = tmp_path / "viz.yaml"
    path.write_text(yaml.safe_dump({"bio": {
        "data_dir": ".",
        "port": 1810,
        "refresh_seconds": 120,
        "grid_file": "custom.geojson",
        "color_limits": {"NASC": [10, 100]},
    }}))
    monkeypatch.setenv("ECHODATAFLOW_VIZ_CONFIG", str(path))
    config = load_config("bio")
    assert config.data_dir == tmp_path
    assert config.port == 1810
    assert config.refresh_seconds == 120
    assert str(config.grid_file) == "custom.geojson"
    assert config.color_limits["NASC"] == (10, 100)
    assert config.track_basemap == "EsriOceanBase"


@pytest.mark.parametrize("overrides", [
    {"port": 0}, {"refresh_seconds": 0}, {"data_dir": "missing"},
    {"color_limits": {"NASC": [100, 10]}},
    {"color_limits": {"unknown": [10, 100]}},
])
def test_invalid_bio_config(tmp_path, overrides):
    path = tmp_path / "viz.yaml"
    path.write_text(yaml.safe_dump({"bio": {"data_dir": ".", **overrides}}))
    with pytest.raises(ValidationError):
        load_config("bio", path)


def test_echogram_refresh_override(tmp_path):
    assert load_config("echogram", write_config(tmp_path, refresh_seconds=120)).refresh_seconds == 120


def test_unknown_section(tmp_path):
    with pytest.raises(ValueError, match="Unknown visualization config section"):
        load_config("unknown", tmp_path / "missing.yaml")


def test_echogram_ignores_invalid_bio_section(tmp_path):
    path = write_config(tmp_path)
    data = yaml.safe_load(path.read_text())
    data["bio"] = {"data_dir": "missing"}
    path.write_text(yaml.safe_dump(data))
    assert load_config("echogram", path).cache_dir == tmp_path


@pytest.mark.parametrize("limits", [[0, -10], [float("nan"), 0], [-70, float("inf")]])
def test_invalid_echogram_color_limits(tmp_path, limits):
    with pytest.raises(ValidationError, match="finite and increasing"):
        load_config("echogram", write_config(tmp_path, color_limits=limits))


def test_external_recipe_config_paths(tmp_path):
    recipe_dir = tmp_path / "echodataflow-recipes" / "recipes_nrt" / "hake"
    recipe_dir.mkdir(parents=True)
    (recipe_dir / "cache").mkdir()
    path = write_config(recipe_dir, cache_dir="cache", mvbs_file="custom.zarr",
                        contours_file="custom.csv", vertical_dimension="range_sample",
                        color_limits=[-80, -40])
    config = load_config("echogram", path)
    assert config.cache_dir == recipe_dir / "cache"
    assert str(config.mvbs_file) == "custom.zarr"
    assert str(config.contours_file) == "custom.csv"
    assert config.vertical_dimension == "range_sample"
    assert config.color_limits == (-80, -40)
