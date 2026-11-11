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
    path.write_text(yaml.safe_dump(data))
    return path


def test_relative_cache_and_defaults(tmp_path):
    config = load_config(write_config(tmp_path))
    assert config.cache_dir == tmp_path
    assert config.port == 1802
    assert config.tricolor_channels == ["120", "38", "18"]


@pytest.mark.parametrize("overrides, message", [
    ({"tricolor_channels": ["18", "38"]}, "at least 3"),
    ({"tricolor_channels": ["18", "38", "200"]}, "selected from"),
    ({"channels": "18,38,120"}, "valid list"),
    ({"channels": ["18", "18", "120"]}, "unique"),
    ({"channels": ["18", "38", " "]}, "at least 1 character"),
    ({"channels": [18, "38", "120"]}, "valid string"),
    ({"tricolor_channels": ["18", "38", "120", "200"]}, "at most 3"),
    ({"port": True}, "port"),
    ({"port": 65536}, "port"),
    ({"cache_dir": "missing"}, "directory"),
])
def test_invalid_config(tmp_path, overrides, message):
    with pytest.raises(ValidationError, match=message):
        load_config(write_config(tmp_path, **overrides))
