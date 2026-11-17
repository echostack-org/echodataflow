import configparser
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from echodataflow.utils import utils


@pytest.fixture
def remote_config(monkeypatch, tmp_path):
    path = tmp_path / "rclone.conf"
    path.write_text(
        "[other_remote]\ntype = s3\naccess_key_id = key%literal\nsecret_access_key = secret\nendpoint = https://example.test\nregion = us-east-1\nsession_token = token\n"
    )
    run = Mock(return_value=SimpleNamespace(stdout=f"Configuration file is stored at:\n{path}\n"))
    monkeypatch.setattr(utils.subprocess, "run", run)
    filesystem = Mock()
    monkeypatch.setattr(utils.s3fs, "S3FileSystem", filesystem)
    return path, run, filesystem


@pytest.mark.parametrize("path", ["other_remote:bucket/path/to/dir", "other_remote:/bucket/path/to/dir/"])
def test_resolve_remote_and_normalize_path(remote_config, path):
    _, run, filesystem = remote_config
    fs, prefix = utils.resolve_rclone_s3_path(path)
    assert prefix == "bucket/path/to/dir"
    assert fs is filesystem.return_value
    run.assert_called_once_with(
        ["rclone", "config", "file"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    filesystem.assert_called_once_with(
        key="key%literal",
        secret="secret",
        token="token",
        anon=False,
        client_kwargs={"endpoint_url": "https://example.test", "region_name": "us-east-1"},
    )


@pytest.mark.parametrize("path", ["bucket/prefix", "remote:", ":bucket", "s3://bucket"])
def test_invalid_path(path):
    with pytest.raises(ValueError, match="remote:bucket/path/to/dir"):
        utils.resolve_rclone_s3_path(path)


def test_unknown_remote(remote_config):
    with pytest.raises(ValueError, match="missing"):
        utils.resolve_rclone_s3_path("missing:bucket")


@pytest.mark.parametrize(
    "contents, message",
    [
        ("[other_remote]\ntype = drive\n", "S3 remote"),
        ("[other_remote]\ntype = s3\naccess_key_id = key\n", "both S3 credential"),
    ],
)
def test_unsupported_config(remote_config, contents, message):
    path, _, _ = remote_config
    path.write_text(contents)
    with pytest.raises(ValueError, match=message):
        utils.resolve_rclone_s3_path("other_remote:bucket")


def test_missing_config(remote_config):
    path, _, _ = remote_config
    path.unlink()
    with pytest.raises(FileNotFoundError):
        utils.resolve_rclone_s3_path("other_remote:bucket")


@pytest.mark.parametrize(
    "error",
    [
        FileNotFoundError(),
        subprocess.CalledProcessError(1, "rclone"),
        subprocess.TimeoutExpired("rclone", 30),
    ],
)
def test_discovery_failure(monkeypatch, error):
    monkeypatch.setattr(utils.subprocess, "run", Mock(side_effect=error))
    with pytest.raises(type(error)):
        utils.resolve_rclone_s3_path("other_remote:bucket")


@pytest.mark.parametrize("env_auth", ["true", "false"])
def test_missing_credentials_rejected_even_with_env_auth(remote_config, env_auth):
    path, _, filesystem = remote_config
    path.write_text(f"[other_remote]\ntype = s3\nenv_auth = {env_auth}\n")
    with pytest.raises(ValueError, match="both S3 credential"):
        utils.resolve_rclone_s3_path("other_remote:bucket")
    filesystem.assert_not_called()


def test_encrypted_config_raises_parser_error(remote_config):
    path, _, _ = remote_config
    path.write_text("RCLONE_ENCRYPT_V0:\nencrypted")
    with pytest.raises(configparser.Error):
        utils.resolve_rclone_s3_path("other_remote:bucket")
