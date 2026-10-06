from types import SimpleNamespace

from echodataflow.operations.operations_acoustics import RawToSvResult, RawToSvSettings
from echodataflow.operations.operations_storage import S3CopySettings, S3CopyWorkItem
from echodataflow.tasks import tasks_postprocessing


def test_s3_raw2sv_chains_copy_into_conversion(monkeypatch):
    copy_item = S3CopyWorkItem(
        s3_path="survey/example.raw",
        local_path="/staging/example.raw",
    )
    copy_settings = S3CopySettings(s3_bucket="source")
    sv_settings = RawToSvSettings(output_directory="/Sv")
    expected = RawToSvResult(
        filename_raw="example.raw",
        filename_Sv="example_Sv.zarr",
        first_ping_time="first",
        last_ping_time="last",
    )
    calls = {}

    def fake_copy(item, settings):
        calls["copy"] = (item, settings)
        return SimpleNamespace(local_path="/staging/example.raw")

    def fake_convert(item, settings):
        calls["convert"] = (item, settings)
        return expected

    monkeypatch.setattr(tasks_postprocessing, "copy_s3_file", fake_copy)
    monkeypatch.setattr(tasks_postprocessing, "convert_raw_to_Sv", fake_convert)

    result = tasks_postprocessing.task_s3_raw2Sv.fn(
        copy_item,
        copy_settings,
        sv_settings,
    )

    assert result is expected
    assert calls["copy"] == (copy_item, copy_settings)
    assert calls["convert"][0].raw_path == "/staging/example.raw"
    assert calls["convert"][1] is sv_settings
