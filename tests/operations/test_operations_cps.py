import numpy as np
import pytest
import xarray as xr

from echodataflow.operations import operations_cps as ops


@pytest.fixture
def sample():
    return xr.Dataset(
        {
            "Sv": (
                ("channel", "ping_time", "range_sample"),
                [[[-80.0, -60.0, -40.0, np.nan]]],
            ),
            "depth": (
                ("channel", "ping_time", "range_sample"),
                [[[5.0, 10.0, 15.0, 20.0]]],
            ),
        },
        coords={
            "channel": ["70"],
            "ping_time": [np.datetime64("2024-08-05T13:20:00", "ns")],
            "range_sample": [0, 1, 2, 3],
        },
    )


def test_surface_mask_excludes_limit_and_shallower_depths(sample):
    result = ops.build_surface_mask(sample, "70", 10)
    np.testing.assert_array_equal(result.values, [[False, False, True, True]])


def test_water_column_combines_both_exclusions(sample):
    surface = ops.build_surface_mask(sample, "70", 10)
    above_bottom = surface.copy(data=[[True, True, True, False]])
    result = ops.build_water_column_mask(sample, surface, above_bottom, "test")
    np.testing.assert_array_equal(
        result.valid_water_column, [[False, False, True, False]]
    )
    np.testing.assert_allclose(
        result.Sv_water_column, [[[np.nan, np.nan, -40.0, np.nan]]]
    )
    xr.testing.assert_equal(result.surface_mask, surface.drop_vars("channel"))
    xr.testing.assert_equal(
        result.above_seafloor_mask, above_bottom.drop_vars("channel")
    )


def test_noise_correction_calls_echopype_with_current_parameters(monkeypatch, sample):
    def correction(ds, **kwargs):
        assert kwargs == dict(ping_num=20, range_sample_num=5, SNR_threshold="5.0dB")
        return ds.assign(Sv_corrected=ds.Sv - 3)

    monkeypatch.setattr(ops.ep.clean, "remove_background_noise", correction)
    result = ops.correct_background_noise(sample, "test")
    xr.testing.assert_equal(result.Sv_corrected, sample.Sv.rename("Sv_corrected") - 3)


def test_noise_failure_preserves_existing_original_sv_fallback(
    monkeypatch, sample, capsys
):
    def fail(*args, **kwargs):
        raise RuntimeError("noise failed")

    monkeypatch.setattr(ops.ep.clean, "remove_background_noise", fail)
    result = ops.correct_background_noise(sample, "test")
    xr.testing.assert_equal(result.Sv_corrected, result.Sv.rename("Sv_corrected"))
    assert "noise failed" in capsys.readouterr().out


def test_threshold_fallback_uses_corrected_sv_and_water_mask(monkeypatch, sample):
    # Dilation is an external helper, not the behavior tested here.
    monkeypatch.setattr(ops, "_dilate_7x7", lambda a: a)
    sample["Sv_corrected"] = sample.Sv - 20
    valid = xr.ones_like(sample.Sv.isel(channel=0, drop=True), dtype=bool)
    valid.values[0, 1] = False
    result = ops.classify_cps(sample, valid, "threshold", -70, "test")
    np.testing.assert_array_equal(result.values, [[[False, False, True, False]]])
    assert (
        "Sv_masked" not in sample
    )  # classification returns a mask; application is separate


def test_classifier_error_preserves_threshold_fallback(monkeypatch, sample, capsys):
    def fail(_array):
        raise RuntimeError("classifier failed")

    monkeypatch.setattr(ops, "_dilate_7x7", fail)
    sample["Sv_corrected"] = sample.Sv
    valid = xr.ones_like(sample.Sv.isel(channel=0, drop=True), dtype=bool)
    result = ops.classify_cps(sample, valid, "cps", -70, "test")
    np.testing.assert_array_equal(result.values, [[[False, True, True, False]]])
    assert "classifier failed" in capsys.readouterr().out


def test_multifrequency_cps_rejects_constant_background(monkeypatch):
    # Constant Sv yields zero variance, so the CPS variance criterion rejects it.
    monkeypatch.setattr(ops, "_dilate_7x7", lambda a: a)
    ds = xr.Dataset(
        {
            "Sv": (
                ("channel", "ping_time", "range_sample"),
                np.full((4, 5, 15), -40.0),
            ),
            "frequency_nominal": ("channel", [38000.0, 70000.0, 120000.0, 200000.0]),
        },
        coords={
            "channel": ["38", "70", "120", "200"],
            "ping_time": np.arange(5),
            "range_sample": np.arange(15),
        },
    )
    valid = xr.ones_like(ds.Sv.isel(channel=0, drop=True), dtype=bool)
    with np.errstate(divide="ignore", invalid="ignore"):
        result = ops.classify_cps(ds, valid, "cps", -70, "test")
    assert result.dims == ("ping_time", "range_sample")
    assert not result.values.any()
    assert "Sv_dilated" in ds  # the multi-frequency path was reached


def test_mask_application_preserves_original_sv_choice(sample):
    sample["Sv_corrected"] = sample.Sv - 20
    mask = xr.ones_like(sample.Sv, dtype=bool)
    mask.values[0, 0, 0] = False
    result = ops.apply_cps_mask(sample, mask, "test")
    np.testing.assert_allclose(result.Sv_masked, [[[np.nan, -60.0, -40.0, np.nan]]])
    xr.testing.assert_equal(result.final_cps_mask, mask.rename("final_cps_mask"))
