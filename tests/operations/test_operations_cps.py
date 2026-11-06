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
    np.testing.assert_array_equal(result.valid_water_column, [[False, False, True, False]])
    np.testing.assert_allclose(result.Sv_water_column, [[[np.nan, np.nan, -40.0, np.nan]]])
    xr.testing.assert_equal(result.surface_mask, surface.drop_vars("channel"))
    xr.testing.assert_equal(result.above_seafloor_mask, above_bottom.drop_vars("channel"))


def test_noise_correction_calls_echopype_with_current_parameters(monkeypatch, sample):
    def correction(ds, **kwargs):
        assert kwargs == dict(ping_num=20, range_sample_num=5, SNR_threshold="5.0dB")
        return ds.assign(Sv_corrected=ds.Sv - 3)

    monkeypatch.setattr(ops.ep.clean, "remove_background_noise", correction)
    result = ops.correct_background_noise(sample, "test")
    xr.testing.assert_equal(result.Sv_corrected, sample.Sv.rename("Sv_corrected") - 3)


def test_noise_failure_propagates_without_silent_fallback(monkeypatch, sample):
    def fail(*args, **kwargs):
        raise RuntimeError("noise failed")

    monkeypatch.setattr(ops.ep.clean, "remove_background_noise", fail)
    with pytest.raises(RuntimeError, match="noise failed"):
        ops.correct_background_noise(sample, "test")
    assert "Sv_corrected" not in sample


def test_classifier_rejects_unsupported_threshold_mode(sample):
    sample["Sv_corrected"] = sample.Sv - 20
    valid = xr.ones_like(sample.Sv.isel(channel=0, drop=True), dtype=bool)
    with pytest.raises(ValueError, match="mask_mode='cps'"):
        ops.classify_cps(sample, valid, "threshold", -70, "test")


def test_classifier_error_propagates_without_threshold_fallback(monkeypatch):
    def fail(_array):
        raise RuntimeError("classifier failed")

    monkeypatch.setattr(ops, "_dilate_7x7", fail)
    ds = multifrequency_dataset()
    ds["Sv_corrected"] = ds.Sv.copy()
    valid = xr.ones_like(ds.Sv.isel(channel=0, drop=True), dtype=bool)
    with pytest.raises(RuntimeError, match="classifier failed"):
        ops.classify_cps(ds, valid, "cps", -70, "test")


def multifrequency_dataset():
    return xr.Dataset(
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


def test_multifrequency_cps_rejects_constant_background(monkeypatch):
    # Identical corrected/original Sv yields a zero residual and must be rejected.
    monkeypatch.setattr(ops, "_dilate_7x7", lambda a: a)
    ds = multifrequency_dataset()
    ds["Sv_corrected"] = ds.Sv.copy()
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


def test_classifier_requires_corrected_sv():
    ds = multifrequency_dataset()
    valid = xr.ones_like(ds.Sv.isel(channel=0, drop=True), dtype=bool)
    with pytest.raises(ValueError, match="Noise-corrected Sv is required"):
        ops.classify_cps(ds, valid, "cps", -70, "test")


def test_demo_bypasses_only_residual_and_retains_water_mask(monkeypatch):
    monkeypatch.setattr(ops, "_dilate_7x7", lambda array: array)
    ds = multifrequency_dataset()
    ds["Sv_corrected"] = ds.Sv.copy()
    valid = xr.ones_like(ds.Sv.isel(channel=0, drop=True), dtype=bool)
    valid.values[0, 0] = False
    result = ops.classify_cps(ds, valid, "cps", -70, "test", use_noise_residual=False)
    xr.testing.assert_equal(result.reset_coords(drop=True).rename(None), valid.rename(None))
    ds["Sv_corrected"].loc[{"channel": "200"}] = -10.0
    result = ops.classify_cps(ds, valid, "cps", -70, "test", use_noise_residual=False)
    assert not result.values.any()  # Frequency-response exclusion remains active.
