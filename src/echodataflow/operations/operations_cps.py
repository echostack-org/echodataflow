"""CPS numerical operations, independent of Prefect and the processing ledger."""

import echopype as ep
import numpy as np
import xarray as xr

from .cps_helpers import _dilate_7x7, _pick_channel_by_frequency


def align_cps_geometry(ds, target_frequency, label, geometry_frequency=38000):
    """Align acoustic geometry using the existing target-channel policy."""
    target_channel = _pick_channel_by_frequency(ds, target_frequency)
    print(f"Target frequency: {target_frequency / 1000:.0f} kHz")
    print(f"Target channel: {target_channel}")
    geometry_channel = _required_channels(ds, [geometry_frequency])[geometry_frequency]
    ds_geometry = ds.chunk({"range_sample": -1})
    aligned = ep.commongrid.resample_to_geometry(
        ds_geometry, target_variable="Sv", target_channel=geometry_channel
    )
    if "sound_absorption" in ds:
        aligned["sound_absorption"] = ds["sound_absorption"]
    aligned = ep.consolidate.add_depth(aligned)
    ds[["Sv", "echo_range"]] = aligned[["Sv", "echo_range"]]
    ds = ep.consolidate.add_depth(ds)
    print(f"{label}: geometry alignment complete")
    return (ds, target_channel)


def detect_cps_seafloor(
    ds,
    target_channel,
    seafloor_threshold,
    seafloor_offset,
    seafloor_r0,
    seafloor_r1,
    seafloor_wtheta,
    seafloor_wphi,
):
    """Run Blackwell detection with the existing parameters."""
    return ep.mask.detect_seafloor(
        ds=ds,
        method="blackwell",
        params={
            "channel": target_channel,
            "var_name": "Sv",
            "threshold": seafloor_threshold,
            "offset": seafloor_offset,
            "r0": seafloor_r0,
            "r1": seafloor_r1,
            "wtheta": seafloor_wtheta,
            "wphi": seafloor_wphi,
        },
    )


def build_surface_mask(ds, target_channel, min_depth):
    """Return the mask excluding depths at or above the surface limit."""
    target_depth = ds["depth"].sel(channel=target_channel)
    return target_depth > min_depth


def build_water_column_mask(ds, surface_mask, above_seafloor_mask, label):
    """Combine exclusions and attach the water-column products."""
    valid_water_column = surface_mask & above_seafloor_mask
    ds["surface_mask"] = surface_mask
    ds["above_seafloor_mask"] = above_seafloor_mask
    ds["valid_water_column"] = valid_water_column
    ds["Sv_water_column"] = ds["Sv"].where(valid_water_column)
    print(f"{label}: water-column mask complete")
    return ds


def _required_channels(ds, frequencies, tolerance_hz=1000):
    """Reject missing or ambiguous nominal frequencies instead of substituting channels."""
    nominal = np.asarray(ds.frequency_nominal.values)
    channels = {}
    for frequency in frequencies:
        matches = np.flatnonzero(np.abs(nominal - frequency) <= tolerance_hz)
        if len(matches) != 1:
            raise ValueError(f"Expected one channel near {frequency} Hz; found {len(matches)}")
        channels[frequency] = str(ds.channel.values[matches[0]])
    if len(set(channels.values())) != len(channels):
        raise ValueError("CPS frequencies must identify distinct channels")
    return channels


def correct_background_noise(ds, label):
    """Run noise correction; failures must stop the validation run.

    Existing settings retained: equivalence to Echoview noise settings is not yet
    established from the binary template.
    """
    print(f"{label}: starting background-noise correction")
    return ep.clean.remove_background_noise(
        ds, ping_num=20, range_sample_num=5, SNR_threshold="5.0dB"
    )


def classify_cps(
    ds, valid_water_column, mask_mode, fallback_sv_threshold, label, use_noise_residual=True
):
    """Evaluate CPS criteria and retain intermediate masks for inspection.

    use_noise_residual=False bypasses the corrected-minus-original criterion
    for a demonstration without noise correction; results are not equivalent.

    Frequency response dilates Clean Sv directly. The variance operand named
    XxY Mean in the EV template is actually a dB-to-linear conversion, not a
    local mean. Residuals compare Clean Sv with the original aligned Sv.
    The fallback argument is retained for existing callers but is never used.
    Centered 3-ping x 11-sample full windows are an explicit provisional policy:
    the EV operator settings/edge handling still require confirmation.
    """
    if mask_mode != "cps":
        raise ValueError("This validation implementation requires mask_mode='cps'")
    channels = _required_channels(ds, [38000, 70000, 120000, 200000])
    if "Sv_corrected" not in ds:
        raise ValueError("Noise-corrected Sv is required for CPS classification")
    # Select each channel without retaining a conflicting scalar channel coordinate.
    def at(da, frequency):
        return da.sel(channel=channels[frequency], drop=True)

    # A bypassed correction makes corrected-original residuals identically zero.
    # Explicitly disable the dependent residual criterion in that case.
    sv = ds.Sv_corrected.where(valid_water_column).transpose("channel", "ping_time", "range_sample")

    def mean(da):
        return da.rolling(ping_time=3, range_sample=11, center=True, min_periods=33).mean()

    if use_noise_residual:
        clean_linear = 10 ** (sv / 10)
        original_linear = 10 ** (ds.Sv.transpose("channel", "ping_time", "range_sample") / 10)
        ds["variance"] = (clean_linear - original_linear) ** 2
        ds["variance_smoothed"] = _dilate_7x7(10 * np.log10(np.sqrt(mean(ds["variance"]))))
        ds["mask_sd120"] = at(ds.variance_smoothed, 120000) > -60
        ds["mask_sd200"] = at(ds.variance_smoothed, 200000) > -60
    ds["Sv_dilated"] = _dilate_7x7(sv)
    for frequency, low, high in [
        (70000, -13.85, 9.89),
        (120000, -13.5, 9.37),
        (200000, -13.51, 12.53),
    ]:
        difference = at(ds.Sv_dilated, frequency) - at(ds.Sv_dilated, 38000)
        name = f"difference_{frequency // 1000}_38"
        ds[name] = difference
        ds[f"mask_frequency{frequency // 1000}"] = (
            np.isfinite(difference) & (difference > low) & (difference < high)
        )
    ds["mask_frequency"] = ds.mask_frequency70 & ds.mask_frequency120 & ds.mask_frequency200
    ds["mask_sd"] = (
        ds.mask_sd120 & ds.mask_sd200
        if use_noise_residual
        else xr.ones_like(valid_water_column, dtype=bool)
    )
    return ds.mask_frequency & ds.mask_sd & valid_water_column


def apply_cps_mask(ds, final_mask, label):
    """Apply the mask to original Sv, preserving the existing output policy."""
    ds["final_cps_mask"] = final_mask
    ds["Sv_masked"] = ds["Sv"].where(final_mask)
    print(f"{label}: CPS graph prepared; computation may still be pending")
    return ds
