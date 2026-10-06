from types import SimpleNamespace

import pandas as pd

from echodataflow.operations import operations_predict_hake


class FakeCombinedDataset:
    def __init__(self):
        self.selection = None
        self.coordinates = {
            "depth": "depth-coordinate",
            "ping_time": [
                SimpleNamespace(values="2026-01-01T00:00:05"),
                SimpleNamespace(values="2026-01-01T00:09:55"),
            ],
        }

    def sel(self, **kwargs):
        self.selection = kwargs
        return self

    def __getitem__(self, key):
        return self.coordinates[key]


class FakeTensor:
    def __init__(self, value):
        self.value = value

    def detach(self):
        return self.value


class FakeModel:
    def __init__(self):
        self.calls = []

    def forward(self, input_tensor, softmax_temperature):
        self.calls.append((input_tensor, softmax_temperature))
        return {
            "interpolated_output": FakeTensor("score-tensor"),
            "softmax_output": FakeTensor("softmax-tensor"),
        }


class FakeDataArray:
    def __init__(self, name):
        self.name = name
        self.saved = []

    def sel(self, **_kwargs):
        return self

    def transpose(self, *_dimensions):
        return self

    def drop_vars(self, *_variables):
        return self

    def __gt__(self, _threshold):
        return self

    def chunk(self, _chunks):
        return self

    def to_zarr(self, **kwargs):
        self.saved.append(kwargs)


class FakeNASCDataset:
    def __init__(self):
        self.saved = None

    def to_zarr(self, **kwargs):
        self.saved = kwargs


def test_predict_hake_result_chains_into_compute_NASC(monkeypatch, tmp_path):
    combined = FakeCombinedDataset()
    model = FakeModel()
    arrays = []
    evr_call = {}

    monkeypatch.setattr(
        operations_predict_hake.xr,
        "open_mfdataset",
        lambda *_args, **_kwargs: combined,
    )
    monkeypatch.setattr(
        operations_predict_hake,
        "get_MVBS_tensor",
        lambda dataset: ("model-input", dataset),
    )

    def fake_data_array(_tensor, *, coords, name):
        array = FakeDataArray(name)
        array.coords = coords
        arrays.append(array)
        return array

    monkeypatch.setattr(operations_predict_hake.xr, "DataArray", fake_data_array)

    def fake_write_evr(path, prediction, region_classification):
        evr_call.update(
            path=path,
            prediction=prediction,
            region_classification=region_classification,
        )

    monkeypatch.setattr(operations_predict_hake.er, "write_evr", fake_write_evr)

    prediction_result = operations_predict_hake.predict_hake(
        operations_predict_hake.PredictHakeWorkItem(
            start_time=pd.Timestamp("2026-01-01T00:00:00Z"),
            end_time=pd.Timestamp("2026-01-01T00:10:00Z"),
            filenames_MVBS=("first.zarr", "second.zarr"),
            filename_postfix="20260101T000000",
        ),
        operations_predict_hake.PredictHakeSettings(
            model=model,
            directory_MVBS=str(tmp_path / "MVBS"),
            directory_prediction=str(tmp_path / "prediction"),
            directory_evr=str(tmp_path / "EVR"),
            temperature=0.75,
            softmax_threshold=0.6,
            max_depth=590,
        ),
    )

    assert prediction_result.mvbs_dataset is combined
    assert prediction_result.hake_prediction is arrays[1]
    assert prediction_result.score_filename == "score_20260101T000000.zarr"
    assert prediction_result.softmax_filename == "softmax_20260101T000000.zarr"
    assert prediction_result.prediction_filename == "prediction_20260101T000000.zarr"
    assert prediction_result.evr_filename == "prediction_20260101T000000.evr"
    assert prediction_result.first_ping_time == pd.Timestamp("2026-01-01T00:00:05Z")
    assert prediction_result.last_ping_time == pd.Timestamp("2026-01-01T00:09:55Z")
    assert model.calls == [(("model-input", combined), 0.75)]
    assert evr_call == {
        "path": tmp_path / "EVR" / "prediction_20260101T000000.evr",
        "prediction": arrays[1],
        "region_classification": "hake",
    }

    masked_dataset = object()
    nasc_dataset = FakeNASCDataset()
    mask_call = {}
    nasc_call = {}

    def fake_apply_mask(**kwargs):
        mask_call.update(kwargs)
        return masked_dataset

    def fake_compute_NASC(**kwargs):
        nasc_call.update(kwargs)
        return nasc_dataset

    monkeypatch.setattr(operations_predict_hake.ep.mask, "apply_mask", fake_apply_mask)
    monkeypatch.setattr(
        operations_predict_hake.ep.commongrid,
        "compute_NASC",
        fake_compute_NASC,
    )

    nasc_result = operations_predict_hake.compute_NASC(
        operations_predict_hake.ComputeNASCWorkItem(
            nasc_filename="NASC_20260101T000000.zarr",
            prediction=prediction_result,
        ),
        operations_predict_hake.ComputeNASCSettings(output_directory=str(tmp_path / "NASC")),
    )

    assert mask_call["source_ds"] is combined
    assert mask_call["mask"] is prediction_result.hake_prediction
    assert nasc_call == {
        "ds_Sv": masked_dataset,
        "range_bin": "10m",
        "dist_bin": "0.5nmi",
    }
    assert nasc_dataset.saved == {
        "store": tmp_path / "NASC" / "NASC_20260101T000000.zarr",
        "mode": "w",
        "consolidated": True,
    }
    assert nasc_result == operations_predict_hake.ComputeNASCResult(
        nasc_filename="NASC_20260101T000000.zarr"
    )
