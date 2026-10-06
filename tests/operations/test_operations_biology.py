from pathlib import Path

import pandas as pd
import pandas.testing as pdt

from echodataflow.operations.operations_biology import (
    BiologyData,
    combine_biology_data,
    get_count_from_length_specimen,
    get_hauls_to_process,
    get_length_weight_regression,
    read_biology_data,
    write_biology_outputs,
)


def _biology_data(haul: int) -> BiologyData:
    location = {"haul": haul, "latitude": 40.0}
    return BiologyData(
        haul_info=pd.DataFrame([location]),
        specimens=pd.DataFrame([{**location, "length": 10}]),
        lengths=pd.DataFrame([{**location, "length": 10}]),
        length_counts=pd.DataFrame([{**location, "length": 10, "frequency": 1}]),
    )


def _output_paths(directory: Path) -> dict[str, Path]:
    return {
        "haul_info": directory / "haul_info.csv",
        "specimens": directory / "specimens.csv",
        "lengths": directory / "lengths.csv",
        "length_counts": directory / "length_counts.csv",
    }


def test_get_hauls_to_process_returns_sorted_unprocessed_hauls():
    valid = {3: {}, 1: {}, 2: {}}
    haul_info = pd.DataFrame({"haul": [2]})

    assert get_hauls_to_process(valid, haul_info) == [1, 3]


def test_count_operation_does_not_mutate_specimens():
    lengths = pd.DataFrame({"sex": ["female"], "length": [10], "haul": [1], "frequency": [2]})
    specimens = pd.DataFrame({"sex": ["female"], "fork_length": [10.2], "haul": [1]})
    original_specimens = specimens.copy()
    counts = get_count_from_length_specimen(lengths, specimens)

    assert counts.iloc[0]["frequency"] == 3
    pdt.assert_frame_equal(specimens, original_specimens)


def test_combine_biology_data_combines_each_dataframe():
    combined = combine_biology_data([_biology_data(1), _biology_data(2)])

    assert combined.haul_info["haul"].tolist() == [1, 2]
    assert combined.specimens["haul"].tolist() == [1, 2]
    assert combined.lengths["haul"].tolist() == [1, 2]
    assert combined.length_counts["haul"].tolist() == [1, 2]


def test_length_weight_regression_ignores_unobserved_strata():
    specimens = pd.DataFrame(
        {
            "sex": ["Female", "Female", "Male", "Male"],
            "stratum": pd.Categorical([1, 1, 2, 2], categories=[1, 2, 3]),
            "length": [10, 20, 12, 24],
            "organism_weight": [5, 20, 7, 28],
        }
    )

    regression = get_length_weight_regression(specimens)

    assert set(regression["stratum"]) == {1, 2}
    assert set(regression["sex"]) == {"female", "male", "all"}
    assert len(regression) == 4


def test_write_biology_outputs_publishes_all_outputs(tmp_path):
    paths = _output_paths(tmp_path)
    stratum_path = tmp_path / "stratum_mean.csv"
    data = _biology_data(1)
    stratum_mean = pd.DataFrame({"stratum": [1], "weight_mean": [2.5]})

    write_biology_outputs(data, stratum_mean, paths, stratum_path)
    loaded = read_biology_data(paths)

    pdt.assert_frame_equal(loaded.haul_info, data.haul_info)
    pdt.assert_frame_equal(loaded.specimens, data.specimens)
    pdt.assert_frame_equal(loaded.lengths, data.lengths)
    pdt.assert_frame_equal(loaded.length_counts, data.length_counts)
    pdt.assert_frame_equal(pd.read_csv(stratum_path, index_col=0), stratum_mean)
    assert not list(tmp_path.glob(".*.tmp"))
    assert not list(tmp_path.glob(".*.bak"))
