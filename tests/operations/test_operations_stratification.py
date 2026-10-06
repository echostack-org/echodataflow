import pandas as pd
import pandas.testing as pdt

from echodataflow.operations.operations_stratification import assign_stratum


def test_assign_stratum_returns_copy_with_latitude_based_strata():
    observations = pd.DataFrame({"latitude": [-90.0, 40.0, 50.0, 90.0]})
    original = observations.copy()
    definitions = pd.DataFrame(
        {
            "stratum": [1, 2],
            "latitude_northern_limit": [45.0, 55.0],
        }
    )

    result = assign_stratum(observations, definitions)

    pdt.assert_frame_equal(observations, original)
    assert result["stratum"].astype(int).tolist() == [1, 1, 2, 3]


def test_assign_stratum_supports_unsorted_open_ended_stratum():
    observations = pd.DataFrame({"latitude": [35.0, 40.0, 50.0, 60.0]})
    definitions = pd.DataFrame(
        {
            "stratum": [7, 3, 1, 6],
            "latitude_northern_limit": [None, 43.0, 36.0, 55.0],
        }
    )

    result = assign_stratum(observations, definitions)

    assert result["stratum"].astype(int).tolist() == [1, 3, 6, 7]
