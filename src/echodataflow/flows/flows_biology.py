import re
from pathlib import Path
import numpy as np
import pandas as pd

import configparser, s3fs

from echodataflow.utils.const import TS_L_PARAMS, INFO_DATAFRAME_MAPPING

from prefect import task, flow
from prefect.events import emit_event


# Set up paths
data_path = "/Users/feresa/code_git/echodataflow/temp_bio"
# csv_path = data_path / "bio_csv"

# # Initialize dataframes
# df_haul_info_all = pd.DataFrame(
#     columns=["operation_number", "timestamp", "latitude", "longitude"]
# )
# df_specimen_all = pd.DataFrame(
#     columns=["operation_number", "partition", "species", "sex", "rounded_length", "frequency"]
# )
# df_length_all = pd.DataFrame(
#     columns=[
#         "operation_number", "partition", "species",
#         "length_type", "length", "sex", "organism_weight", "barcode"
#     ]
# )
# df_length_count_all = pd.DataFrame(
#     columns=["sex", "rounded_length", "frequency", "operation_number"]
# )
# df_haul_info_all.to_csv(data_path / "haul_info_all.csv")
# df_specimen_all.to_csv(data_path / "specimen_all.csv")
# df_length_all.to_csv(data_path / "length_all.csv")
# df_length_count_all.to_csv(data_path / "length_count_all.csv")



def get_valid_hauls(
    bio_filenames: dict,
):
    # Pull out haul numbers
    HAUL_NUM_PATTERN = rf"(\w+)?(?P<haul_num>\d{{3}})_\w+\.xlsx"
    haul_num_all = {k: set() for k in bio_filenames.keys()}
    for file_type in bio_filenames.keys():
        for fname in bio_filenames[file_type]:
            match_str = re.match(HAUL_NUM_PATTERN, Path(fname).name)
            if match_str is not None:
                haul_num_all[file_type].add(int(match_str["haul_num"]))

    # Each haul number should have 4 files as defined above
    valid_hauls = set.union(*haul_num_all.values())
    haul_num_to_remove = set()
    for file_type in bio_filenames.keys():
        haul_num_diff = valid_hauls.difference(haul_num_all[file_type])
        if haul_num_diff:
            haul_num_to_remove.update(haul_num_diff)
    valid_hauls.difference_update(haul_num_to_remove)
    return valid_hauls


def get_count_from_length_specimen(
    df_length: pd.DataFrame,
    df_specimen: pd.DataFrame,
) -> pd.DataFrame:
    """
    Get length count from length and specimen dataframes.
    """
    # Round fish length to nearest integer
    df_specimen["length"] = df_specimen["fork_length"].round(0).astype(int)

    # Get length count from both dataframes
    specimen_counts = (
        df_specimen.groupby(["sex", "length", "haul"]).size()
        .reset_index(name="frequency")
    )
    length_counts = (
        df_length.groupby(["sex", "length", "haul"])
        .agg({"frequency": "sum"}).reset_index()
    )

    df_combined = pd.merge(
        specimen_counts.reset_index(),
        length_counts.reset_index(),
        on=["sex", "length", "haul"],
        how="outer"
    ).fillna(0)
    df_combined["frequency"] = (df_combined["frequency_x"] + df_combined["frequency_y"]).astype(int)

    return df_combined[["sex", "length", "frequency", "haul"]]


def get_length_weight_regression(df_specimen: pd.DataFrame) -> pd.DataFrame:
    """
    Get length-weight regression coefficients for male, female, and all fish combined.
    """
    # Male, female separately
    df_regres = df_specimen.groupby(["sex", "stratum"]).apply(
        lambda df: pd.Series(
            np.polyfit(np.log10(df["length"]), np.log10(df["organism_weight"]), 1),
            index=["p1", "p2"],
        ),
        include_groups = False
    ).reset_index()

    # All fish combined
    df_all = df_specimen.groupby("stratum").apply(
        lambda df: pd.Series(
            np.polyfit(np.log10(df["length"]),
                    np.log10(df["organism_weight"]), 1),
            index=["p1", "p2"],
        ),
        include_groups = False
    ).reset_index()
    df_all["sex"] = "all"

    # Combine all and make everything lowercase
    df_regres = pd.concat([df_regres, df_all]).reset_index()
    df_regres["sex"] = df_regres["sex"].str.lower()

    return df_regres


def add_stratum(df, df_stratum):
    # Create latitude bins from the stratum definitions
    lat_bins = [-90.0] + df_stratum["latitude_northern_limit"].tolist() + [90.0]
    lat_labels = df_stratum["stratum"].tolist() + [max(df_stratum["stratum"]) + 1]

    # Add stratum column based on latitude
    df["stratum"] = pd.cut(
        df["latitude"],
        bins=lat_bins,
        labels=lat_labels,
        include_lowest=True
    )

    return df


def get_sigma_bs_mean_stratum(
    df_length_count: pd.DataFrame,
) -> pd.DataFrame:
    # Compute sigma_bs for each row
    df_length_count["sigma_bs"] = 10 ** ((
        TS_L_PARAMS["slope"] * np.log10(df_length_count["length"])
        + TS_L_PARAMS["intercept"]
    ) / 10)

    # Get mean sigma_bs for each stratum
    return df_length_count.groupby("stratum").apply(
        lambda df: pd.Series(
            (df["sigma_bs"] * df["frequency"]).sum() / df["frequency"].sum(),
            index=["sigma_bs_mean"],
        ),
        include_groups=False
    ).reset_index()


def get_weight_mean_stratum(
    df_length_count: pd.DataFrame,
    df_length_weight_regression: pd.DataFrame,
) -> pd.DataFrame:
    # Merge length-weight regression coefficients
    df_merged = pd.merge(
        df_length_count,
        df_length_weight_regression[df_length_weight_regression["sex"] == "all"][["stratum", "p1", "p2"]],
        on="stratum",
        how="left"
    )

    # Then compute weight using the length-weight relationship: W = 10^(p1 * log10(L) + p2)
    df_merged["weight"] = 10 ** (
        df_merged["p1"] * np.log10(df_merged["length"]) + df_merged["p2"]
    )

    # Get mean weight for each stratum
    return df_merged.groupby("stratum").apply(
        lambda df: pd.Series(
            (df["weight"] * df["frequency"]).sum() / df["frequency"].sum(),
            index=["weight_mean"],
        ),
        include_groups=False
    ).reset_index()


@flow(log_prints=True)
def flow_ingest_haul(
    path_vm_local: str = data_path,
    path_bio_files: str = "BIO_CSV_CLOUD_LOCATION",
    cred_file: str = "CREDENTIAL_FILE",
    file_haul_info_all: str = "haul_info_all.csv",
    file_specimen_all: str = "specimen_all.csv",
    file_length_all: str = "length_all.csv",
    file_length_count_all: str = "length_count_all.csv",
    file_stratum_mean: str = "stratum_mean.csv",
):

    # Assemble full paths
    path_vm_local: Path = Path(path_vm_local)
    file_haul_info_all: Path = path_vm_local / file_haul_info_all
    file_specimen_all: Path = path_vm_local / file_specimen_all
    file_length_all: Path = path_vm_local / file_length_all
    file_length_count_all: Path = path_vm_local / file_length_count_all
    file_stratum_mean: Path = path_vm_local / file_stratum_mean

    # Get cloud bucket
    config = configparser.ConfigParser()
    config.read(cred_file)
    fs = s3fs.S3FileSystem(
        key=config["osn_sdsc_hake"]["access_key_id"],
        secret=config["osn_sdsc_hake"]["secret_access_key"],
        client_kwargs={"endpoint_url": config["osn_sdsc_hake"]["endpoint"]},
    )

    bio_filenames = {
        "length": fs.glob(f"{path_bio_files}/*/*_LFdata.xlsx"),
        "specimen": fs.glob(f"{path_bio_files}/*/*_specimens.xlsx"),
        "catch": fs.glob(f"{path_bio_files}/*/*_CatchPerc.xlsx"),
        "info": fs.glob(f"{path_bio_files}/*/*_NetConfig.xlsx"),
    }

    # Exist if no file present
    if not any(bio_filenames.values()):
        print(f"No biology files found.")
        return

    # Get valid haul numbers (those with 4 files)
    hauls_valid = get_valid_hauls(bio_filenames)
    print("Valid hauls:", hauls_valid)

    # Get hauls to process
    if not file_haul_info_all.exists():
        df_haul_info_all = pd.DataFrame()
        hauls_processed = set()
    else:
        df_haul_info_all = pd.read_csv(file_haul_info_all, index_col=0)
        hauls_processed = set(df_haul_info_all["haul"].unique())
    hauls_to_process = list(hauls_valid.difference(hauls_processed))

    if not hauls_to_process:  # if there are hauls to process
        print(f"No hauls to process.")
        return
    else:
        print(
            f"Processing {len(hauls_to_process)} hauls for :\n",
            hauls_to_process
        )
        # Load dataframes from all hauls to process
        df_length = []
        df_specimen = []
        df_info = []
        for haul_num in hauls_to_process:
            with fs.open(f"{path_bio_files}/LengthFreq/{haul_num:03d}_LFdata.xlsx") as f:
                df_length_temp = pd.read_excel(f, index_col=0, sheet_name="Codend").reset_index().drop("Sum", axis=1)
                df_length_temp = df_length_temp.melt(
                    id_vars=["length"],
                    var_name="sex",
                    value_name="frequency"
                ).assign(haul=haul_num)
                df_length_temp["frequency"] = df_length_temp["frequency"].fillna(0).astype(int)
                df_length.append(df_length_temp)
            with fs.open(f"{path_bio_files}/Specimens/{haul_num:03d}_specimens.xlsx") as f:
                df_specimen.append(
                    pd.read_excel(f, index_col=0, sheet_name="Codend")
                    .reset_index().assign(haul=haul_num)
                )
            with fs.open(f"{path_bio_files}/NetConfig/202506_{haul_num:03d}_NetConfig.xlsx") as f:
                df_info_temp = (
                    pd.read_excel(f, index_col=0, sheet_name="ButtonPresses").reset_index()
                    .rename(columns=INFO_DATAFRAME_MAPPING)
                )
                df_info.append(
                    # reset index to get haul number into a column
                    df_info_temp[df_info_temp["button"] == "NIW"][["haul", "timestamp", "latitude", "longitude"]]
                )
        df_length = pd.concat(df_length, ignore_index=True)
        df_specimen = pd.concat(df_specimen, ignore_index=True)
        df_info = pd.concat(df_info, ignore_index=True)

        # Combined length frequency from length and specimen dataframes
        df_length_count = get_count_from_length_specimen(df_length, df_specimen)

        # Add haul number and lat/lon for downstream stratification
        df_specimen = pd.merge(
            df_specimen,
            df_info,
            on="haul",
            how="left"
        )
        df_length = pd.merge(
            df_length,
            df_info,
            on="haul",
            how="left"
        )
        df_length_count = pd.merge(
            df_length_count,
            df_info,
            on="haul",
            how="left"
        )

        # Update df_haul_info_all, df_specimen_all, df_length_all
        df_specimen_all = pd.read_csv(file_specimen_all, index_col=0) if file_specimen_all.exists() else pd.DataFrame()
        df_length_all = pd.read_csv(file_length_all, index_col=0) if file_length_all.exists() else pd.DataFrame()
        df_length_count_all = pd.read_csv(file_length_count_all, index_col=0) if file_length_count_all.exists() else pd.DataFrame()
        df_haul_info_all = pd.read_csv(file_haul_info_all, index_col=0) if file_haul_info_all.exists() else pd.DataFrame()

        df_specimen_all = pd.concat([df_specimen_all, df_specimen], ignore_index=True)
        df_length_all = pd.concat([df_length_all, df_length], ignore_index=True)
        df_length_count_all = pd.concat([df_length_count_all, df_length_count], ignore_index=True)
        df_haul_info_all = pd.concat([df_haul_info_all, df_info], ignore_index=True)

        # Add stratrum info to df_specimen and df_length_count based on latitude
        df_stratum = pd.read_csv(
            Path(__file__).parent / "inpfc_def.csv", index_col=0
        ).reset_index().rename(columns={"stratum_num": "stratum"})
        df_specimen_all = add_stratum(df_specimen_all, df_stratum)
        df_length_all = add_stratum(df_length_all, df_stratum)
        df_length_count_all = add_stratum(df_length_count_all, df_stratum)
        df_haul_info_all = add_stratum(df_haul_info_all, df_stratum)

        # Save updated dataframes
        df_specimen_all.to_csv(file_specimen_all)
        df_length_all.to_csv(file_length_all)
        df_length_count_all.to_csv(file_length_count_all)
        df_haul_info_all.to_csv(file_haul_info_all)

        # Compute length-weight relationship for each stratum
        # Separately for: male, female, all fish combined
        df_length_weight_regression = get_length_weight_regression(df_specimen_all)

        # Compute mean sigma_bs and mean weight for each stratum
        # columns: stratum, sigma_bs_mean, weight_mean
        df_stratum = pd.merge(
            df_stratum,
            get_sigma_bs_mean_stratum(df_length_count_all),
            on="stratum",
            how="outer"
        )
        df_stratum = pd.merge(
            df_stratum,
            get_weight_mean_stratum(df_length_count_all, df_length_weight_regression),
            on="stratum",
            how="outer"
        )
        df_stratum.to_csv(file_stratum_mean)

        # Emit custom event when new hauls are processed
        emit_event(
            event="haul.ingested",
            resource={"prefect.resource.id": "ingest_haul"}
        )
