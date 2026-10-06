from __future__ import annotations

import configparser
from pathlib import Path

import pandas as pd
import s3fs
from prefect import flow

from echodataflow.operations.operations_biology import (
    assign_strata,
    combine_biology_data,
    compute_stratum_estimates,
    get_hauls_to_process,
    load_haul_data,
    read_biology_data,
    write_biology_outputs,
)
from echodataflow.utils.trawl_files import discover_trawl_files, get_valid_hauls


@flow(log_prints=True)
def flow_ingest_haul(
    path_vm_local: str = "VM_LOCAL_PATH",
    path_bio_files: str = "BIO_CSV_CLOUD_LOCATION",
    cred_file: str = "CREDENTIAL_FILE",
    file_haul_info_all: str = "haul_info_all.csv",
    file_specimen_all: str = "specimen_all.csv",
    file_length_all: str = "length_all.csv",
    file_length_count_all: str = "length_count_all.csv",
    file_stratum_mean: str = "stratum_mean.csv",
    path_stratum_def: str = "inpfc_def.csv",
):
    """Ingest complete cloud trawl hauls and recompute biological estimates."""
    # Assemble the four accumulated output paths and the derived estimate path
    output_directory = Path(path_vm_local)
    output_paths = {
        "haul_info": output_directory / file_haul_info_all,
        "specimens": output_directory / file_specimen_all,
        "lengths": output_directory / file_length_all,
        "length_counts": output_directory / file_length_count_all,
    }
    stratum_mean_path = output_directory / file_stratum_mean

    # Connect to the S3-compatible biology data store
    config = configparser.ConfigParser()
    config.read(cred_file)
    fs = s3fs.S3FileSystem(
        key=config["osn_sdsc_hake"]["access_key_id"],
        secret=config["osn_sdsc_hake"]["secret_access_key"],
        client_kwargs={"endpoint_url": config["osn_sdsc_hake"]["endpoint"]},
    )

    # A haul is ready only after all four expected workbook types are present
    discovered_files = discover_trawl_files(fs, path_bio_files)
    if not any(discovered_files.values()):
        print("No biology files found.")
        return

    valid_hauls = get_valid_hauls(discovered_files)
    print("Valid hauls:", list(valid_hauls))

    # Process only complete hauls not already in accumulated output
    existing_hauls = read_biology_data(output_paths)
    hauls_to_process = get_hauls_to_process(valid_hauls, existing_hauls.haul_info)
    if not hauls_to_process:
        print("No new hauls to process.")
        return

    print(f"Processing {len(hauls_to_process)} hauls:\n", hauls_to_process)
    # Load every haul before publishing anything so one failure aborts the batch
    new_hauls = combine_biology_data(
        load_haul_data(fs, haul_num, valid_hauls[haul_num]) for haul_num in hauls_to_process
    )
    combined_hauls = combine_biology_data([existing_hauls, new_hauls])

    # Assign strata to accumulated records, then recompute estimates from full history
    stratum_definitions = (
        pd.read_csv(path_stratum_def, index_col=0)
        .reset_index()
        .rename(columns={"stratum_num": "stratum"})
    )
    combined_hauls = assign_strata(combined_hauls, stratum_definitions)
    stratum_mean = compute_stratum_estimates(combined_hauls, stratum_definitions)

    # Update all accumulated datasets and estimates as one rollback-protected update
    write_biology_outputs(
        combined_hauls,
        stratum_mean,
        output_paths,
        stratum_mean_path,
    )
