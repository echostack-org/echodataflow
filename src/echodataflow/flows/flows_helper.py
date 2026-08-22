from pathlib import Path
import datetime

from prefect import flow


@flow(timeout_seconds=600, log_prints=True)
def flow_file_upload(
    src_dir: str,
    dest_dir: str,
    exclude_subdirs: list[str],
    max_age: int = -1,
):
    """
    Upload files via rclone.

    Parameters
    ----------
    src_dir : str
        Source directory to upload files from.
    dest_dir : str, optional
        Destination directory to upload files to, by default "osn_sdsc_hake:/agr230002-bucket01/prefect_test".
    exclude_subdirs : list, optional
        List of subdirectories to exclude from the upload, by default [].
    max_age : int, optional
        Maximum age of files to upload in hours, by default -1 (no limit).
    """
    # TODO: need to fix dependency issue
    # TODO: consider moving it back to top imports
    from prefect_shell import ShellOperation

    # Generate upload_exclude_folders.txt
    exclude_filename = (
        f"upload_exclude_folders_"
        f"{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d_%H%M%S')}.txt"
    )
    exclude_path = Path(src_dir) / exclude_filename
    with open(exclude_path, "w") as file:
        # Add .DS_Store to exclude list
        file.write(".DS_Store\n")
        # Exclude all upload_exclude_folders_*.txt files (i.e. this file and any leftover ones)
        file.write("upload_exclude_folders_*.txt\n")
        # Add other subdirectories
        for subdir in exclude_subdirs:
            file.write(f"/{subdir}/**\n")

    # Potentially long running so using a context manager
    if max_age == -1:
        command = (
            "rclone copy -v --s3-no-check-bucket --no-traverse "
            f"{src_dir} {dest_dir} --exclude-from {exclude_path}"
        )
    else:
        command = (
            f"rclone copy -v --s3-no-check-bucket --max-age {max_age}h "
            f"--no-traverse {src_dir} {dest_dir} --exclude-from {exclude_path}"
        )

    with ShellOperation(
        commands=[command],
        working_dir=src_dir,
    ) as file_upload_operation:
        # Trigger runs the process in the background
        file_upload_process = file_upload_operation.trigger()

        # Wait for the process to finish
        file_upload_process.wait_for_completion()

        # Print results
        file_upload_process.fetch_result()

    # Remove the exclude list file after upload
    exclude_path.unlink(missing_ok=True)
