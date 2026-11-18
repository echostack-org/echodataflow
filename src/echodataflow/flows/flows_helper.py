import subprocess
import tempfile
import threading
from pathlib import Path

from prefect import flow, get_run_logger


@flow(log_prints=True)
def flow_file_upload(
    src_dir: str,
    dest_dir: str,
    exclude_subdirs: list[str],
    max_age: int = -1,
    timeout_seconds: float = 600,
):
    """Upload files via rclone.

    Parameters
    ----------
    src_dir : str
        Source directory to upload files from.
    dest_dir : str
        Destination directory for the upload.
    exclude_subdirs : list[str]
        Subdirectories to exclude from the upload.
    max_age : int, optional
        Maximum age of files to upload in hours; -1 means no limit.
    timeout_seconds : float, optional
        Maximum time in seconds for rclone to run, by default 600.
    """
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    src_dir = Path(src_dir).expanduser().resolve()
    if not src_dir.exists():
        print(f"Skipping file upload: source directory does not exist: {src_dir}")
        return

    logger = get_run_logger()
    exclude_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", prefix="upload_exclude_folders_", suffix=".txt",
            dir=src_dir, delete=False,
        ) as file:
            exclude_path = Path(file.name)
            file.write(".DS_Store\nupload_exclude_folders_*.txt\n")
            for subdir in exclude_subdirs:
                file.write(f"/{subdir}/**\n")

        logger.info("Exclusion list file: %s", exclude_path)
        command = ["rclone", "copy", "-v", "--s3-no-check-bucket", "--no-traverse"]
        if max_age != -1:
            command.extend(["--max-age", f"{max_age}h"])
        command.extend([str(src_dir), dest_dir, "--exclude-from", str(exclude_path)])

        with subprocess.Popen(
            command, cwd=src_dir, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, bufsize=1,
        ) as process:
            def log_output():
                for line in process.stdout:
                    logger.info("%s", line.rstrip())

            reader = threading.Thread(target=log_output, daemon=True)
            reader.start()
            try:
                returncode = process.wait(timeout=timeout_seconds)
            except BaseException:
                process.kill()
                process.wait()
                raise
            finally:
                reader.join()
            if returncode:
                raise subprocess.CalledProcessError(returncode, command)
    finally:
        if exclude_path is not None:
            exclude_path.unlink(missing_ok=True)
