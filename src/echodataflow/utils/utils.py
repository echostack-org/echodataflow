import configparser
import subprocess
from pathlib import Path

import s3fs
import re
import datetime
import numpy as np
import pandas as pd


def round_up_mins(dt: datetime.datetime, slice_mins: int) -> datetime.datetime:
    """Round up datetime to nearest interval minutes.
    
    Parameters
    ----------
    dt : datetime.datetime
        Datetime to round
    slice_mins : int
        Interval in minutes to round to
        
    Returns
    -------
    datetime.datetime
        Rounded datetime
    """
    # Round up minutes and handle day rollover
    minutes_since_midnight = dt.hour * 60 + dt.minute
    rounded_minutes = ((minutes_since_midnight + slice_mins - 1) // slice_mins) * slice_mins
    
    days_to_add = rounded_minutes // (24 * 60)  # Number of days to add
    final_minutes = rounded_minutes % (24 * 60)  # Minutes in the final day
    
    return (dt
            .replace(hour=final_minutes // 60,
                    minute=final_minutes % 60,
                    second=0,
                    microsecond=0) 
            + datetime.timedelta(days=days_to_add))


def get_slice_start_end_times(
    end_time: datetime.datetime, 
    slice_mins: int, 
    num_slices: int
):
    """
    Get slice start and end times based on the end time of the last slice.
    
    Returns
    -------
    tuple
        Start and end times as datetime objects.
    """
    end_time = pd.to_datetime(end_time)
    slice_mins = pd.to_timedelta(f"{slice_mins}min")
    start_time = sorted([end_time - s * slice_mins for s in np.arange(num_slices)+1])
    end_time = [st + slice_mins for st in start_time]
    
    return start_time, end_time


# Extract datetime objects from filenames
def extract_datetime_from_filename(filename):
    """
    Extracts a datetime object from a filename that contains a date and time in the format DYYYYMMDD-THHMMSS.
    """
    match = re.search(r'D(\d{8})-T(\d{6})', filename)
    if match:
        date_str = match.group(1)
        time_str = match.group(2)
        return datetime.datetime.strptime(f"{date_str} {time_str}", "%Y%m%d %H%M%S").replace(tzinfo=datetime.timezone.utc)
    return None

def resolve_rclone_s3_path(remote_path: str) -> tuple[s3fs.S3FileSystem, str]:
    """Return an S3 filesystem and bucket/path for ``remote:bucket/path/to/dir``.
    """
    remote, separator, path = remote_path.partition(":")
    if path.startswith("//"):
        raise ValueError("Expected an rclone path in the form remote:bucket/path/to/dir")
    path = path.lstrip("/").rstrip("/")
    if not separator or not remote.strip() or not path or path.startswith(":"):
        raise ValueError("Expected an rclone path in the form remote:bucket/path/to/dir")

    result = subprocess.run(
        ["rclone", "config", "file"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
        timeout=30,
    )

    lines = result.stdout.strip().splitlines()
    if not lines:
        raise RuntimeError("rclone did not return a config file location")
    config_path = Path(lines[-1]).expanduser()
    config = configparser.ConfigParser(interpolation=None)
    with config_path.open() as stream:
        config.read_file(stream)

    if remote not in config:
        raise ValueError(f"Remote {remote!r} is missing from the rclone config")
    section = config[remote]
    if section.get("type", "").lower() != "s3":
        raise ValueError(f"Remote {remote!r} must be an S3 remote")

    # Require explicit credentials from the selected rclone remote.
    key = section.get("access_key_id")
    secret = section.get("secret_access_key")
    if not key or not secret:
        raise ValueError(f"Remote {remote!r} must provide both S3 credential fields")
    client_kwargs = {}
    for source, target in (("endpoint", "endpoint_url"), ("region", "region_name")):
        if section.get(source):
            client_kwargs[target] = section[source]
    fs = s3fs.S3FileSystem(
        key=key,
        secret=secret,
        token=section.get("session_token"),
        anon=False,
        client_kwargs=client_kwargs,
    )
    return fs, path
