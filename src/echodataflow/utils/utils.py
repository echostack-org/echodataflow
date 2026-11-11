import re
import datetime
import numpy as np
import pandas as pd


def round_up_mins(dt: datetime.datetime, slice_mins: int) -> datetime.datetime:
    """Round down datetime to nearest interval minutes.
    
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