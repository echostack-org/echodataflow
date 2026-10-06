"""Utilities for matching cloud trawl files to complete hauls."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from pathlib import Path

HAUL_NUMBER_PATTERN = re.compile(r"(?P<haul_num>\d{3})_[^_]+\.xlsx$", re.IGNORECASE)
REQUIRED_TRAWL_FILE_TYPES = ("length", "specimen", "catch", "info")


def discover_trawl_files(fs, path_bio_files: str) -> dict[str, list[str]]:
    """Discover the four file types required for a complete trawl haul."""
    return {
        "length": fs.glob(f"{path_bio_files}/*/*_LFdata.xlsx"),
        "specimen": fs.glob(f"{path_bio_files}/*/*_specimens.xlsx"),
        "catch": fs.glob(f"{path_bio_files}/*/*_CatchPerc.xlsx"),
        "info": fs.glob(f"{path_bio_files}/*/*_NetConfig.xlsx"),
    }


def get_valid_hauls(
    bio_filenames: Mapping[str, Iterable[str]],
) -> dict[int, dict[str, str]]:
    """Return complete hauls mapped to their file paths by file type.

    A haul is valid only when exactly one matching file exists for every file
    type supplied in ``bio_filenames``. Files whose names do not end in a
    three-digit haul number followed by an Excel filename suffix are ignored.
    """
    if not bio_filenames:
        return {}

    missing_types = set(REQUIRED_TRAWL_FILE_TYPES).difference(bio_filenames)
    if missing_types:
        raise ValueError(f"Missing required trawl file types: {sorted(missing_types)}")

    files_by_type: dict[str, dict[int, str]] = {}
    for file_type, filenames in bio_filenames.items():
        files_by_haul: dict[int, str] = {}
        for filename in filenames:
            match = HAUL_NUMBER_PATTERN.search(Path(filename).name)
            if match is None:
                continue

            haul_num = int(match["haul_num"])
            if haul_num in files_by_haul:
                raise ValueError(
                    f"Multiple {file_type!r} files found for haul {haul_num:03d}: "
                    f"{files_by_haul[haul_num]!r} and {filename!r}"
                )
            files_by_haul[haul_num] = filename
        files_by_type[file_type] = files_by_haul

    valid_haul_numbers = set.intersection(
        *(set(files_by_haul) for files_by_haul in files_by_type.values())
    )
    return {
        haul_num: {file_type: files_by_type[file_type][haul_num] for file_type in files_by_type}
        for haul_num in sorted(valid_haul_numbers)
    }
