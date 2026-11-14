"""Publish full-resolution CPS transects without acoustic subsampling."""

import hashlib
import json
import os
import shutil
import sqlite3
import uuid
from contextlib import ExitStack, closing
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr


def _fingerprint(paths, start, end):
    sources = []
    for path in paths:
        metadata = []
        for name in (".zmetadata", ".zattrs", ".zgroup", "zarr.json", ".revision"):
            item = path / name
            if item.exists():
                metadata.append((name, hashlib.sha256(item.read_bytes()).hexdigest()))
        sources.append((path.name, path.stat().st_mtime_ns, metadata))
    payload = ["cps-transect-cache-v1", start.isoformat(), end.isoformat(), sources]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _published(path, revision):
    marker = path / ".revision"
    return marker.exists() and marker.read_text(encoding="utf-8") == revision


def _publish(data, path, revision):
    """Finish a sibling staging store before replacing an existing cache."""
    stage = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    backup = path.with_name(path.name + ".old-" + uuid.uuid4().hex)
    clean = data.copy(deep=False)
    for variable in clean.variables:
        clean[variable].encoding = {}
    sample = next(d for d in clean.Sv.dims if d not in ("channel", "ping_time"))
    clean = clean.chunk({"channel": 1, "ping_time": 128, sample: 512})
    try:
        clean.to_zarr(stage, mode="w", consolidated=True, zarr_format=2)
        (stage / ".revision").write_text(revision, encoding="utf-8")
        if path.exists():
            os.replace(path, backup)
        try:
            os.replace(stage, path)
        except OSError:
            if backup.exists():
                os.replace(backup, path)
            raise
        if backup.exists():
            shutil.rmtree(backup)
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def prepare_transect_caches(
    path_cps,
    path_cache,
    database,
    *,
    preview_max_pings=None,
    preview_max_range_samples=None,
):
    """Backfill all completed intervals; unchanged stores do no acoustic reads.

    Original per-file CPS products are read only. All acquired samples and
    coordinates are retained, with no interpolation or subsampling.
    Existing NASC outputs and ledger completion states are not changed.
    """
    path_cps, path_cache, database = (
        Path(path_cps),
        Path(path_cache),
        Path(database).resolve(),
    )
    if not database.exists():
        return []
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as con:
        con.row_factory = sqlite3.Row
        try:
            rows = con.execute(
                "SELECT * FROM transects WHERE status='completed' ORDER BY end_time DESC"
            ).fetchall()
            files = con.execute(
                "SELECT * FROM sv_cps WHERE status='completed' ORDER BY first_ping_time"
            ).fetchall()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower():
                return []
            raise
    output = path_cache / "transects"
    output.mkdir(parents=True, exist_ok=True)
    written = []
    for row in rows:
        number = str(row["transect_part"]).zfill(3)
        if not number.isdigit():
            raise ValueError(f"Invalid transect part: {number!r}")
        start, end = (
            pd.to_datetime(row["start_time"], utc=True),
            pd.to_datetime(row["end_time"], utc=True),
        )
        selected = [
            record["cps_filename"]
            for record in files
            if pd.to_datetime(record["first_ping_time"], utc=True) <= end
            and pd.to_datetime(record["last_ping_time"], utc=True) >= start
        ]
        if not selected:
            continue
        paths = [path_cps / name for name in selected]
        if any(not path.exists() for path in paths):
            print(f"Transect {number}: registered source files are missing; cache not rebuilt.")
            continue
        revision = _fingerprint(paths, start, end)
        full = output / f"transect_{number}.zarr"
        if _published(full, revision):
            continue
        if not _published(full, revision):
            print(f"Preparing full-resolution transect {number} from {len(paths)} CPS stores.")
            with ExitStack() as stack:
                parts = []
                for path in paths:
                    source = stack.enter_context(xr.open_zarr(path))
                    names = [
                        name
                        for name in (
                            "Sv",
                            "Sv_water_column",
                            "Sv_masked",
                            "depth",
                            "echo_range",
                            "longitude",
                            "latitude",
                            "frequency_nominal",
                        )
                        if name in source
                    ]
                    part = source[names]
                    if any(var.chunks is not None for var in part.variables.values()):
                        part = part.chunk({d: -1 for d in part.dims if d != "ping_time"})
                    parts.append(part)
                data = xr.concat(
                    parts,
                    dim="ping_time",
                    data_vars="minimal",
                    coords="minimal",
                    compat="equals",
                    join="outer",
                )
                times = data.ping_time.values
                keep = (times >= start.tz_convert(None).to_datetime64()) & (
                    times <= end.tz_convert(None).to_datetime64()
                )
                data = data.isel(ping_time=np.flatnonzero(keep))
                times = data.ping_time.values.astype("datetime64[ns]")
                if not times.size:
                    raise ValueError(f"Transect {number}: no pings in the interval.")
                if np.isnat(times).any() or np.any(np.diff(times.astype("int64")) <= 0):
                    raise ValueError(
                        f"Transect {number}: invalid, duplicate or unordered ping times; source order was not changed."
                    )
                data.attrs = dict(
                    data.attrs,
                    transect_part=number,
                    transect_start=start.isoformat(),
                    transect_end=end.isoformat(),
                    source_cps_filenames=json.dumps(selected),
                    cache_kind="full_resolution",
                )
                _publish(data, full, revision)
        written.append(number)
    return written
