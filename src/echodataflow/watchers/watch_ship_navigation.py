"""Continuously record ship navigation from an NMEA 0183 serial stream."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import serial
import yaml
import time

def nmea_to_decimal(value: str, hemisphere: str) -> float:
    """Convert an NMEA coordinate to decimal degrees."""
    raw = float(value)

    degrees = int(raw // 100)
    minutes = raw - degrees * 100

    decimal = degrees + minutes / 60.0

    if hemisphere in {"S", "W"}:
        decimal *= -1

    return decimal


def parse_rmc(sentence: str) -> dict[str, object] | None:
    """Parse a valid GPRMC sentence."""
    if not sentence.startswith("$GPRMC"):
        return None

    fields = sentence.split(",")

    if len(fields) < 10 or fields[2] != "A":
        return None

    time_string = fields[1]
    latitude = fields[3]
    latitude_hemisphere = fields[4]
    longitude = fields[5]
    longitude_hemisphere = fields[6]
    speed_knots = fields[7]
    course_deg = fields[8]
    date_string = fields[9]

    if not all(
        [
            time_string,
            date_string,
            latitude,
            latitude_hemisphere,
            longitude,
            longitude_hemisphere,
        ]
    ):
        return None

    timestamp = datetime.strptime(
        date_string + time_string.split(".")[0],
        "%d%m%y%H%M%S",
    ).replace(tzinfo=timezone.utc)

    return {
        "timestamp_utc": timestamp,
        "latitude": nmea_to_decimal(latitude, latitude_hemisphere),
        "longitude": nmea_to_decimal(longitude, longitude_hemisphere),
        "speed_knots": float(speed_knots) if speed_knots else None,
        "course_deg": float(course_deg) if course_deg else None,
    }


def write_parquet_chunk(
    records: list[dict[str, object]],
    output_dir: Path,
) -> Path:
    """Write one completed navigation minute to Parquet."""
    if not records:
        raise ValueError("Cannot write an empty navigation chunk.")

    output_dir.mkdir(parents=True, exist_ok=True)

    first_timestamp = records[0]["timestamp_utc"]

    filename = (
        f"ship_navigation_"
        f"{first_timestamp:%Y%m%d_%H%M}.parquet"
    )

    output_path = output_dir / filename

    dataframe = pd.DataFrame(records)
    dataframe.to_parquet(output_path, index=False)

    return output_path


def load_navigation_config(config_path: Path) -> dict:
    """Load navigation configuration from the cruise params YAML."""
    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if "navigation" not in config:
        raise ValueError(
            f"No 'navigation' section found in {config_path}"
        )

    return config["navigation"]


def watch_navigation(
    port: str,
    baudrate: int,
    output_dir: Path,
) -> None:
    """Continuously read GPS positions and write one Parquet file per minute."""
    print(f"Reading navigation from {port} @ {baudrate} baud")
    print(f"Writing navigation to {output_dir}")
    print("Press Ctrl+C to stop.")

    records: list[dict[str, object]] = []
    current_minute = None

    try:
        while True:
            try:
                with serial.Serial(
                    port=port,
                    baudrate=baudrate,
                    timeout=1,
                ) as stream:
                    print(f"Connected to {port}.")

                    while True:
                        sentence = (
                            stream.readline()
                            .decode("ascii", errors="ignore")
                            .strip()
                        )

                        navigation = parse_rmc(sentence)

                        if navigation is None:
                            continue

                        timestamp = navigation["timestamp_utc"]
                        minute = timestamp.replace(
                            second=0,
                            microsecond=0,
                        )

                        if current_minute is None:
                            current_minute = minute

                        # A new UTC minute has started.
                        if minute != current_minute:
                            output_path = write_parquet_chunk(
                                records,
                                output_dir,
                            )

                            print(
                                f"Saved {len(records)} positions -> "
                                f"{output_path.name}"
                            )

                            records = []
                            current_minute = minute

                        records.append(navigation)

                        print(
                            f"{timestamp.isoformat()} | "
                            f"lat={navigation['latitude']:.6f} | "
                            f"lon={navigation['longitude']:.6f} | "
                            f"speed={navigation['speed_knots']} kn | "
                            f"course={navigation['course_deg']}°"
                        )

            except serial.SerialException as exc:
                print(
                    f"Serial connection lost: {exc}\n"
                    f"Retrying {port} in 5 seconds..."
                )
                time.sleep(5)

    except KeyboardInterrupt:
        print("\nStopping navigation reader.")

        # Preserve the unfinished minute when stopped manually.
        if records:
            output_path = write_parquet_chunk(
                records,
                output_dir,
            )

            print(
                f"Saved final {len(records)} positions -> "
                f"{output_path.name}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Record ship navigation from an NMEA serial stream."
    )

    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the cruise parameter YAML.",
    )

    args = parser.parse_args()

    navigation_config = load_navigation_config(args.config)

    watch_navigation(
        port=navigation_config["port"],
        baudrate=int(navigation_config["baudrate"]),
        output_dir=Path(navigation_config["path_output"]),
    )


if __name__ == "__main__":
    main()