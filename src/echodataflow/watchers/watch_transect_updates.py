import argparse
import time
from pathlib import Path

from echodataflow.operations.operations_watchdog import watch_transect_file


def main():
    parser = argparse.ArgumentParser(
        description="Watch a transect CSV file and register transect updates."
    )

    parser.add_argument(
        "path",
        type=Path,
        help="Path to the transect CSV file to watch.",
    )

    parser.add_argument(
        "--db-path",
        type=str,
        required=True,
        help="Processing database path or SQLAlchemy database URL.",
    )

    args = parser.parse_args()

    target_file = args.path.resolve()
    db_path = args.db_path

    if not target_file.exists():
        raise FileNotFoundError(
            f"Transect file does not exist: {target_file}"
        )

    observer = watch_transect_file(
        target_file,
        db_path=db_path,
    )

    print(f"Watching {target_file}")
    print(f"Processing ledger: {db_path}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == "__main__":
    main()