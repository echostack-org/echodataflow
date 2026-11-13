import argparse
import time
from pathlib import Path

from echodataflow.operations.operations_watchdog import watch_raw_directory


def main():
    parser = argparse.ArgumentParser(
        description="Watch a RAW directory and register RAW updates."
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Path to the RAW directory to watch.",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        required=True,
        help="Processing database path or SQLAlchemy database URL.",
    )
    args = parser.parse_args()

    target_dir = args.path.resolve()
    db_path = args.db_path

    if not target_dir.exists():
        raise FileNotFoundError(
            f"RAW directory does not exist: {target_dir}"
        )

    observer = watch_raw_directory(
        target_dir,
        db_path=db_path,
    )

    print(f"Watching {target_dir}")
    print(f"Processing ledger: {db_path}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == "__main__":
    main()
