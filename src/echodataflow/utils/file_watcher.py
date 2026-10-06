import logging
from collections.abc import Callable
from pathlib import Path

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer


logger = logging.getLogger(__name__)


class FileChangeHandler(FileSystemEventHandler):
    """Run a callback when a changed file satisfies ``matches``.

    Match one specific file, for example a transect CSV::

        target = Path("transects.csv").resolve()
        FileChangeHandler(
            callback=process,
            matches=lambda path: path == target,
        )

    Match files by name, for example all RAW files in the watched directory::

        FileChangeHandler(
            callback=process,
            matches=lambda path: path.match("*.raw"),
        )

    Callback exceptions are logged so they do not stop the filesystem
    observer.
    """

    def __init__(
        self,
        callback: Callable[[Path], None],
        matches: Callable[[Path], bool],
    ):
        self.callback = callback
        self.matches = matches

    def _handle_path(self, path: str | Path) -> None:
        event_path = Path(path).resolve()

        if not self.matches(event_path):
            return

        try:
            self.callback(event_path)
        except Exception:
            logger.exception(
                "Error handling filesystem update for %s",
                event_path,
            )

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self._handle_path(event.src_path)

    def on_created(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self._handle_path(event.src_path)

    def on_moved(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self._handle_path(event.dest_path)


def watch_file(
    target_file: str | Path,
    callback: Callable[[Path], None],
) -> Observer:
    """Start watching a file for modifications."""

    target_file = Path(target_file).resolve()

    observer = Observer()
    observer.schedule(
        FileChangeHandler(
            callback=callback,
            matches=lambda path: path == target_file,
        ),
        str(target_file.parent),
        recursive=False,
    )
    observer.start()

    return observer


def watch_directory(
    directory: str | Path,
    callback: Callable[[Path], None],
    pattern: str,
) -> Observer:
    """Start watching a directory for matching file changes."""

    directory = Path(directory).resolve()

    observer = Observer()
    observer.schedule(
        FileChangeHandler(
            callback=callback,
            matches=lambda path: path.match(pattern),
        ),
        str(directory),
        recursive=False,
    )
    observer.start()

    return observer
