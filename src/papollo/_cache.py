"""Local disk cache of the last config fetched, to start while the config service is down."""

import os
import tempfile
from contextlib import suppress

from ._core import Snapshot, cache_file_name, dump_cache, logger, parse_cache


class LocalCache:
    """One file per namespace, replaced atomically on every new release.

    Failures are logged as warnings and never raised, the cache is only a fallback. There is no
    in-process state, so processes and forked children can share a directory.
    """

    def __init__(self, directory: str | os.PathLike[str], app_id: str, cluster: str) -> None:
        # Absolute, so a later chdir() does not move the cache.
        self.directory = os.path.abspath(directory)
        self._app_id = app_id
        self._cluster = cluster

    def path(self, namespace: str) -> str:
        return os.path.join(self.directory, cache_file_name(self._app_id, self._cluster, namespace))

    def load(self, namespace: str) -> Snapshot | None:
        path = self.path(namespace)
        try:
            with open(path, "rb") as f:
                return parse_cache(f.read())
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            logger.warning("ignoring unreadable config cache %s: %s", path, exc)
            return None

    def save(self, namespace: str, snapshot: Snapshot) -> None:
        path = self.path(namespace)
        try:
            self._write(path, dump_cache(snapshot))
        except OSError as exc:
            logger.warning("failed to write config cache %s: %s", path, exc)

    def _write(self, path: str, data: bytes) -> None:
        # Only the last directory is created 0o700, as it will hold secrets.
        os.makedirs(self.directory, mode=0o700, exist_ok=True)
        # A unique temporary file in the same directory, renamed over the old one, so concurrent
        # writers and readers never see a partial file. mkstemp() creates it 0o600.
        fd, tmp = tempfile.mkstemp(dir=self.directory, prefix=".papollo-", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp, path)
        except BaseException:
            with suppress(OSError):
                os.unlink(tmp)
            raise
