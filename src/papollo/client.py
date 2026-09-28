import sys
import threading
from collections.abc import Callable, Mapping
from time import monotonic
from types import TracebackType
from typing import TypeVar, overload

import httpx

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from . import _fork
from ._core import (
    DEFAULT_NAMESPACE,
    Settings,
    Snapshot,
    logger,
    normalize_namespace,
    parse_config_response,
)
from .exceptions import ApolloError

T = TypeVar("T")

Listener = Callable[[str, Mapping[str, str], Mapping[str, str]], None]
L = TypeVar("L", bound=Listener)


class Apollo:
    """Blocking Apollo config client.

    Namespaces are fetched on first access and then served from memory. Call ``refresh()`` to
    pull the latest release, or set ``max_age`` to have a read refetch a namespace once its
    cached config is older than that many seconds.
    """

    def __init__(
        self,
        server_url: str,
        app_id: str,
        *,
        cluster: str = "default",
        secret: str | None = None,
        ip: str | None = None,
        label: str | None = None,
        timeout: float | None = None,
        max_age: float | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._settings = Settings(
            server_url.rstrip("/"), app_id, cluster, secret, ip, label, timeout
        )
        self._max_age = max_age
        self._http = http_client if http_client is not None else httpx.Client()
        self._owns_http = http_client is None
        self._snapshots: dict[str, Snapshot] = {}
        self._fetched_at: dict[str, float] = {}
        self._listeners: tuple[Listener, ...] = ()
        self._locks: dict[str, threading.Lock] = {}
        _fork.track(self)

    @overload
    def get(self, key: str, *, namespace: str = DEFAULT_NAMESPACE) -> str | None: ...
    @overload
    def get(self, key: str, default: T, *, namespace: str = DEFAULT_NAMESPACE) -> str | T: ...
    def get(
        self, key: str, default: object = None, *, namespace: str = DEFAULT_NAMESPACE
    ) -> object:
        return self.namespace(namespace).get(key, default)

    def namespace(self, name: str = DEFAULT_NAMESPACE) -> Mapping[str, str]:
        name = normalize_namespace(name)
        snapshot = self._snapshots.get(name)
        if snapshot is None:
            snapshot = self._load(name, only_if_missing=True)
        elif self._is_stale(name):
            snapshot = self._refresh_stale(name, snapshot)
        return snapshot.configurations

    def refresh(self, name: str | None = None) -> None:
        """Refetch one namespace, or every loaded namespace when ``name`` is None.

        A namespace that has not been loaded yet is loaded, so this can also be
        used to preload namespaces at startup. On failure the cached config is
        kept and the error is raised after all namespaces were tried.
        """
        names = [normalize_namespace(name)] if name is not None else list(self._snapshots)
        error: Exception | None = None
        for ns in names:
            try:
                self._load(ns)
            except Exception as exc:  # noqa: BLE001 raised below after all were tried
                error = error or exc
        if error is not None:
            raise error

    def add_listener(self, callback: L) -> L:
        """Call ``callback(namespace, old, new)`` when a loaded namespace's config changes.

        It is not called on first load. It runs in whichever thread fetched the change, a reader,
        ``refresh()`` or the watch thread, and exceptions it raises are logged to the ``papollo``
        logger. Returns the callback, so it can be used as a decorator.
        """
        if callback not in self._listeners:
            self._listeners = (*self._listeners, callback)
        return callback

    def remove_listener(self, callback: Listener) -> None:
        self._listeners = tuple(cb for cb in self._listeners if cb != callback)

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _after_fork_in_child(self) -> None:
        # Locks may be held by threads that are gone, and the pooled sockets are shared with the
        # parent. The old client is dropped rather than closed, closing it could block on its lock.
        self._locks = {}
        if self._owns_http:
            self._http = httpx.Client()

    def _is_stale(self, name: str) -> bool:
        return self._max_age is not None and monotonic() - self._fetched_at[name] >= self._max_age

    def _refresh_stale(self, name: str, current: Snapshot) -> Snapshot:
        lock = self._locks.setdefault(name, threading.Lock())
        # Only one reader refreshes, the others keep serving the cached config meanwhile.
        if not lock.acquire(blocking=False):
            return current
        try:
            before = self._snapshots[name]
            if not self._is_stale(name):
                return before
            try:
                snapshot = self._fetch(name)
            except ApolloError as exc:
                logger.warning("serving cached config after refresh failed: %s", exc)
                return before
        finally:
            lock.release()
        self._emit(name, before, snapshot)
        return snapshot

    def _load(self, name: str, *, only_if_missing: bool = False) -> Snapshot:
        with self._locks.setdefault(name, threading.Lock()):
            before = self._snapshots.get(name)
            if only_if_missing and before is not None:
                return before
            snapshot = self._fetch(name)
        self._emit(name, before, snapshot)
        return snapshot

    def _emit(self, name: str, old: Snapshot | None, new: Snapshot) -> None:
        # Called without the namespace lock held, so a listener may read any namespace.
        if old is None or old is new or old.configurations == new.configurations:
            return
        for callback in self._listeners:
            try:
                callback(name, old.configurations, new.configurations)
            except Exception:  # noqa: BLE001 a broken listener must not break the fetch
                logger.exception("config change listener %r failed", callback)

    def _fetch(self, name: str) -> Snapshot:
        """Fetch a namespace, the caller must hold its lock."""
        current = self._snapshots.get(name)
        release_key = current.release_key if current is not None else None
        # Set before the request, so a failing server is retried once per max_age at most.
        self._fetched_at[name] = monotonic()
        try:
            request = self._settings.build_config_request(self._http, name, release_key)
            response = self._http.send(request)
        except (httpx.HTTPError, httpx.InvalidURL) as exc:
            raise ApolloError(f"failed to fetch namespace {name!r}: {exc}") from exc
        snapshot = parse_config_response(response, name)
        if snapshot is None:
            if current is None:
                raise ApolloError(
                    f"unexpected 304 for unloaded namespace {name!r}", status_code=304
                )
            return current
        self._snapshots[name] = snapshot
        return snapshot
