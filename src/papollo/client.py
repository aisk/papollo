import os
import socket
import sys
import threading
from collections.abc import Callable, Mapping
from contextlib import suppress
from time import monotonic
from types import TracebackType
from typing import Any, TypeVar, overload

import httpx

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from . import _core, _fork
from ._cache import LocalCache
from ._core import (
    DEFAULT_NAMESPACE,
    Settings,
    Snapshot,
    is_server_unavailable,
    logger,
    normalize_namespace,
    parse_config_response,
    parse_notifications_response,
)
from .exceptions import ApolloError

T = TypeVar("T")

Listener = Callable[[str, Mapping[str, str], Mapping[str, str]], None]
L = TypeVar("L", bound=Listener)

# httpcore trace events returning the network stream a request is sent over.
_STREAM_EVENTS = (".connect_tcp.complete", ".connect_unix_socket.complete", ".start_tls.complete")


class Apollo:
    """Blocking Apollo config client.

    Namespaces are fetched on first access and then served from memory. Call ``refresh()`` to
    pull the latest release, set ``max_age`` to have a read refetch a namespace once its cached
    config is older than that many seconds, or set ``watch`` to have a background thread long
    poll the server and refetch namespaces as soon as they are released. Set ``cache_dir`` to
    keep a copy of every namespace on disk, served when the server is down at first load.
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
        watch: bool = False,
        cache_dir: str | os.PathLike[str] | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._settings = Settings(
            server_url.rstrip("/"), app_id, cluster, secret, ip, label, timeout
        )
        self._cache = LocalCache(cache_dir, app_id, cluster) if cache_dir is not None else None
        self._max_age = max_age
        self._http = http_client if http_client is not None else httpx.Client()
        self._owns_http = http_client is None
        self._snapshots: dict[str, Snapshot] = {}
        self._fetched_at: dict[str, float] = {}
        # Replaced rather than mutated, so _emit() iterates without the lock.
        self._listeners: tuple[Listener, ...] = ()
        self._listeners_lock = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}
        self._watch = watch
        self._notification_ids: dict[str, int] = {}
        self._closed = False
        self._init_poller()
        _fork.track(self)

    def _init_poller(self) -> None:
        # The poller thread is started by the first read, not here, so a client created in a
        # process that forks before reading does not run a thread the children would not have.
        self._poller: threading.Thread | None = None
        self._stop = threading.Event()
        # Guards the socket of the long poll in flight and the namespaces it covers.
        self._poll_lock = threading.Lock()
        self._poll_socket: socket.socket | None = None
        self._polled: frozenset[str] = frozenset()

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
            try:
                snapshot = self._load(name, only_if_missing=True)
            except ApolloError:
                # Served from the local cache if _load() restored it.
                snapshot = self._snapshots.get(name)
                if snapshot is None:
                    raise
        elif self._is_stale(name):
            snapshot = self._refresh_stale(name, snapshot)
        self._ensure_watching()
        return snapshot.configurations

    def refresh(self, name: str | None = None) -> None:
        """Refetch one namespace, or every loaded namespace when ``name`` is None.

        A namespace that has not been loaded yet is loaded, so this can also be
        used to preload namespaces at startup. On failure the cached config is
        kept and the error is raised after all namespaces were tried. This also
        holds when an unloaded namespace was loaded from ``cache_dir`` instead.
        """
        names = [normalize_namespace(name)] if name is not None else list(self._snapshots)
        error: Exception | None = None
        for ns in names:
            try:
                self._load(ns)
            except Exception as exc:  # noqa: BLE001 raised below after all were tried
                error = error or exc
        self._ensure_watching()
        if error is not None:
            raise error

    def add_listener(self, callback: L) -> L:
        """Call ``callback(namespace, old, new)`` when a loaded namespace's config changes.

        It is not called on first load. It runs in whichever thread fetched the change, a reader,
        ``refresh()`` or the watch thread, and exceptions it raises are logged to the ``papollo``
        logger. Returns the callback, so it can be used as a decorator.
        """
        with self._listeners_lock:
            if callback not in self._listeners:
                self._listeners = (*self._listeners, callback)
        return callback

    def remove_listener(self, callback: Listener) -> None:
        """Unregister a callback added with ``add_listener()``, if it was."""
        with self._listeners_lock:
            self._listeners = tuple(cb for cb in self._listeners if cb != callback)

    def close(self) -> None:
        with self._poll_lock:
            self._closed = True
            self._stop.set()
            poller = self._poller
            # Closing the httpx client would not wake up a thread blocked reading the socket.
            if self._poll_socket is not None:
                _shutdown(self._poll_socket)
        if poller is not None and poller is not threading.current_thread():
            poller.join()
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
        self._listeners_lock = threading.Lock()
        if self._owns_http:
            self._http = httpx.Client()
        # The poller thread does not exist in the child, the next read starts a new one. The
        # socket of its long poll is shared with the parent and must not be shut down here.
        self._init_poller()

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

    def _load(
        self,
        name: str,
        *,
        only_if_missing: bool = False,
        messages: Mapping[str, int] | None = None,
    ) -> Snapshot:
        with self._locks.setdefault(name, threading.Lock()):
            before = self._snapshots.get(name)
            if only_if_missing and before is not None:
                return before
            try:
                snapshot = self._fetch(name, messages)
            except ApolloError as exc:
                if before is not None or not self._restore_cached(name, exc):
                    raise
                error: ApolloError | None = exc
            else:
                error = None
        if before is None:
            self._watch_new_namespace(name)
        if error is not None:
            raise error
        self._emit(name, before, snapshot)
        return snapshot

    def _restore_cached(self, name: str, error: ApolloError) -> bool:
        """Serve a namespace that failed to load from the local cache, the caller holds its lock.

        The next fetch sends the cached release key, so it gets a 304 or the new release. The
        error is raised anyway, ``namespace()`` catches it and ``refresh()`` passes it on.
        """
        if self._cache is None or not is_server_unavailable(error):
            return False
        snapshot = self._cache.load(name)
        if snapshot is None:
            return False
        logger.warning("serving namespace %r from local cache: %s", name, error)
        self._snapshots[name] = snapshot
        return True

    def _emit(self, name: str, old: Snapshot | None, new: Snapshot) -> None:
        # Called without the namespace lock held, so a listener may read any namespace.
        if old is None or old is new or old.configurations == new.configurations:
            return
        for callback in self._listeners:
            try:
                callback(name, old.configurations, new.configurations)
            except Exception:  # noqa: BLE001 a broken listener must not break the fetch
                logger.exception("config change listener %r failed", callback)

    def _fetch(self, name: str, messages: Mapping[str, int] | None = None) -> Snapshot:
        """Fetch a namespace, the caller must hold its lock."""
        current = self._snapshots.get(name)
        release_key = current.release_key if current is not None else None
        # Set before the request, so a failing server is retried once per max_age at most.
        self._fetched_at[name] = monotonic()
        try:
            request = self._settings.build_config_request(self._http, name, release_key, messages)
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
        if self._cache is not None:
            self._cache.save(name, snapshot)
        self._snapshots[name] = snapshot
        return snapshot

    def _ensure_watching(self) -> None:
        if not self._watch or self._poller is not None or not self._snapshots:
            return
        with self._poll_lock:
            if self._poller is None and not self._closed:
                self._poller = threading.Thread(
                    target=self._poll_forever, name="papollo-watch", daemon=True
                )
                self._poller.start()

    def _watch_new_namespace(self, name: str) -> None:
        # A long poll in flight does not cover a namespace loaded meanwhile. Abort it rather than
        # waiting up to 60 seconds for its answer, the poller then starts one covering both.
        with self._poll_lock:
            if self._poll_socket is not None and name not in self._polled:
                _shutdown(self._poll_socket)

    def _trace(self, event: str, info: Mapping[str, Any]) -> None:
        if not event.endswith(_STREAM_EVENTS):
            return
        sock = info["return_value"].get_extra_info("socket")
        with self._poll_lock:
            self._poll_socket = sock
            # close() or a new namespace may have come before the socket was known.
            if sock is not None and (self._stop.is_set() or self._has_unpolled_namespace()):
                _shutdown(sock)

    def _has_unpolled_namespace(self) -> bool:
        return not self._polled.issuperset(list(self._snapshots))

    def _poll_forever(self) -> None:
        delay = _core.RETRY_DELAYS[0]
        with self._create_poll_http() as http:
            while not self._stop.is_set():
                try:
                    self._poll(http)
                except Exception as exc:  # noqa: BLE001 the poller must keep running
                    if self._stop.is_set():
                        break
                    if isinstance(exc, ApolloError):
                        logger.warning("watch failed, retrying in %g seconds: %s", delay, exc)
                    else:
                        logger.exception("watch failed, retrying in %g seconds", delay)
                    self._stop.wait(delay)
                    delay = min(delay * 2, _core.RETRY_DELAYS[1])
                else:
                    delay = _core.RETRY_DELAYS[0]

    def _create_poll_http(self) -> httpx.Client:
        # A separate client, a pooled connection of the user's own would be held for a minute.
        # Keep-alive is off so every long poll has its own socket, see _trace().
        return httpx.Client(limits=httpx.Limits(max_keepalive_connections=0))

    def _poll(self, http: httpx.Client) -> None:
        with self._poll_lock:
            ids = {name: self._notification_ids.get(name, -1) for name in list(self._snapshots)}
            self._polled = frozenset(ids)
        try:
            request = self._settings.build_notifications_request(http, ids)
            request.extensions["trace"] = self._trace
            response = http.send(request)
        except (httpx.HTTPError, httpx.InvalidURL) as exc:
            if self._stop.is_set() or self._has_unpolled_namespace():
                return  # aborted on purpose
            raise ApolloError(f"failed to poll notifications: {exc}") from exc
        finally:
            with self._poll_lock:
                self._poll_socket = None
        error: ApolloError | None = None
        for notification in parse_notifications_response(response):
            name = notification.namespace
            if name not in ids:
                continue
            try:
                self._load(name, messages=notification.messages)
            except ApolloError as exc:
                # The id is not updated, so the next poll reports the namespace again.
                error = error or exc
            else:
                self._notification_ids[name] = notification.notification_id
        if error is not None:
            raise error


def _shutdown(sock: socket.socket) -> None:
    # Unlike close(), this wakes up a thread blocked reading the socket.
    with suppress(OSError):
        sock.shutdown(socket.SHUT_RDWR)
