import asyncio
import inspect
import sys
from collections.abc import Awaitable, Callable, Mapping
from time import monotonic
from types import TracebackType
from typing import TypeVar, overload

import httpx

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from . import _core, _fork
from ._core import (
    DEFAULT_NAMESPACE,
    Settings,
    Snapshot,
    logger,
    normalize_namespace,
    parse_config_response,
    parse_notifications_response,
)
from .exceptions import ApolloError

T = TypeVar("T")

AsyncListener = Callable[[str, Mapping[str, str], Mapping[str, str]], Awaitable[None] | None]
L = TypeVar("L", bound=AsyncListener)


class AsyncApollo:
    """Asyncio Apollo config client, same API as ``Apollo``.

    A client instance is bound to the event loop it is first used in, and with ``watch`` the long
    polling runs as a task in that loop. A child process created by ``os.fork()`` gets a fresh
    connection pool and may use the client in its own event loop.
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
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = Settings(
            server_url.rstrip("/"), app_id, cluster, secret, ip, label, timeout
        )
        self._max_age = max_age
        self._http = http_client if http_client is not None else httpx.AsyncClient()
        self._owns_http = http_client is None
        self._snapshots: dict[str, Snapshot] = {}
        self._fetched_at: dict[str, float] = {}
        self._listeners: tuple[AsyncListener, ...] = ()
        self._locks: dict[str, asyncio.Lock] = {}
        self._watch = watch
        self._notification_ids: dict[str, int] = {}
        self._closed = False
        # The poller task is created by the first read, in the loop the client is used in.
        self._poll_task: asyncio.Task[None] | None = None
        # The long poll in flight and the namespaces it covers.
        self._poll_request: asyncio.Future[httpx.Response] | None = None
        self._polled: frozenset[str] = frozenset()
        _fork.track(self)

    @overload
    async def get(self, key: str, *, namespace: str = DEFAULT_NAMESPACE) -> str | None: ...
    @overload
    async def get(self, key: str, default: T, *, namespace: str = DEFAULT_NAMESPACE) -> str | T: ...
    async def get(
        self, key: str, default: object = None, *, namespace: str = DEFAULT_NAMESPACE
    ) -> object:
        return (await self.namespace(namespace)).get(key, default)

    async def namespace(self, name: str = DEFAULT_NAMESPACE) -> Mapping[str, str]:
        name = normalize_namespace(name)
        snapshot = self._snapshots.get(name)
        if snapshot is None:
            snapshot = await self._load(name, only_if_missing=True)
        elif self._is_stale(name):
            snapshot = await self._refresh_stale(name, snapshot)
        self._ensure_watching()
        return snapshot.configurations

    async def refresh(self, name: str | None = None) -> None:
        """Refetch one namespace, or every loaded namespace when ``name`` is None.

        A namespace that has not been loaded yet is loaded, so this can also be
        used to preload namespaces at startup. On failure the cached config is
        kept and the error is raised after all namespaces were tried.
        """
        names = [normalize_namespace(name)] if name is not None else list(self._snapshots)
        results = await asyncio.gather(*(self._load(ns) for ns in names), return_exceptions=True)
        self._ensure_watching()
        for result in results:
            if isinstance(result, BaseException):
                raise result

    def add_listener(self, callback: L) -> L:
        """Call ``callback(namespace, old, new)`` when a loaded namespace's config changes.

        The callback may be a coroutine function, it is then awaited. It is not called on first
        load. It is called by whichever coroutine fetched the change, and exceptions it raises
        are logged to the ``papollo`` logger. Returns the callback, so it can be a decorator.
        """
        if callback not in self._listeners:
            self._listeners = (*self._listeners, callback)
        return callback

    def remove_listener(self, callback: AsyncListener) -> None:
        self._listeners = tuple(cb for cb in self._listeners if cb != callback)

    async def aclose(self) -> None:
        self._closed = True
        task = self._poll_task
        if (
            task is not None
            and not task.done()
            and task is not asyncio.current_task()
            and task.get_loop() is asyncio.get_running_loop()
        ):
            task.cancel()
            await asyncio.wait([task])
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    def _after_fork_in_child(self) -> None:
        # The locks and pooled connections belong to the parent's event loop, and the sockets are
        # shared with the parent. The old client is dropped rather than closed.
        self._locks = {}
        if self._owns_http:
            self._http = httpx.AsyncClient()
        # The poller task belongs to the parent's event loop, the next read in the child starts a
        # new one in the child's loop.
        self._poll_task = None
        self._poll_request = None

    def _is_stale(self, name: str) -> bool:
        return self._max_age is not None and monotonic() - self._fetched_at[name] >= self._max_age

    async def _refresh_stale(self, name: str, current: Snapshot) -> Snapshot:
        lock = self._locks.setdefault(name, asyncio.Lock())
        # Only one reader refreshes, the others keep serving the cached config meanwhile.
        if lock.locked():
            return current
        async with lock:
            before = self._snapshots[name]
            if not self._is_stale(name):
                return before
            try:
                snapshot = await self._fetch(name)
            except ApolloError as exc:
                logger.warning("serving cached config after refresh failed: %s", exc)
                return before
        await self._emit(name, before, snapshot)
        return snapshot

    async def _load(
        self,
        name: str,
        *,
        only_if_missing: bool = False,
        messages: Mapping[str, int] | None = None,
    ) -> Snapshot:
        async with self._locks.setdefault(name, asyncio.Lock()):
            before = self._snapshots.get(name)
            if only_if_missing and before is not None:
                return before
            snapshot = await self._fetch(name, messages)
        if before is None:
            self._watch_new_namespace(name)
        await self._emit(name, before, snapshot)
        return snapshot

    async def _emit(self, name: str, old: Snapshot | None, new: Snapshot) -> None:
        if old is None or old is new or old.configurations == new.configurations:
            return
        for callback in self._listeners:
            try:
                result = callback(name, old.configurations, new.configurations)
                if inspect.isawaitable(result):
                    await result
            except Exception:  # noqa: BLE001 a broken listener must not break the fetch
                logger.exception("config change listener %r failed", callback)

    async def _fetch(self, name: str, messages: Mapping[str, int] | None = None) -> Snapshot:
        """Fetch a namespace, the caller must hold its lock."""
        current = self._snapshots.get(name)
        release_key = current.release_key if current is not None else None
        # Set before the request, so a failing server is retried once per max_age at most.
        self._fetched_at[name] = monotonic()
        try:
            request = self._settings.build_config_request(self._http, name, release_key, messages)
            response = await self._http.send(request)
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

    def _ensure_watching(self) -> None:
        if not self._watch or self._closed or not self._snapshots:
            return
        task = self._poll_task
        loop = asyncio.get_running_loop()
        # A task of another loop is left over from an earlier asyncio.run(), start a new one.
        if task is None or task.done() or task.get_loop() is not loop:
            self._poll_task = loop.create_task(self._poll_forever(), name="papollo-watch")

    def _watch_new_namespace(self, name: str) -> None:
        # A long poll in flight does not cover a namespace loaded meanwhile. Cancel it rather than
        # waiting up to 60 seconds for its answer, the poller then starts one covering both.
        if self._poll_request is not None and name not in self._polled:
            self._poll_request.cancel()

    async def _poll_forever(self) -> None:
        delay = _core.RETRY_DELAYS[0]
        async with self._create_poll_http() as http:
            while True:
                try:
                    await self._poll(http)
                except Exception as exc:  # noqa: BLE001 the poller must keep running
                    if isinstance(exc, ApolloError):
                        logger.warning("watch failed, retrying in %g seconds: %s", delay, exc)
                    else:
                        logger.exception("watch failed, retrying in %g seconds", delay)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, _core.RETRY_DELAYS[1])
                else:
                    delay = _core.RETRY_DELAYS[0]

    def _create_poll_http(self) -> httpx.AsyncClient:
        # A separate client, a pooled connection of the user's own would be held for a minute.
        return httpx.AsyncClient()

    async def _poll(self, http: httpx.AsyncClient) -> None:
        ids = {name: self._notification_ids.get(name, -1) for name in self._snapshots}
        self._polled = frozenset(ids)
        try:
            request = self._settings.build_notifications_request(http, ids)
        except httpx.InvalidURL as exc:
            raise ApolloError(f"failed to poll notifications: {exc}") from exc
        # Sent in its own task, so a new namespace can cancel it without cancelling the poller.
        send = self._poll_request = asyncio.ensure_future(http.send(request))
        try:
            await asyncio.wait([send])
        finally:
            self._poll_request = None
            if not send.done():  # the poller itself was cancelled
                send.cancel()
                await asyncio.wait([send])
        if send.cancelled():
            return  # aborted to add a namespace
        try:
            response = send.result()
        except httpx.HTTPError as exc:
            raise ApolloError(f"failed to poll notifications: {exc}") from exc
        error: ApolloError | None = None
        for notification in parse_notifications_response(response):
            name = notification.namespace
            if name not in ids:
                continue
            try:
                await self._load(name, messages=notification.messages)
            except ApolloError as exc:
                # The id is not updated, so the next poll reports the namespace again.
                error = error or exc
            else:
                self._notification_ids[name] = notification.notification_id
        if error is not None:
            raise error
