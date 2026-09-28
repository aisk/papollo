import asyncio
import sys
from collections.abc import Mapping
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


class AsyncApollo:
    """Asyncio Apollo config client, same API as ``Apollo``.

    A client instance is bound to the event loop it is first used in. A child process created by
    ``os.fork()`` gets a fresh connection pool and may use the client in its own event loop.
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
        self._locks: dict[str, asyncio.Lock] = {}
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
        return snapshot.configurations

    async def refresh(self, name: str | None = None) -> None:
        """Refetch one namespace, or every loaded namespace when ``name`` is None.

        A namespace that has not been loaded yet is loaded, so this can also be
        used to preload namespaces at startup. On failure the cached config is
        kept and the error is raised after all namespaces were tried.
        """
        names = [normalize_namespace(name)] if name is not None else list(self._snapshots)
        results = await asyncio.gather(*(self._load(ns) for ns in names), return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result

    async def aclose(self) -> None:
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

    def _is_stale(self, name: str) -> bool:
        return self._max_age is not None and monotonic() - self._fetched_at[name] >= self._max_age

    async def _refresh_stale(self, name: str, current: Snapshot) -> Snapshot:
        lock = self._locks.setdefault(name, asyncio.Lock())
        # Only one reader refreshes, the others keep serving the cached config meanwhile.
        if lock.locked():
            return current
        async with lock:
            if not self._is_stale(name):
                return self._snapshots[name]
            try:
                return await self._fetch(name)
            except ApolloError as exc:
                logger.warning("serving cached config after refresh failed: %s", exc)
                return self._snapshots[name]

    async def _load(self, name: str, *, only_if_missing: bool = False) -> Snapshot:
        async with self._locks.setdefault(name, asyncio.Lock()):
            current = self._snapshots.get(name)
            if only_if_missing and current is not None:
                return current
            return await self._fetch(name)

    async def _fetch(self, name: str) -> Snapshot:
        """Fetch a namespace, the caller must hold its lock."""
        current = self._snapshots.get(name)
        release_key = current.release_key if current is not None else None
        # Set before the request, so a failing server is retried once per max_age at most.
        self._fetched_at[name] = monotonic()
        try:
            request = self._settings.build_config_request(self._http, name, release_key)
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
