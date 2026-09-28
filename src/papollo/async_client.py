import asyncio
from collections.abc import Mapping
from types import TracebackType
from typing import Self, overload

import httpx

from ._core import (
    DEFAULT_NAMESPACE,
    Settings,
    Snapshot,
    normalize_namespace,
    parse_config_response,
)
from .exceptions import ApolloError


class AsyncApollo:
    """Asyncio Apollo config client, same API as ``Apollo``.

    A client instance is bound to the event loop it is first used in.
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
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = Settings(
            server_url.rstrip("/"), app_id, cluster, secret, ip, label, timeout
        )
        self._http = http_client if http_client is not None else httpx.AsyncClient()
        self._owns_http = http_client is None
        self._snapshots: dict[str, Snapshot] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    @overload
    async def get(self, key: str, *, namespace: str = DEFAULT_NAMESPACE) -> str | None: ...
    @overload
    async def get[T](
        self, key: str, default: T, *, namespace: str = DEFAULT_NAMESPACE
    ) -> str | T: ...
    async def get(
        self, key: str, default: object = None, *, namespace: str = DEFAULT_NAMESPACE
    ) -> object:
        return (await self.namespace(namespace)).get(key, default)

    async def namespace(self, name: str = DEFAULT_NAMESPACE) -> Mapping[str, str]:
        name = normalize_namespace(name)
        snapshot = self._snapshots.get(name)
        if snapshot is None:
            snapshot = await self._load(name, only_if_missing=True)
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

    async def _load(self, name: str, *, only_if_missing: bool = False) -> Snapshot:
        async with self._locks.setdefault(name, asyncio.Lock()):
            current = self._snapshots.get(name)
            if only_if_missing and current is not None:
                return current
            release_key = current.release_key if current is not None else None
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
