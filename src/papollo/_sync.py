import threading
from collections.abc import Mapping
from types import TracebackType
from typing import Self, overload

import httpx

from ._core import (
    DEFAULT_NAMESPACE,
    ApolloError,
    Settings,
    Snapshot,
    normalize_namespace,
    parse_config_response,
)


class ApolloClient:
    """Blocking Apollo config client.

    Namespaces are fetched on first access and then served from memory.
    Call ``refresh()`` to pull the latest release.
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
        http_client: httpx.Client | None = None,
    ) -> None:
        self._settings = Settings(
            server_url.rstrip("/"), app_id, cluster, secret, ip, label, timeout
        )
        self._http = http_client if http_client is not None else httpx.Client()
        self._owns_http = http_client is None
        self._snapshots: dict[str, Snapshot] = {}
        self._locks: dict[str, threading.Lock] = {}

    @overload
    def get(self, key: str, *, namespace: str = DEFAULT_NAMESPACE) -> str | None: ...
    @overload
    def get[T](self, key: str, default: T, *, namespace: str = DEFAULT_NAMESPACE) -> str | T: ...
    def get(
        self, key: str, default: object = None, *, namespace: str = DEFAULT_NAMESPACE
    ) -> object:
        return self.namespace(namespace).get(key, default)

    def namespace(self, name: str = DEFAULT_NAMESPACE) -> Mapping[str, str]:
        name = normalize_namespace(name)
        snapshot = self._snapshots.get(name)
        if snapshot is None:
            snapshot = self._load(name, only_if_missing=True)
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
            except Exception as exc:
                error = error or exc
        if error is not None:
            raise error

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

    def _load(self, name: str, *, only_if_missing: bool = False) -> Snapshot:
        with self._locks.setdefault(name, threading.Lock()):
            current = self._snapshots.get(name)
            if only_if_missing and current is not None:
                return current
            release_key = current.release_key if current is not None else None
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
