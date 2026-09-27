"""Sans-IO protocol logic shared by the sync and async clients."""

import base64
import hashlib
import hmac
import time
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from urllib.parse import quote

import httpx

DEFAULT_NAMESPACE = "application"

_PROPERTIES_SUFFIX = ".properties"


class ApolloError(Exception):
    """Raised when config can not be fetched from Apollo."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class Snapshot:
    release_key: str
    configurations: Mapping[str, str]


def normalize_namespace(name: str) -> str:
    # Apollo treats "foo" and "foo.properties" as the same namespace.
    if name.lower().endswith(_PROPERTIES_SUFFIX):
        return name[: -len(_PROPERTIES_SUFFIX)]
    return name


def sign(timestamp: str, path_with_query: str, secret: str) -> str:
    message = f"{timestamp}\n{path_with_query}".encode()
    digest = hmac.new(secret.encode(), message, hashlib.sha1).digest()
    return base64.b64encode(digest).decode()


@dataclass(frozen=True, slots=True)
class Settings:
    server_url: str
    app_id: str
    cluster: str
    secret: str | None
    ip: str | None
    label: str | None
    timeout: float | None

    def build_config_request(
        self,
        http: httpx.Client | httpx.AsyncClient,
        namespace: str,
        release_key: str | None,
    ) -> httpx.Request:
        path = "/".join(quote(part, safe="") for part in (self.app_id, self.cluster, namespace))
        params = {
            key: value
            for key, value in (("releaseKey", release_key), ("ip", self.ip), ("label", self.label))
            if value is not None
        }
        request = http.build_request(
            "GET",
            f"{self.server_url}/configs/{path}",
            params=params,
            timeout=self.timeout if self.timeout is not None else httpx.USE_CLIENT_DEFAULT,
        )
        if self.secret is not None:
            # Sign exactly what goes on the wire so the server side check matches.
            timestamp = str(int(time.time() * 1000))
            signature = sign(timestamp, request.url.raw_path.decode("ascii"), self.secret)
            request.headers["Authorization"] = f"Apollo {self.app_id}:{signature}"
            request.headers["Timestamp"] = timestamp
        return request


def parse_config_response(response: httpx.Response, namespace: str) -> Snapshot | None:
    """Return the new snapshot, or None when the server answers 304 Not Modified."""
    if response.status_code == 304:
        return None
    if response.status_code != 200:
        raise ApolloError(
            f"failed to fetch namespace {namespace!r}: HTTP {response.status_code}",
            status_code=response.status_code,
        )
    try:
        body = response.json()
        release_key = body["releaseKey"]
        configurations = dict(body["configurations"])
    except (ValueError, KeyError, TypeError) as exc:
        raise ApolloError(
            f"invalid response for namespace {namespace!r}", status_code=response.status_code
        ) from exc
    return Snapshot(release_key, MappingProxyType(configurations))
