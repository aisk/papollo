"""Sans-IO protocol logic shared by the sync and async clients."""

import base64
import hashlib
import hmac
import json
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from urllib.parse import quote

import httpx

from .exceptions import ApolloError

DEFAULT_NAMESPACE = "application"

logger = logging.getLogger("papollo")

_PROPERTIES_SUFFIX = ".properties"

# The server holds a long poll for 60 seconds before answering 304.
LONG_POLL_READ_TIMEOUT = 90.0
_DEFAULT_TIMEOUT = 5.0  # same as httpx

# A failing long poll is retried after a delay doubling from the first to the second.
RETRY_DELAYS = (1.0, 120.0)


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
        messages: Mapping[str, int] | None = None,
    ) -> httpx.Request:
        """Build a /configs request.

        ``messages`` are the details of the notification that triggered this fetch, they make a
        caching config service reload the namespace instead of answering from a stale cache.
        """
        path = "/".join(quote(part, safe="") for part in (self.app_id, self.cluster, namespace))
        params = {
            key: value
            for key, value in (
                ("releaseKey", release_key),
                ("messages", _dump_json({"details": messages}) if messages else None),
                ("ip", self.ip),
                ("label", self.label),
            )
            if value is not None
        }
        request = http.build_request(
            "GET",
            f"{self.server_url}/configs/{path}",
            params=params,
            timeout=self.timeout if self.timeout is not None else httpx.USE_CLIENT_DEFAULT,
        )
        return self._sign(request)

    def build_notifications_request(
        self, http: httpx.Client | httpx.AsyncClient, notification_ids: Mapping[str, int]
    ) -> httpx.Request:
        """Build a long poll request, answered once one of the namespaces has a newer id."""
        notifications = [
            {"namespaceName": namespace, "notificationId": notification_id}
            for namespace, notification_id in notification_ids.items()
        ]
        params = {
            "appId": self.app_id,
            "cluster": self.cluster,
            "notifications": _dump_json(notifications),
        }
        if self.ip is not None:
            params["ip"] = self.ip
        timeout = self.timeout if self.timeout is not None else _DEFAULT_TIMEOUT
        request = http.build_request(
            "GET",
            f"{self.server_url}/notifications/v2",
            params=params,
            timeout=httpx.Timeout(timeout, read=LONG_POLL_READ_TIMEOUT),
        )
        return self._sign(request)

    def _sign(self, request: httpx.Request) -> httpx.Request:
        if self.secret is not None:
            # Sign exactly what goes on the wire so the server side check matches.
            timestamp = str(int(time.time() * 1000))
            signature = sign(timestamp, request.url.raw_path.decode("ascii"), self.secret)
            request.headers["Authorization"] = f"Apollo {self.app_id}:{signature}"
            request.headers["Timestamp"] = timestamp
        return request


def _dump_json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"))


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


@dataclass(frozen=True, slots=True)
class Notification:
    namespace: str
    notification_id: int
    # Passed on to the /configs request, see Settings.build_config_request.
    messages: Mapping[str, int]


def parse_notifications_response(response: httpx.Response) -> list[Notification]:
    """Return the namespaces with a newer notification id, none when the server answers 304."""
    if response.status_code == 304:
        return []
    if response.status_code != 200:
        raise ApolloError(
            f"failed to poll notifications: HTTP {response.status_code}",
            status_code=response.status_code,
        )
    try:
        return [
            Notification(
                item["namespaceName"],
                int(item["notificationId"]),
                dict((item.get("messages") or {}).get("details") or {}),
            )
            for item in response.json()
        ]
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise ApolloError(
            "invalid notifications response", status_code=response.status_code
        ) from exc
