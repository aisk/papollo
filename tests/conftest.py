"""Test fixtures.

Most tests run against a real Apollo, by default the quick start all in one jar on localhost.
They are skipped when it is not reachable, unless ``PAPOLLO_REQUIRE_APOLLO`` is set, in which
case they fail. The portal is used to create apps and publish releases.

``FakeApollo`` backs the few tests for failures a real server can not produce on demand.
"""

import json
import os
import pickle
import signal
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import httpx
import pytest

CONFIG_URL = os.environ.get("APOLLO_CONFIG_URL", "http://localhost:8080")
PORTAL_URL = os.environ.get("APOLLO_PORTAL_URL", "http://localhost:8070")
PORTAL_USER = os.environ.get("APOLLO_PORTAL_USER", "apollo")
PORTAL_PASSWORD = os.environ.get("APOLLO_PORTAL_PASSWORD", "admin")
ENV = os.environ.get("APOLLO_ENV", "LOCAL")
REQUIRE_APOLLO = bool(os.environ.get("PAPOLLO_REQUIRE_APOLLO"))

requires_fork = pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork()")


class Portal:
    """Minimal Apollo portal client used to set up test data."""

    def __init__(self, http: httpx.Client) -> None:
        self.http = http

    def create_app(self) -> str:
        app_id = f"papollo-{uuid.uuid4().hex[:12]}"
        self._call(
            "POST",
            "/apps",
            json={
                "appId": app_id,
                "name": app_id,
                "orgId": "TEST1",
                "orgName": "test",
                "ownerName": PORTAL_USER,
                "admins": [],
            },
        )
        return app_id

    def create_namespace(self, app_id: str, name: str, fmt: str) -> None:
        self._call(
            "POST",
            f"/apps/{app_id}/appnamespaces",
            json={"appId": app_id, "name": name, "format": fmt, "isPublic": False, "comment": ""},
        )

    def publish(self, app_id: str, namespace: str, text: str, fmt: str = "properties") -> str:
        base = f"/apps/{app_id}/envs/{ENV}/clusters/default/namespaces/{namespace}"
        namespace_id = self._call("GET", base).json()["baseInfo"]["id"]
        self._call(
            "PUT",
            f"{base}/items",
            json={"namespaceId": namespace_id, "format": fmt, "configText": text},
        )
        release = self._call(
            "POST",
            f"{base}/releases",
            json={"releaseTitle": "test", "releaseComment": "", "isEmergencyPublish": False},
        )
        return str(release.json()["releaseKey"])

    def enable_access_key(self, app_id: str) -> str:
        base = f"/apps/{app_id}/envs/{ENV}/accesskeys"
        key = self._call(
            "POST",
            base,
            json={
                "appId": app_id,
                "dataChangeCreatedBy": PORTAL_USER,
                "dataChangeLastModifiedBy": PORTAL_USER,
            },
        ).json()
        self._call("PUT", f"{base}/{key['id']}/enable")
        # The config service reloads access keys periodically, wait until it is enforced.
        url = f"{CONFIG_URL}/configs/{app_id}/default/application"
        deadline = time.monotonic() + 30
        while httpx.get(url).status_code != 401:
            if time.monotonic() > deadline:
                raise TimeoutError("access key was not enforced in time")
            time.sleep(0.5)
        return str(key["secret"])

    def _call(self, method: str, path: str, **kwargs: object) -> httpx.Response:
        response = self.http.request(method, path, **kwargs)  # type: ignore[arg-type]
        response.raise_for_status()
        return response


@dataclass
class App:
    app_id: str
    config_url: str
    portal: Portal

    def publish(self, namespace: str, text: str, fmt: str = "properties") -> str:
        return self.portal.publish(self.app_id, namespace, text, fmt)


@pytest.fixture(scope="session")
def portal() -> Iterator[Portal]:
    try:
        httpx.get(CONFIG_URL, timeout=2)
    except httpx.TransportError as exc:
        message = f"Apollo is not reachable at {CONFIG_URL}: {exc}"
        if REQUIRE_APOLLO:
            pytest.fail(message)
        pytest.skip(message)
    with httpx.Client(base_url=PORTAL_URL) as http:
        http.post("/signin", data={"username": PORTAL_USER, "password": PORTAL_PASSWORD})
        if "JSESSIONID" not in http.cookies:
            pytest.fail(f"failed to sign in to portal at {PORTAL_URL}")
        yield Portal(http)


@pytest.fixture
def app(portal: Portal) -> App:
    """A fresh app with a released ``application`` and ``app.json`` namespace."""
    app_id = portal.create_app()
    portal.create_namespace(app_id, "app", "json")
    app = App(app_id, CONFIG_URL, portal)
    app.publish("application", "timeout=30\nname=demo")
    app.publish("app.json", '{"a": 1}', fmt="json")
    return app


class ResponseRecorder:
    """httpx response hook that remembers the responses a client received."""

    def __init__(self) -> None:
        self.responses: list[httpx.Response] = []

    def __call__(self, response: httpx.Response) -> None:
        self.responses.append(response)

    async def async_hook(self, response: httpx.Response) -> None:
        self(response)

    @property
    def statuses(self) -> list[int]:
        return [response.status_code for response in self.responses]


@pytest.fixture
def recorder() -> ResponseRecorder:
    return ResponseRecorder()


class FakeApollo:
    """In-memory stand-in for the Apollo config service /configs endpoint."""

    def __init__(self) -> None:
        self.namespaces: dict[str, tuple[str, dict[str, str]]] = {}
        self.requests: list[httpx.Request] = []
        self.fail_with: int | None = None

    def publish(self, namespace: str, release_key: str, configurations: dict[str, str]) -> None:
        self.namespaces[namespace] = (release_key, configurations)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.fail_with is not None:
            return httpx.Response(self.fail_with)
        _, _, app_id, cluster, namespace = request.url.path.split("/")
        if namespace not in self.namespaces:
            return httpx.Response(404)
        release_key, configurations = self.namespaces[namespace]
        if request.url.params.get("releaseKey") == release_key:
            return httpx.Response(304)
        body = {
            "appId": app_id,
            "cluster": cluster,
            "namespaceName": namespace,
            "configurations": configurations,
            "releaseKey": release_key,
        }
        return httpx.Response(200, content=json.dumps(body))


@pytest.fixture
def fake_apollo() -> FakeApollo:
    fake = FakeApollo()
    fake.publish("application", "r1", {"timeout": "30", "name": "demo"})
    fake.publish("app.json", "j1", {"content": '{"a": 1}'})
    return fake


def run_in_child(fn: Callable[[], object], timeout: int = 10) -> object:
    """Run ``fn`` in a forked child and return its result, or raise if it failed or hung."""
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # pragma: no cover, runs in the child
        os.close(read_fd)
        signal.alarm(timeout)  # a deadlocked child is killed and sends nothing
        try:
            result: tuple[bool, object] = (True, fn())
        except BaseException as exc:  # noqa: BLE001 reported to the parent
            result = (False, repr(exc))
        with os.fdopen(write_fd, "wb") as f:
            f.write(pickle.dumps(result))
        os._exit(0)
    os.close(write_fd)
    with os.fdopen(read_fd, "rb") as f:
        data = f.read()
    os.waitpid(pid, 0)
    if not data:
        raise AssertionError("child process hung or crashed")
    ok, value = pickle.loads(data)
    if not ok:
        raise AssertionError(f"child process failed: {value}")
    return value
