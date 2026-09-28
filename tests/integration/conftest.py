"""Fixtures for tests against a real Apollo server.

Set ``APOLLO_CONFIG_URL`` to enable them, for example ``http://localhost:8080`` for the
quick start all in one jar. The portal is used to create apps and publish releases.
"""

import os
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass

import httpx
import pytest

CONFIG_URL = os.environ.get("APOLLO_CONFIG_URL")
PORTAL_URL = os.environ.get("APOLLO_PORTAL_URL", "http://localhost:8070")
PORTAL_USER = os.environ.get("APOLLO_PORTAL_USER", "apollo")
PORTAL_PASSWORD = os.environ.get("APOLLO_PORTAL_PASSWORD", "admin")
ENV = os.environ.get("APOLLO_ENV", "LOCAL")


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
    if CONFIG_URL is None:
        pytest.skip("APOLLO_CONFIG_URL is not set")
    with httpx.Client(base_url=PORTAL_URL) as http:
        http.post("/signin", data={"username": PORTAL_USER, "password": PORTAL_PASSWORD})
        if "JSESSIONID" not in http.cookies:
            pytest.fail(f"failed to sign in to portal at {PORTAL_URL}")
        yield Portal(http)


@pytest.fixture
def app(portal: Portal) -> App:
    """A fresh app with a released ``application`` and ``app.json`` namespace."""
    assert CONFIG_URL is not None
    app_id = portal.create_app()
    portal.create_namespace(app_id, "app", "json")
    app = App(app_id, CONFIG_URL, portal)
    app.publish("application", "timeout=30\nname=demo")
    app.publish("app.json", '{"a": 1}', fmt="json")
    return app


class StatusRecorder:
    """httpx response hook that remembers status codes seen by a client."""

    def __init__(self) -> None:
        self.statuses: list[int] = []

    def __call__(self, response: httpx.Response) -> None:
        self.statuses.append(response.status_code)

    async def async_hook(self, response: httpx.Response) -> None:
        self(response)
