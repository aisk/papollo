import json

import httpx
import pytest


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
def apollo() -> FakeApollo:
    fake = FakeApollo()
    fake.publish("application", "r1", {"timeout": "30", "name": "demo"})
    fake.publish("app.json", "j1", {"content": '{"a": 1}'})
    return fake
