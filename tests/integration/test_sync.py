from collections.abc import Iterator

import httpx
import pytest

from papollo import ApolloClient, ApolloError

from .conftest import App, StatusRecorder


@pytest.fixture
def recorder() -> StatusRecorder:
    return StatusRecorder()


@pytest.fixture
def client(app: App, recorder: StatusRecorder) -> Iterator[ApolloClient]:
    with (
        httpx.Client(event_hooks={"response": [recorder]}) as http,
        ApolloClient(app.config_url, app.app_id, http_client=http) as client,
    ):
        yield client


def test_get(client: ApolloClient) -> None:
    assert client.get("timeout") == "30"
    assert client.get("name") == "demo"
    assert client.get("missing") is None
    assert client.get("missing", "1") == "1"


def test_non_properties_namespace(client: ApolloClient) -> None:
    assert client.namespace("app.json") == {"content": '{"a": 1}'}


def test_properties_suffix(client: ApolloClient, recorder: StatusRecorder) -> None:
    assert client.get("timeout", namespace="application.properties") == "30"
    assert client.namespace("application") is client.namespace("application.properties")
    assert recorder.statuses == [200]


def test_refresh_not_modified(client: ApolloClient, recorder: StatusRecorder) -> None:
    before = client.namespace()
    client.refresh()
    assert recorder.statuses == [200, 304]
    assert client.namespace() is before


def test_refresh_new_release(app: App, client: ApolloClient) -> None:
    before = client.namespace()
    app.publish("application", "timeout=60")
    client.refresh()
    assert client.get("timeout") == "60"
    assert client.get("name") is None
    assert before["timeout"] == "30"


def test_missing_namespace(client: ApolloClient) -> None:
    with pytest.raises(ApolloError) as info:
        client.namespace("nope")
    assert info.value.status_code == 404


def test_access_key(app: App) -> None:
    secret = app.portal.enable_access_key(app.app_id)
    with ApolloClient(app.config_url, app.app_id, secret=secret) as client:
        assert client.get("timeout") == "30"
    with ApolloClient(app.config_url, app.app_id) as client, pytest.raises(ApolloError) as info:
        client.get("timeout")
    assert info.value.status_code == 401
    with (
        ApolloClient(app.config_url, app.app_id, secret="wrong") as client,
        pytest.raises(ApolloError) as info,
    ):
        client.get("timeout")
    assert info.value.status_code == 401
