import threading
import time
from collections.abc import Iterator

import httpx
import pytest

from papollo import Apollo, ApolloError

from .conftest import App, FakeApollo, ResponseRecorder, requires_fork, run_in_child


@pytest.fixture
def client(app: App, recorder: ResponseRecorder) -> Iterator[Apollo]:
    with (
        httpx.Client(event_hooks={"response": [recorder]}) as http,
        Apollo(app.config_url, app.app_id, http_client=http) as client,
    ):
        yield client


def test_get(client: Apollo) -> None:
    assert client.get("timeout") == "30"
    assert client.get("name") == "demo"
    assert client.get("missing") is None
    assert client.get("missing", "1") == "1"


def test_non_properties_namespace(client: Apollo) -> None:
    assert client.namespace("app.json") == {"content": '{"a": 1}'}


def test_properties_suffix(client: Apollo, recorder: ResponseRecorder) -> None:
    assert client.get("timeout", namespace="application.properties") == "30"
    assert client.namespace("application") is client.namespace("application.properties")
    assert recorder.statuses == [200]


def test_cached_after_first_load(client: Apollo, recorder: ResponseRecorder) -> None:
    client.get("timeout")
    client.get("name")
    client.namespace()
    assert recorder.statuses == [200]


def test_refresh_not_modified(client: Apollo, recorder: ResponseRecorder) -> None:
    before = client.namespace()
    client.refresh()
    assert recorder.statuses == [200, 304]
    assert client.namespace() is before


def test_refresh_new_release(app: App, client: Apollo) -> None:
    before = client.namespace()
    app.publish("application", "timeout=60")
    client.refresh()
    assert client.get("timeout") == "60"
    assert client.get("name") is None
    assert before["timeout"] == "30"


def test_namespace_is_read_only(client: Apollo) -> None:
    with pytest.raises(TypeError):
        client.namespace()["timeout"] = "1"  # type: ignore[index]


def test_refresh_loads_unloaded_namespace(client: Apollo, recorder: ResponseRecorder) -> None:
    client.refresh("app.json")
    client.namespace("app.json")
    assert recorder.statuses == [200]


def test_refresh_all_only_touches_loaded(client: Apollo, recorder: ResponseRecorder) -> None:
    client.refresh()
    assert recorder.statuses == []
    client.namespace()
    client.refresh()
    assert recorder.statuses == [200, 304]


def test_missing_namespace(client: Apollo) -> None:
    with pytest.raises(ApolloError) as info:
        client.namespace("nope")
    assert info.value.status_code == 404


def test_options_sent_on_wire(app: App, recorder: ResponseRecorder) -> None:
    with (
        httpx.Client(event_hooks={"response": [recorder]}) as http,
        Apollo(
            app.config_url, app.app_id, cluster="sh", ip="10.0.0.1", label="gray", http_client=http
        ) as client,
    ):
        # An unknown cluster falls back to the default one on the server.
        assert client.get("timeout") == "30"
    request = recorder.responses[0].request
    assert request.url.path == f"/configs/{app.app_id}/sh/application"
    assert request.url.params["ip"] == "10.0.0.1"
    assert request.url.params["label"] == "gray"


def test_access_key(app: App) -> None:
    secret = app.portal.enable_access_key(app.app_id)
    with Apollo(app.config_url, app.app_id, secret=secret) as client:
        assert client.get("timeout") == "30"
    with Apollo(app.config_url, app.app_id) as client, pytest.raises(ApolloError) as info:
        client.get("timeout")
    assert info.value.status_code == 401
    with (
        Apollo(app.config_url, app.app_id, secret="wrong") as client,
        pytest.raises(ApolloError) as info,
    ):
        client.get("timeout")
    assert info.value.status_code == 401


@requires_fork
def test_fork(app: App) -> None:
    with Apollo(app.config_url, app.app_id) as client:
        assert client.get("timeout") == "30"
        parent_http = client._http
        app.publish("application", "timeout=60")

        def child() -> tuple[bool, str | None, str | None]:
            cached = client.get("timeout")
            client.refresh()
            return client._http is not parent_http, cached, client.get("timeout")

        # A lock held by a parent thread at fork time must not deadlock the child.
        with client._locks["application"]:
            assert run_in_child(child) == (True, "30", "60")
        client.refresh()
        assert client.get("timeout") == "60"


# The tests below need no server, or a failure a real server can not produce on demand.


def test_network_error_wrapped() -> None:
    with Apollo("http://127.0.0.1:1", "demo") as client, pytest.raises(ApolloError) as info:
        client.get("timeout")
    assert isinstance(info.value.__cause__, httpx.ConnectError)


def test_invalid_url_wrapped() -> None:
    with Apollo("http://[bad", "demo") as client, pytest.raises(ApolloError):
        client.get("timeout")


def test_owned_http_client_closed() -> None:
    client = Apollo("http://apollo:8080", "demo")
    client.close()
    assert client._http.is_closed


def test_given_http_client_not_closed() -> None:
    with httpx.Client() as http:
        Apollo("http://apollo:8080", "demo", http_client=http).close()
        assert not http.is_closed


@pytest.fixture
def fake_client(fake_apollo: FakeApollo) -> Iterator[Apollo]:
    with (
        httpx.Client(transport=httpx.MockTransport(fake_apollo.handler)) as http,
        Apollo("http://apollo:8080", "demo", http_client=http) as client,
    ):
        yield client


def test_refresh_failure_keeps_cache(fake_client: Apollo, fake_apollo: FakeApollo) -> None:
    fake_client.namespace()
    fake_client.namespace("app.json")
    fake_apollo.fail_with = 500
    with pytest.raises(ApolloError):
        fake_client.refresh()
    assert len(fake_apollo.requests) == 4  # both namespaces were tried
    assert fake_client.get("timeout") == "30"


def test_failed_first_load_not_cached(fake_client: Apollo, fake_apollo: FakeApollo) -> None:
    fake_apollo.fail_with = 500
    with pytest.raises(ApolloError):
        fake_client.refresh("application")
    fake_apollo.fail_with = None
    assert fake_client.get("timeout") == "30"
    assert len(fake_apollo.requests) == 2


def test_concurrent_first_load_fetches_once(fake_apollo: FakeApollo) -> None:
    started = threading.Event()
    release = threading.Event()

    def slow_handler(request: httpx.Request) -> httpx.Response:
        started.set()
        release.wait(5)
        return fake_apollo.handler(request)

    http = httpx.Client(transport=httpx.MockTransport(slow_handler))
    client = Apollo("http://apollo:8080", "demo", http_client=http)
    results: list[str | None] = []
    threads = [
        threading.Thread(target=lambda: results.append(client.get("timeout"))) for _ in range(5)
    ]
    for t in threads:
        t.start()
    started.wait(5)
    time.sleep(0.05)  # give the other threads time to block on the namespace lock
    release.set()
    for t in threads:
        t.join(5)
    assert results == ["30"] * 5
    assert len(fake_apollo.requests) == 1


@requires_fork
def test_fork_keeps_given_http_client(fake_client: Apollo) -> None:
    fake_client.get("timeout")
    given_http = fake_client._http

    def child() -> tuple[bool, str | None]:
        fake_client.refresh()
        return fake_client._http is given_http, fake_client.get("timeout")

    with fake_client._locks["application"]:
        assert run_in_child(child) == (True, "30")
