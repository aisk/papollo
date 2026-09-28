import threading
import time

import httpx
import pytest

from papollo import ApolloClient, ApolloError

from .conftest import FakeApollo


@pytest.fixture
def client(apollo: FakeApollo):
    http = httpx.Client(transport=httpx.MockTransport(apollo.handler))
    with ApolloClient("http://apollo:8080/", "demo", http_client=http) as client:
        yield client
    http.close()


def test_namespace_is_read_only_snapshot(client: ApolloClient, apollo: FakeApollo) -> None:
    before = client.namespace()
    with pytest.raises(TypeError):
        before["timeout"] = "1"  # type: ignore[index]
    apollo.publish("application", "r2", {"timeout": "60"})
    client.refresh()
    assert before["timeout"] == "30"
    assert client.get("timeout") == "60"


def test_refresh_loads_unloaded_namespace(client: ApolloClient, apollo: FakeApollo) -> None:
    client.refresh("app.json")
    client.namespace("app.json")
    assert len(apollo.requests) == 1


def test_refresh_all_only_touches_loaded(client: ApolloClient, apollo: FakeApollo) -> None:
    client.refresh()
    assert apollo.requests == []


def test_refresh_failure_keeps_cache(client: ApolloClient, apollo: FakeApollo) -> None:
    client.namespace()
    client.namespace("app.json")
    apollo.fail_with = 500
    with pytest.raises(ApolloError):
        client.refresh()
    assert len(apollo.requests) == 4  # both namespaces were tried
    assert client.get("timeout") == "30"


def test_network_error_wrapped() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    http = httpx.Client(transport=httpx.MockTransport(handler))
    with ApolloClient("http://apollo:8080", "demo", http_client=http) as client:
        with pytest.raises(ApolloError) as info:
            client.get("timeout")
    assert isinstance(info.value.__cause__, httpx.ConnectError)
    assert not http.is_closed


def test_concurrent_first_load_fetches_once(apollo: FakeApollo) -> None:
    started = threading.Event()
    release = threading.Event()

    def slow_handler(request: httpx.Request) -> httpx.Response:
        started.set()
        release.wait(5)
        return apollo.handler(request)

    http = httpx.Client(transport=httpx.MockTransport(slow_handler))
    client = ApolloClient("http://apollo:8080", "demo", http_client=http)
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
    assert len(apollo.requests) == 1


def test_owned_http_client_closed() -> None:
    client = ApolloClient("http://apollo:8080", "demo")
    client.close()
    assert client._http.is_closed


def test_options_sent_on_wire(apollo: FakeApollo) -> None:
    http = httpx.Client(transport=httpx.MockTransport(apollo.handler))
    with ApolloClient(
        "http://apollo:8080",
        "demo",
        cluster="sh",
        ip="10.0.0.1",
        label="gray",
        secret="s",
        http_client=http,
    ) as client:
        client.namespace()
    request = apollo.requests[0]
    assert request.url.path == "/configs/demo/sh/application"
    assert request.url.params["ip"] == "10.0.0.1"
    assert request.url.params["label"] == "gray"
    assert request.headers["Authorization"].startswith("Apollo demo:")


def test_failed_first_load_not_cached(client: ApolloClient, apollo: FakeApollo) -> None:
    apollo.fail_with = 500
    with pytest.raises(ApolloError):
        client.refresh("application")
    apollo.fail_with = None
    assert client.get("timeout") == "30"
    assert len(apollo.requests) == 2


def test_invalid_url_wrapped() -> None:
    with ApolloClient("http://[bad", "demo") as client, pytest.raises(ApolloError):
        client.get("timeout")
