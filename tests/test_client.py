import json
import logging
import os
import tempfile
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from papollo import Apollo, ApolloError
from papollo._cache import LocalCache
from papollo._core import Snapshot, parse_cache, parse_notifications_response

from .conftest import (
    App,
    Clock,
    FakeApollo,
    ResponseRecorder,
    portal_session,
    requires_fork,
    run_in_child,
    wait_until,
    write_cache,
)


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


def test_max_age(app: App, recorder: ResponseRecorder, clock: Clock) -> None:
    with (
        httpx.Client(event_hooks={"response": [recorder]}) as http,
        Apollo(app.config_url, app.app_id, max_age=60, http_client=http) as client,
    ):
        assert client.get("timeout") == "30"
        app.publish("application", "timeout=60")
        clock.now += 59
        assert client.get("timeout") == "30"
        clock.now += 1
        assert client.get("timeout") == "60"
        clock.now += 60
        assert client.get("timeout") == "60"
    assert recorder.statuses == [200, 200, 304]


def test_listener(app: App, client: Apollo) -> None:
    changes: list[tuple[str, dict[str, str], dict[str, str]]] = []
    listener = client.add_listener(lambda ns, old, new: changes.append((ns, dict(old), dict(new))))
    client.get("timeout")
    client.namespace("app.json")
    client.refresh()
    assert changes == []  # neither the first load nor a 304 is a change
    app.publish("application", "timeout=60")
    client.refresh()
    assert changes == [("application", {"timeout": "30", "name": "demo"}, {"timeout": "60"})]
    client.remove_listener(listener)
    app.publish("application", "timeout=90")
    client.refresh()
    assert len(changes) == 1


def test_listener_on_max_age(app: App, clock: Clock) -> None:
    changes: list[str | None] = []
    with Apollo(app.config_url, app.app_id, max_age=10) as client:
        client.add_listener(lambda ns, old, new: changes.append(new.get("timeout")))
        client.get("timeout")
        app.publish("application", "timeout=60")
        clock.now += 10
        assert client.get("timeout") == "60"
    assert changes == ["60"]


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


def poller_alive(client: Apollo) -> bool:
    return client._poller is not None and client._poller.is_alive()


def wait_polling(client: Apollo, *namespaces: str) -> None:
    """Wait until a long poll that the server holds is in flight for exactly ``namespaces``."""
    wait_until(
        lambda: (
            client._poll_socket is not None
            and client._polled == set(namespaces)
            and all(name in client._notification_ids for name in namespaces)
        )
    )


@pytest.fixture
def watching(app: App) -> Iterator[Apollo]:
    with Apollo(app.config_url, app.app_id, watch=True) as client:
        yield client


def test_watch(app: App, watching: Apollo) -> None:
    changes: list[tuple[str, dict[str, str]]] = []
    watching.add_listener(lambda ns, old, new: changes.append((ns, dict(new))))
    assert watching._poller is None  # started by the first read
    assert watching.get("timeout") == "30"
    assert poller_alive(watching)
    wait_polling(watching, "application")
    app.publish("application", "timeout=60")
    wait_until(lambda: changes)
    assert changes == [("application", {"timeout": "60"})]
    assert watching.get("timeout") == "60"


def test_watch_namespace_loaded_while_polling(
    app: App, watching: Apollo, caplog: pytest.LogCaptureFixture
) -> None:
    changes: list[str] = []
    watching.add_listener(lambda ns, old, new: changes.append(ns))
    watching.get("timeout")
    wait_polling(watching, "application")
    with caplog.at_level(logging.WARNING, logger="papollo"):
        watching.namespace("app.json")
        # The poll in flight is aborted and a new one covers both, without waiting a minute.
        wait_polling(watching, "application", "app.json")
    assert caplog.records == []
    app.publish("app.json", '{"a": 2}', fmt="json")
    wait_until(lambda: changes)
    assert changes == ["app.json"]


def test_close_while_polling(watching: Apollo) -> None:
    watching.get("timeout")
    wait_polling(watching, "application")
    poller = watching._poller
    started = time.monotonic()
    watching.close()
    assert time.monotonic() - started < 2
    assert poller is not None and not poller.is_alive()
    watching.get("timeout")  # still served from memory, but no new poller
    assert watching._poller is poller


def test_watch_access_key(app: App) -> None:
    secret = app.portal.enable_access_key(app.app_id)
    changed = threading.Event()
    with Apollo(app.config_url, app.app_id, secret=secret, watch=True) as client:
        client.add_listener(lambda ns, old, new: changed.set())
        client.get("timeout")
        wait_polling(client, "application")
        app.publish("application", "timeout=60")
        assert changed.wait(10)
        assert client.get("timeout") == "60"


@requires_fork
@pytest.mark.filterwarnings("ignore:.*fork.*:DeprecationWarning")
def test_watch_fork(app: App, watching: Apollo, caplog: pytest.LogCaptureFixture) -> None:
    changes: list[str | None] = []
    watching.add_listener(lambda ns, old, new: changes.append(new.get("timeout")))
    watching.get("timeout")
    wait_polling(watching, "application")

    def child() -> tuple[bool, list[str | None], str | None]:
        # The poller thread does not exist in the child, the next read starts a new one.
        restarted = watching._poller is None
        watching.get("timeout")
        wait_polling(watching, "application")
        with portal_session() as portal:
            portal.publish(app.app_id, "application", "timeout=60")
        wait_until(lambda: changes)
        watching.close()
        return restarted, changes, watching.get("timeout")

    with caplog.at_level(logging.WARNING, logger="papollo"):
        assert run_in_child(child, timeout=30) == (True, ["60"], "60")
        # The parent kept polling on its own connection, the child did not break it.
        wait_until(lambda: changes)
    assert changes == ["60"]
    assert caplog.records == []


def test_watch_off_by_default(client: Apollo) -> None:
    client.get("timeout")
    assert client._poller is None


def test_cache_written(app: App, tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    with Apollo(app.config_url, app.app_id, cache_dir=cache_dir) as client:
        client.get("timeout")
        client.namespace("app.json")
        release_key = client._snapshots["application"].release_key
    assert cache_dir.stat().st_mode & 0o777 == 0o700
    files = sorted(cache_dir.iterdir())
    assert [f.name for f in files] == [
        f"{app.app_id}+default+app.json.json",
        f"{app.app_id}+default+application.json",
    ]
    assert all(f.stat().st_mode & 0o777 == 0o600 for f in files)
    snapshot = parse_cache(files[1].read_bytes())
    assert snapshot.release_key == release_key
    assert snapshot.configurations == {"timeout": "30", "name": "demo"}


def test_cache_served_when_unreachable(
    app: App, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with Apollo(app.config_url, app.app_id, cache_dir=tmp_path) as client:
        client.get("timeout")
    with (
        caplog.at_level(logging.WARNING, logger="papollo"),
        Apollo("http://127.0.0.1:1", app.app_id, cache_dir=tmp_path) as client,
    ):
        assert client.get("timeout") == "30"
        assert "from local cache" in caplog.text
        with pytest.raises(ApolloError):
            client.namespace("app.json")  # never cached


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


def test_max_age_off_by_default(fake_client: Apollo, fake_apollo: FakeApollo, clock: Clock) -> None:
    fake_client.get("timeout")
    clock.now += 10**6
    fake_client.get("timeout")
    assert len(fake_apollo.requests) == 1


def test_max_age_failure_serves_cache(
    fake_apollo: FakeApollo, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    http = httpx.Client(transport=httpx.MockTransport(fake_apollo.handler))
    with Apollo("http://apollo:8080", "demo", max_age=10, http_client=http) as client:
        client.get("timeout")
        fake_apollo.fail_with = 500
        clock.now += 10
        with caplog.at_level(logging.WARNING, logger="papollo"):
            assert client.get("timeout") == "30"
        assert "HTTP 500" in caplog.text
        # A failed refresh is not retried on every read, only once max_age passed again.
        clock.now += 9
        client.get("timeout")
        assert len(fake_apollo.requests) == 2
        clock.now += 1
        client.get("timeout")
        assert len(fake_apollo.requests) == 3


def test_max_age_readers_do_not_wait(fake_apollo: FakeApollo, clock: Clock) -> None:
    started = threading.Event()
    release = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if fake_apollo.requests:
            started.set()
            release.wait(5)
        return fake_apollo.handler(request)

    http = httpx.Client(transport=httpx.MockTransport(handler))
    with Apollo("http://apollo:8080", "demo", max_age=10, http_client=http) as client:
        client.get("timeout")
        fake_apollo.publish("application", "r2", {"timeout": "60"})
        clock.now += 10
        refresher = threading.Thread(target=client.get, args=("timeout",))
        refresher.start()
        assert started.wait(5)
        assert client.get("timeout") == "30"  # served from cache while the refresh is blocked
        release.set()
        refresher.join(5)
        assert client.get("timeout") == "60"
    assert len(fake_apollo.requests) == 2


def test_listener_errors_logged(
    fake_client: Apollo, fake_apollo: FakeApollo, caplog: pytest.LogCaptureFixture
) -> None:
    calls: list[str] = []

    def broken(namespace: str, old: object, new: object) -> None:
        raise RuntimeError("boom")

    fake_client.add_listener(broken)
    fake_client.add_listener(lambda ns, old, new: calls.append(ns))
    fake_client.add_listener(broken)  # added once only
    fake_client.get("timeout")
    fake_apollo.publish("application", "r2", {"timeout": "60"})
    with caplog.at_level(logging.ERROR, logger="papollo"):
        fake_client.refresh()
    assert fake_client.get("timeout") == "60"
    assert calls == ["application"]
    [record] = caplog.records
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], RuntimeError)


def test_listener_not_fired_for_same_configurations(
    fake_client: Apollo, fake_apollo: FakeApollo
) -> None:
    calls: list[str] = []
    fake_client.add_listener(lambda ns, old, new: calls.append(ns))
    fake_client.get("timeout")
    fake_apollo.publish("application", "r2", {"timeout": "30", "name": "demo"})
    fake_client.refresh()
    assert calls == []


def test_listener_can_read(fake_client: Apollo, fake_apollo: FakeApollo) -> None:
    seen: list[str | None] = []
    # Runs outside the namespace lock, a read of the same namespace must not deadlock.
    fake_client.add_listener(lambda ns, old, new: seen.append(fake_client.get("timeout")))
    fake_client.get("timeout")
    fake_apollo.publish("application", "r2", {"timeout": "60"})
    fake_client.refresh()
    assert seen == ["60"]


@requires_fork
def test_fork_keeps_given_http_client(fake_client: Apollo) -> None:
    fake_client.get("timeout")
    given_http = fake_client._http

    def child() -> tuple[bool, str | None]:
        fake_client.refresh()
        return fake_client._http is given_http, fake_client.get("timeout")

    with fake_client._locks["application"]:
        assert run_in_child(child) == (True, "30")


@pytest.fixture
def fake_watching(fake_apollo: FakeApollo, monkeypatch: pytest.MonkeyPatch) -> Iterator[Apollo]:
    def create_poll_http(self: Apollo) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(fake_apollo.handler))

    monkeypatch.setattr(Apollo, "_create_poll_http", create_poll_http)
    http = httpx.Client(transport=httpx.MockTransport(fake_apollo.handler))
    with http, Apollo("http://apollo:8080", "demo", watch=True, http_client=http) as client:
        yield client


def retry_delays(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage().split("retrying in ")[1].split(" ")[0]
        for record in caplog.records
        if record.getMessage().startswith("watch failed")
    ]


def test_watch_refetch_sends_messages(fake_watching: Apollo, fake_apollo: FakeApollo) -> None:
    changed = threading.Event()
    fake_watching.add_listener(lambda ns, old, new: changed.set())
    fake_watching.get("timeout")
    fake_apollo.publish("application", "r2", {"timeout": "60"})
    assert changed.wait(5)
    assert fake_watching.get("timeout") == "60"
    request = fake_apollo.config_requests[-1]
    assert request.url.params["messages"] == '{"details":{"demo+default+application":2}}'
    # Plain reads and refresh() do not send messages.
    fake_watching.refresh()
    assert "messages" not in fake_apollo.config_requests[-1].url.params


@pytest.mark.usefixtures("fast_retry")
def test_watch_backoff(
    fake_watching: Apollo, fake_apollo: FakeApollo, caplog: pytest.LogCaptureFixture
) -> None:
    fake_apollo.notifications_fail_with = 500
    with caplog.at_level(logging.WARNING, logger="papollo"):
        fake_watching.get("timeout")
        wait_until(lambda: len(retry_delays(caplog)) >= 4)
        assert retry_delays(caplog)[:4] == ["0.01", "0.02", "0.04", "0.04"]
        assert "HTTP 500" in caplog.text
        # A successful poll resets the delay.
        fake_apollo.notifications_fail_with = None
        polls = len(fake_apollo.poll_requests)
        wait_until(lambda: len(fake_apollo.poll_requests) > polls + 1)
        caplog.clear()
        fake_apollo.notifications_fail_with = 500
        wait_until(lambda: retry_delays(caplog))
        assert retry_delays(caplog)[0] == "0.01"
        fake_watching.close()


@pytest.mark.usefixtures("fast_retry")
def test_watch_refetch_failure_retried(fake_watching: Apollo, fake_apollo: FakeApollo) -> None:
    changed = threading.Event()
    fake_watching.add_listener(lambda ns, old, new: changed.set())
    fake_watching.get("timeout")
    wait_until(lambda: fake_watching._notification_ids.get("application") == 1)
    fake_apollo.fail_with = 500
    fake_apollo.publish("application", "r2", {"timeout": "60"})
    # The notification id is kept, so the namespace is refetched again after the failure.
    wait_until(lambda: len(fake_apollo.config_requests) >= 4)
    assert fake_watching._notification_ids["application"] == 1
    fake_apollo.fail_with = None
    assert changed.wait(5)
    assert fake_watching._notification_ids["application"] == 2


@pytest.mark.usefixtures("fast_retry")
def test_watch_survives_unexpected_errors(
    fake_watching: Apollo,
    fake_apollo: FakeApollo,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls = 0

    def broken(response: httpx.Response) -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("bug")
        return parse_notifications_response(response)

    monkeypatch.setattr("papollo.client.parse_notifications_response", broken)
    changed = threading.Event()
    fake_watching.add_listener(lambda ns, old, new: changed.set())
    with caplog.at_level(logging.WARNING, logger="papollo"):
        fake_watching.get("timeout")
        wait_until(lambda: calls > 1)
    [record] = caplog.records
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], RuntimeError)
    fake_apollo.publish("application", "r2", {"timeout": "60"})
    assert changed.wait(5)


def test_watch_ignores_unknown_namespaces(fake_watching: Apollo, fake_apollo: FakeApollo) -> None:
    fake_watching.get("timeout")
    wait_until(lambda: "application" in fake_watching._notification_ids)
    fake_apollo.publish("app.json", "j2", {"content": "{}"})
    fake_apollo.publish("application", "r2", {"timeout": "60"})
    wait_until(lambda: fake_watching.get("timeout") == "60")
    assert "app.json" not in fake_watching._snapshots
    assert all("app.json" not in r.url.path for r in fake_apollo.config_requests)


def test_watch_starts_after_first_load(fake_watching: Apollo) -> None:
    with pytest.raises(ApolloError):
        fake_watching.namespace("nope")
    assert fake_watching._poller is None
    fake_watching.get("timeout")
    assert poller_alive(fake_watching)


def test_closed_client_does_not_watch(fake_watching: Apollo) -> None:
    fake_watching.close()
    assert fake_watching.get("timeout") == "30"
    assert fake_watching._poller is None


def test_close_in_listener_stops_watch(fake_watching: Apollo, fake_apollo: FakeApollo) -> None:
    fake_watching.add_listener(lambda ns, old, new: fake_watching.close())
    fake_watching.get("timeout")
    fake_apollo.publish("application", "r2", {"timeout": "60"})
    # Called in the poller thread, which then stops.
    wait_until(lambda: not poller_alive(fake_watching))


@pytest.fixture
def fake_http(fake_apollo: FakeApollo) -> Iterator[httpx.Client]:
    with httpx.Client(transport=httpx.MockTransport(fake_apollo.handler)) as http:
        yield http


@pytest.mark.parametrize(
    ("status_code", "served"), [(500, True), (503, True), (401, False), (404, False)]
)
def test_cache_fallback_status(
    fake_apollo: FakeApollo,
    fake_http: httpx.Client,
    tmp_path: Path,
    status_code: int,
    served: bool,
) -> None:
    write_cache(tmp_path, "application", "r0", {"timeout": "10"})
    fake_apollo.fail_with = status_code
    with Apollo("http://apollo:8080", "demo", cache_dir=tmp_path, http_client=fake_http) as client:
        if served:
            assert client.get("timeout") == "10"
        else:
            # A wrong secret or namespace is a mistake a stale cache would hide.
            with pytest.raises(ApolloError) as info:
                client.get("timeout")
            assert info.value.status_code == status_code


def test_cache_not_served_for_bad_url(tmp_path: Path) -> None:
    write_cache(tmp_path, "application", "r0", {"timeout": "10"})
    # A server URL without a scheme will never work, the cache must not hide that.
    with Apollo("apollo:8080", "demo", cache_dir=tmp_path) as client, pytest.raises(ApolloError):
        client.get("timeout")


def test_cache_concurrent_first_loads(
    fake_apollo: FakeApollo, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def slow_handler(request: httpx.Request) -> httpx.Response:
        time.sleep(0.05)  # let the other readers pile up on the namespace lock
        return fake_apollo.handler(request)

    loads: list[str] = []
    load = LocalCache.load

    def counting_load(self: LocalCache, namespace: str) -> Snapshot | None:
        loads.append(namespace)
        return load(self, namespace)

    monkeypatch.setattr(LocalCache, "load", counting_load)
    write_cache(tmp_path, "application", "r0", {"timeout": "10"})
    fake_apollo.fail_with = 500
    results: list[str | None] = []
    with (
        httpx.Client(transport=httpx.MockTransport(slow_handler)) as http,
        Apollo("http://apollo:8080", "demo", cache_dir=tmp_path, http_client=http) as client,
    ):
        threads = [
            threading.Thread(target=lambda: results.append(client.get("timeout"))) for _ in range(8)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    assert results == ["10"] * 8
    assert len(fake_apollo.requests) == 1
    assert loads == ["application"]


def test_cache_refresh_raises_but_serves(
    fake_apollo: FakeApollo, fake_http: httpx.Client, tmp_path: Path
) -> None:
    write_cache(tmp_path, "application", "r0", {"timeout": "10"})
    fake_apollo.fail_with = 500
    with Apollo("http://apollo:8080", "demo", cache_dir=tmp_path, http_client=fake_http) as client:
        with pytest.raises(ApolloError) as info:
            client.refresh("application")
        assert info.value.status_code == 500
        assert client.get("timeout") == "10"
        assert len(fake_apollo.requests) == 1


def test_cache_caught_up(fake_apollo: FakeApollo, fake_http: httpx.Client, tmp_path: Path) -> None:
    path = write_cache(tmp_path, "application", "r1", {"timeout": "30", "name": "demo"})
    fake_apollo.fail_with = 500
    changes: list[dict[str, str]] = []
    with Apollo("http://apollo:8080", "demo", cache_dir=tmp_path, http_client=fake_http) as client:
        client.add_listener(lambda ns, old, new: changes.append(dict(new)))
        before = client.namespace()
        fake_apollo.fail_with = None
        # The cached release key is sent, the server has nothing newer.
        client.refresh()
        assert fake_apollo.config_requests[-1].url.params["releaseKey"] == "r1"
        assert client.namespace() is before
        fake_apollo.publish("application", "r2", {"timeout": "60"})
        client.refresh()
        assert client.get("timeout") == "60"
    assert changes == [{"timeout": "60"}]
    assert parse_cache(path.read_bytes()).release_key == "r2"
    assert os.listdir(tmp_path) == [path.name]  # no temporary files left


def test_cache_corrupt(
    fake_apollo: FakeApollo,
    fake_http: httpx.Client,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = write_cache(tmp_path, "application", "r0", {"timeout": "10"})
    path.write_bytes(b'{"format": 1, "releaseKey"')
    fake_apollo.fail_with = 500
    with (
        caplog.at_level(logging.WARNING, logger="papollo"),
        Apollo("http://apollo:8080", "demo", cache_dir=tmp_path, http_client=fake_http) as client,
        pytest.raises(ApolloError) as info,
    ):
        client.get("timeout")
    assert info.value.status_code == 500
    assert "ignoring unreadable config cache" in caplog.text


def test_cache_write_failure_logged(
    fake_http: httpx.Client, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    not_a_dir = tmp_path / "file"
    not_a_dir.touch()
    with (
        caplog.at_level(logging.WARNING, logger="papollo"),
        Apollo("http://apollo:8080", "demo", cache_dir=not_a_dir, http_client=fake_http) as client,
    ):
        assert client.get("timeout") == "30"
    assert "failed to write config cache" in caplog.text


def test_cache_failed_write_leaves_no_temp_file(
    fake_http: httpx.Client,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def broken_replace(src: str, dst: str) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", broken_replace)
    with (
        caplog.at_level(logging.WARNING, logger="papollo"),
        Apollo("http://apollo:8080", "demo", cache_dir=tmp_path, http_client=fake_http) as client,
    ):
        assert client.get("timeout") == "30"
    assert "disk full" in caplog.text
    assert os.listdir(tmp_path) == []


def test_cache_path_stays_in_directory(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"releaseKey": "r1", "configurations": {"k": "v"}})

    cache_dir = tmp_path / "cache"
    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as http,
        Apollo(
            "http://apollo:8080", "../../x", cluster="a/..", cache_dir=cache_dir, http_client=http
        ) as client,
    ):
        client.namespace("../../../etc/passwd")
    [path] = [p for p in tmp_path.rglob("*") if p.is_file()]
    assert path.parent == cache_dir


@pytest.mark.usefixtures("fast_retry")
def test_cache_watch_catches_up(
    fake_apollo: FakeApollo,
    fake_http: httpx.Client,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def create_poll_http(self: Apollo) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(fake_apollo.handler))

    monkeypatch.setattr(Apollo, "_create_poll_http", create_poll_http)
    path = write_cache(tmp_path, "application", "r1", {"timeout": "30", "name": "demo"})
    fake_apollo.fail_with = 500
    with Apollo(
        "http://apollo:8080", "demo", watch=True, cache_dir=tmp_path, http_client=fake_http
    ) as client:
        assert client.get("timeout") == "30"
        wait_until(lambda: fake_apollo.poll_requests)
        notifications = json.loads(fake_apollo.poll_requests[0].url.params["notifications"])
        assert notifications == [{"namespaceName": "application", "notificationId": -1}]
        fake_apollo.publish("application", "r2", {"timeout": "60"})
        fake_apollo.fail_with = None
        wait_until(lambda: client.get("timeout") == "60")
    assert parse_cache(path.read_bytes()).release_key == "r2"


def test_cache_fd_closed_when_fdopen_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    fds: list[int] = []
    mkstemp = tempfile.mkstemp

    def recording_mkstemp(**kwargs: str) -> tuple[int, str]:
        fd, path = mkstemp(**kwargs)
        fds.append(fd)
        return fd, path

    def broken_fdopen(fd: int, mode: str) -> object:
        raise OSError("fdopen failed")

    monkeypatch.setattr(tempfile, "mkstemp", recording_mkstemp)
    monkeypatch.setattr(os, "fdopen", broken_fdopen)
    with caplog.at_level(logging.WARNING, logger="papollo"):
        write_cache(tmp_path, "application", "r1", {"k": "v"})
    assert "fdopen failed" in caplog.text
    [fd] = fds
    with pytest.raises(OSError):
        os.fstat(fd)
    assert os.listdir(tmp_path) == []
