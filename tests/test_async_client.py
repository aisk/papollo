import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator, Mapping
from pathlib import Path

import httpx
import pytest

from papollo import ApolloError, AsyncApollo
from papollo._core import parse_cache, parse_notifications_response

from .conftest import (
    App,
    Clock,
    FakeApollo,
    ResponseRecorder,
    async_wait_until,
    portal_session,
    requires_fork,
    retry_delays,
    run_in_child,
    write_cache,
)


@pytest.fixture
async def client(app: App, recorder: ResponseRecorder) -> AsyncIterator[AsyncApollo]:
    async with (
        httpx.AsyncClient(event_hooks={"response": [recorder.async_hook]}) as http,
        AsyncApollo(app.config_url, app.app_id, http_client=http) as client,
    ):
        yield client


async def test_get(client: AsyncApollo) -> None:
    assert await client.get("timeout") == "30"
    assert await client.get("missing") is None
    assert await client.get("missing", "1") == "1"
    assert await client.namespace("app.json") == {"content": '{"a": 1}'}


async def test_properties_suffix(client: AsyncApollo, recorder: ResponseRecorder) -> None:
    assert await client.namespace("application") is await client.namespace("application.properties")
    assert recorder.statuses == [200]


async def test_refresh(app: App, client: AsyncApollo, recorder: ResponseRecorder) -> None:
    before = await client.namespace()
    await client.refresh()
    assert await client.namespace() is before
    app.publish("application", "timeout=60")
    await client.refresh()
    assert await client.get("timeout") == "60"
    assert before["timeout"] == "30"
    assert recorder.statuses == [200, 304, 200]


async def test_refresh_loads_unloaded_namespace(
    client: AsyncApollo, recorder: ResponseRecorder
) -> None:
    await client.refresh("app.json")
    await client.namespace("app.json")
    assert recorder.statuses == [200]


async def test_refresh_all_only_touches_loaded(
    client: AsyncApollo, recorder: ResponseRecorder
) -> None:
    await client.refresh()
    assert recorder.statuses == []


async def test_missing_namespace(client: AsyncApollo) -> None:
    with pytest.raises(ApolloError) as info:
        await client.namespace("nope")
    assert info.value.status_code == 404


async def test_access_key(app: App) -> None:
    secret = app.portal.enable_access_key(app.app_id)
    async with AsyncApollo(app.config_url, app.app_id, secret=secret) as client:
        assert await client.get("timeout") == "30"


async def test_max_age(app: App, recorder: ResponseRecorder, clock: Clock) -> None:
    async with (
        httpx.AsyncClient(event_hooks={"response": [recorder.async_hook]}) as http,
        AsyncApollo(app.config_url, app.app_id, max_age=60, http_client=http) as client,
    ):
        assert await client.get("timeout") == "30"
        app.publish("application", "timeout=60")
        clock.now += 59
        assert await client.get("timeout") == "30"
        clock.now += 1
        assert await client.get("timeout") == "60"
    assert recorder.statuses == [200, 200]


async def test_listener(app: App, client: AsyncApollo) -> None:
    changes: list[tuple[str, dict[str, str], dict[str, str]]] = []

    async def listener(namespace: str, old: Mapping[str, str], new: Mapping[str, str]) -> None:
        await asyncio.sleep(0)
        changes.append((namespace, dict(old), dict(new)))

    client.add_listener(listener)
    await client.get("timeout")
    await client.refresh()
    assert changes == []
    app.publish("application", "timeout=60")
    await client.refresh()
    assert changes == [("application", {"timeout": "30", "name": "demo"}, {"timeout": "60"})]
    client.remove_listener(listener)
    app.publish("application", "timeout=90")
    await client.refresh()
    assert len(changes) == 1


async def test_listener_on_max_age(app: App, clock: Clock) -> None:
    changes: list[str | None] = []
    async with AsyncApollo(app.config_url, app.app_id, max_age=10) as client:
        client.add_listener(lambda ns, old, new: changes.append(new.get("timeout")))
        await client.get("timeout")
        app.publish("application", "timeout=60")
        clock.now += 10
        assert await client.get("timeout") == "60"
    assert changes == ["60"]


@requires_fork
def test_fork(app: App) -> None:
    # Not a coroutine, the parent and the child each run their own event loop.
    client = AsyncApollo(app.config_url, app.app_id)
    assert asyncio.run(client.get("timeout")) == "30"
    app.publish("application", "timeout=60")

    async def refresh_and_get() -> str | None:
        await client.refresh()
        return await client.get("timeout")

    assert run_in_child(lambda: asyncio.run(refresh_and_get())) == "60"


def poller_running(client: AsyncApollo) -> bool:
    return client._poll_task is not None and not client._poll_task.done()


async def wait_polling(client: AsyncApollo, *namespaces: str) -> None:
    """Wait until a long poll that the server holds is in flight for exactly ``namespaces``."""
    await async_wait_until(
        lambda: (
            client._poll_request is not None
            and client._polled == set(namespaces)
            and all(name in client._notification_ids for name in namespaces)
        )
    )


@pytest.fixture
async def watching(app: App) -> AsyncIterator[AsyncApollo]:
    async with AsyncApollo(app.config_url, app.app_id, watch=True) as client:
        yield client


async def test_watch(app: App, watching: AsyncApollo) -> None:
    changes: list[tuple[str, dict[str, str]]] = []

    async def listener(namespace: str, old: Mapping[str, str], new: Mapping[str, str]) -> None:
        changes.append((namespace, dict(new)))

    watching.add_listener(listener)
    assert watching._poll_task is None  # started by the first read
    assert await watching.get("timeout") == "30"
    assert poller_running(watching)
    await wait_polling(watching, "application")
    app.publish("application", "timeout=60")
    await async_wait_until(lambda: changes)
    assert changes == [("application", {"timeout": "60"})]
    assert await watching.get("timeout") == "60"


async def test_watch_namespace_loaded_while_polling(
    app: App, watching: AsyncApollo, caplog: pytest.LogCaptureFixture
) -> None:
    changes: list[str] = []
    watching.add_listener(lambda ns, old, new: changes.append(ns))
    await watching.get("timeout")
    await wait_polling(watching, "application")
    task = watching._poll_task
    with caplog.at_level(logging.WARNING, logger="papollo"):
        await watching.namespace("app.json")
        await wait_polling(watching, "application", "app.json")
    assert caplog.records == []
    assert watching._poll_task is task  # only the request was cancelled, not the poller
    app.publish("app.json", '{"a": 2}', fmt="json")
    await async_wait_until(lambda: changes)
    assert changes == ["app.json"]


async def test_aclose_while_polling(watching: AsyncApollo) -> None:
    await watching.get("timeout")
    await wait_polling(watching, "application")
    task = watching._poll_task
    started = asyncio.get_running_loop().time()
    await watching.aclose()
    assert asyncio.get_running_loop().time() - started < 2
    assert task is not None and task.done()
    await watching.get("timeout")
    assert watching._poll_task is task


async def test_watch_access_key(app: App) -> None:
    secret = app.portal.enable_access_key(app.app_id)
    changed = asyncio.Event()
    async with AsyncApollo(app.config_url, app.app_id, secret=secret, watch=True) as client:
        client.add_listener(lambda ns, old, new: changed.set())
        await client.get("timeout")
        await wait_polling(client, "application")
        app.publish("application", "timeout=60")
        await asyncio.wait_for(changed.wait(), 10)


@requires_fork
@pytest.mark.filterwarnings("ignore:.*fork.*:DeprecationWarning")
async def test_watch_fork(
    app: App, watching: AsyncApollo, caplog: pytest.LogCaptureFixture
) -> None:
    changes: list[str | None] = []
    watching.add_listener(lambda ns, old, new: changes.append(new.get("timeout")))
    await watching.get("timeout")
    await wait_polling(watching, "application")
    parent_task = watching._poll_task

    async def child() -> tuple[bool, list[str | None], str | None]:
        # The poller task belongs to the parent's loop, the next read starts one in this loop.
        restarted = watching._poll_task is None
        await watching.get("timeout")
        await wait_polling(watching, "application")
        with portal_session() as portal:
            portal.publish(app.app_id, "application", "timeout=60")
        await async_wait_until(lambda: changes)
        await watching.aclose()
        return restarted, changes, await watching.get("timeout")

    with caplog.at_level(logging.WARNING, logger="papollo"):
        assert run_in_child(lambda: asyncio.run(child()), timeout=30) == (True, ["60"], "60")
        # The parent kept polling on its own connection, the child did not break it.
        await async_wait_until(lambda: changes)
    assert changes == ["60"]
    assert caplog.records == []
    assert watching._poll_task is parent_task


async def test_watch_http_client(app: App, recorder: ResponseRecorder) -> None:
    changed = asyncio.Event()
    async with (
        httpx.AsyncClient(
            headers={"X-Test": "1"}, event_hooks={"response": [recorder.async_hook]}
        ) as http,
        AsyncApollo(app.config_url, app.app_id, watch=True, watch_http_client=http) as client,
    ):
        client.add_listener(lambda ns, old, new: changed.set())
        await client.get("timeout")
        await wait_polling(client, "application")
        app.publish("application", "timeout=60")
        await asyncio.wait_for(changed.wait(), 10)
        await client.aclose()
        assert not http.is_closed
    polls = [r.request for r in recorder.responses]
    assert polls and all(r.url.path == "/notifications/v2" for r in polls)
    assert all(r.headers["X-Test"] == "1" for r in polls)


def test_watch_restarts_in_new_loop(app: App) -> None:
    client = AsyncApollo(app.config_url, app.app_id, watch=True)
    changed: list[str] = []
    client.add_listener(lambda ns, old, new: changed.append(ns))

    async def read() -> None:
        await client.get("timeout")
        await wait_polling(client, "application")

    asyncio.run(read())  # the poller task is cancelled when this loop ends

    async def read_again() -> None:
        await read()
        app.publish("application", "timeout=60")
        await async_wait_until(lambda: changed)
        await client.aclose()

    asyncio.run(read_again())
    assert changed == ["application"]


async def test_cache_served_when_unreachable(
    app: App, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    async with AsyncApollo(app.config_url, app.app_id, cache_dir=tmp_path) as client:
        await client.get("timeout")
    [path] = tmp_path.iterdir()
    assert path.name == f"{app.app_id}+default+application.json"
    assert path.stat().st_mode & 0o777 == 0o600
    with caplog.at_level(logging.WARNING, logger="papollo"):
        async with AsyncApollo("http://127.0.0.1:1", app.app_id, cache_dir=tmp_path) as client:
            assert await client.get("timeout") == "30"
            with pytest.raises(ApolloError):
                await client.namespace("app.json")  # never cached
    assert "from local cache" in caplog.text


# The tests below need no server, or a failure a real server can not produce on demand.


async def test_network_error_wrapped() -> None:
    async with AsyncApollo("http://127.0.0.1:1", "demo") as client:
        with pytest.raises(ApolloError) as info:
            await client.get("timeout")
    assert isinstance(info.value.__cause__, httpx.ConnectError)


async def test_owned_http_client_closed() -> None:
    client = AsyncApollo("http://apollo:8080", "demo")
    await client.aclose()
    assert client._http.is_closed


async def test_invalid_url_wrapped() -> None:
    async with AsyncApollo("http://[bad", "demo") as client:
        with pytest.raises(ApolloError):
            await client.get("timeout")


async def test_given_http_client_not_closed() -> None:
    async with httpx.AsyncClient() as http:
        await AsyncApollo("http://apollo:8080", "demo", http_client=http).aclose()
        assert not http.is_closed


async def test_failed_first_load_not_cached(
    fake_client: AsyncApollo, fake_apollo: FakeApollo
) -> None:
    fake_apollo.fail_with = 500
    with pytest.raises(ApolloError):
        await fake_client.refresh("application")
    fake_apollo.fail_with = None
    assert await fake_client.get("timeout") == "30"
    assert len(fake_apollo.requests) == 2


@pytest.fixture
async def fake_client(fake_apollo: FakeApollo) -> AsyncIterator[AsyncApollo]:
    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(fake_apollo.handler)) as http,
        AsyncApollo("http://apollo:8080", "demo", http_client=http) as client,
    ):
        yield client


async def test_refresh_failure_keeps_cache(
    fake_client: AsyncApollo, fake_apollo: FakeApollo
) -> None:
    await fake_client.namespace()
    await fake_client.namespace("app.json")
    fake_apollo.fail_with = 500
    with pytest.raises(ApolloError):
        await fake_client.refresh()
    assert len(fake_apollo.requests) == 4
    assert await fake_client.get("timeout") == "30"


async def test_concurrent_first_load_fetches_once(fake_apollo: FakeApollo) -> None:
    async def slow_handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.01)
        return fake_apollo.handler(request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(slow_handler))
    async with AsyncApollo("http://apollo:8080", "demo", http_client=http) as client:
        results = await asyncio.gather(*(client.get("timeout") for _ in range(5)))
    assert results == ["30"] * 5
    assert len(fake_apollo.requests) == 1


async def test_namespaces_load_independently(fake_apollo: FakeApollo) -> None:
    gate = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/application"):
            await gate.wait()
        return fake_apollo.handler(request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with AsyncApollo("http://apollo:8080", "demo", http_client=http) as client:
        slow = asyncio.create_task(client.namespace("application"))
        # app.json must not wait behind the blocked application load
        assert await asyncio.wait_for(client.get("content", namespace="app.json"), 1) == '{"a": 1}'
        gate.set()
        await slow


async def test_max_age_failure_serves_cache(
    fake_apollo: FakeApollo, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake_apollo.handler))
    async with AsyncApollo("http://apollo:8080", "demo", max_age=10, http_client=http) as client:
        await client.get("timeout")
        fake_apollo.fail_with = 500
        clock.now += 10
        with caplog.at_level(logging.WARNING, logger="papollo"):
            assert await client.get("timeout") == "30"
        assert "HTTP 500" in caplog.text
        clock.now += 9
        await client.get("timeout")
        assert len(fake_apollo.requests) == 2


async def test_max_age_readers_do_not_wait(fake_apollo: FakeApollo, clock: Clock) -> None:
    gate = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        if fake_apollo.requests:
            await gate.wait()
        return fake_apollo.handler(request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with AsyncApollo("http://apollo:8080", "demo", max_age=10, http_client=http) as client:
        await client.get("timeout")
        fake_apollo.publish("application", "r2", {"timeout": "60"})
        clock.now += 10
        refresher = asyncio.create_task(client.get("timeout"))
        while not client._locks["application"].locked():
            await asyncio.sleep(0)
        assert await asyncio.wait_for(client.get("timeout"), 1) == "30"
        gate.set()
        assert await refresher == "60"
    assert len(fake_apollo.requests) == 2


async def test_listener_errors_logged(
    fake_client: AsyncApollo, fake_apollo: FakeApollo, caplog: pytest.LogCaptureFixture
) -> None:
    calls: list[str] = []

    async def broken(namespace: str, old: object, new: object) -> None:
        raise RuntimeError("boom")

    fake_client.add_listener(broken)
    fake_client.add_listener(lambda ns, old, new: calls.append(ns))
    await fake_client.get("timeout")
    fake_apollo.publish("application", "r2", {"timeout": "60"})
    with caplog.at_level(logging.ERROR, logger="papollo"):
        await fake_client.refresh()
    assert await fake_client.get("timeout") == "60"
    assert calls == ["application"]
    [record] = caplog.records
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], RuntimeError)


@pytest.fixture
async def fake_watching(fake_apollo: FakeApollo) -> AsyncIterator[AsyncApollo]:
    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(fake_apollo.async_handler)) as http,
        AsyncApollo(
            "http://apollo:8080", "demo", watch=True, http_client=http, watch_http_client=http
        ) as client,
    ):
        yield client


async def test_watch_refetch_sends_messages(
    fake_watching: AsyncApollo, fake_apollo: FakeApollo
) -> None:
    changed = asyncio.Event()
    fake_watching.add_listener(lambda ns, old, new: changed.set())
    await fake_watching.get("timeout")
    fake_apollo.publish("application", "r2", {"timeout": "60"})
    await asyncio.wait_for(changed.wait(), 5)
    assert await fake_watching.get("timeout") == "60"
    request = fake_apollo.config_requests[-1]
    assert request.url.params["messages"] == '{"details":{"demo+default+application":2}}'


@pytest.mark.usefixtures("fast_retry")
async def test_watch_backoff(
    fake_watching: AsyncApollo, fake_apollo: FakeApollo, caplog: pytest.LogCaptureFixture
) -> None:
    fake_apollo.notifications_fail_with = 500
    with caplog.at_level(logging.WARNING, logger="papollo"):
        await fake_watching.get("timeout")
        await async_wait_until(lambda: len(retry_delays(caplog)) >= 4)
        assert retry_delays(caplog)[:4] == ["0.01", "0.02", "0.04", "0.04"]
        assert "HTTP 500" in caplog.text
        fake_apollo.notifications_fail_with = None
        polls = len(fake_apollo.poll_requests)
        await async_wait_until(lambda: len(fake_apollo.poll_requests) > polls + 1)
        caplog.clear()
        fake_apollo.notifications_fail_with = 500
        await async_wait_until(lambda: retry_delays(caplog))
        assert retry_delays(caplog)[0] == "0.01"
        await fake_watching.aclose()


@pytest.mark.usefixtures("fast_retry")
async def test_watch_survives_unexpected_errors(
    fake_watching: AsyncApollo,
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

    monkeypatch.setattr("papollo.async_client.parse_notifications_response", broken)
    changed = asyncio.Event()
    fake_watching.add_listener(lambda ns, old, new: changed.set())
    with caplog.at_level(logging.WARNING, logger="papollo"):
        await fake_watching.get("timeout")
        await async_wait_until(lambda: calls > 1)
    [record] = caplog.records
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], RuntimeError)
    fake_apollo.publish("application", "r2", {"timeout": "60"})
    await asyncio.wait_for(changed.wait(), 5)


@pytest.mark.usefixtures("fast_retry")
async def test_watch_refetch_failure_retried(
    fake_watching: AsyncApollo, fake_apollo: FakeApollo
) -> None:
    changed = asyncio.Event()
    fake_watching.add_listener(lambda ns, old, new: changed.set())
    await fake_watching.get("timeout")
    await async_wait_until(lambda: fake_watching._notification_ids.get("application") == 1)
    fake_apollo.fail_with = 500
    fake_apollo.publish("application", "r2", {"timeout": "60"})
    await async_wait_until(lambda: len(fake_apollo.config_requests) >= 4)
    assert fake_watching._notification_ids["application"] == 1
    fake_apollo.fail_with = None
    await asyncio.wait_for(changed.wait(), 5)
    assert fake_watching._notification_ids["application"] == 2


async def test_watch_starts_after_first_load(fake_watching: AsyncApollo) -> None:
    with pytest.raises(ApolloError):
        await fake_watching.namespace("nope")
    assert fake_watching._poll_task is None
    await fake_watching.get("timeout")
    assert poller_running(fake_watching)


async def test_closed_client_does_not_watch(fake_watching: AsyncApollo) -> None:
    await fake_watching.aclose()
    assert await fake_watching.get("timeout") == "30"
    assert fake_watching._poll_task is None


async def test_aclose_in_listener_stops_watch(
    fake_watching: AsyncApollo, fake_apollo: FakeApollo
) -> None:
    async def listener(namespace: str, old: object, new: object) -> None:
        await fake_watching.aclose()

    fake_watching.add_listener(listener)
    await fake_watching.get("timeout")
    fake_apollo.publish("application", "r2", {"timeout": "60"})
    # Called in the poller task, which then stops instead of polling a closed client forever.
    await async_wait_until(
        lambda: fake_watching._poll_task is not None and fake_watching._poll_task.done()
    )


@pytest.fixture
async def fake_http(fake_apollo: FakeApollo) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(transport=httpx.MockTransport(fake_apollo.handler)) as http:
        yield http


@pytest.mark.parametrize(("status_code", "served"), [(500, True), (401, False), (404, False)])
async def test_cache_fallback_status(
    fake_apollo: FakeApollo,
    fake_http: httpx.AsyncClient,
    tmp_path: Path,
    status_code: int,
    served: bool,
) -> None:
    write_cache(tmp_path, "application", "r0", {"timeout": "10"})
    fake_apollo.fail_with = status_code
    async with AsyncApollo(
        "http://apollo:8080", "demo", cache_dir=tmp_path, http_client=fake_http
    ) as client:
        if served:
            assert await client.get("timeout") == "10"
        else:
            with pytest.raises(ApolloError) as info:
                await client.get("timeout")
            assert info.value.status_code == status_code


async def test_cache_not_served_for_bad_url(tmp_path: Path) -> None:
    write_cache(tmp_path, "application", "r0", {"timeout": "10"})
    async with AsyncApollo("apollo:8080", "demo", cache_dir=tmp_path) as client:
        with pytest.raises(ApolloError):
            await client.get("timeout")


async def test_cache_refresh_raises_but_serves(
    fake_apollo: FakeApollo, fake_http: httpx.AsyncClient, tmp_path: Path
) -> None:
    write_cache(tmp_path, "application", "r0", {"timeout": "10"})
    fake_apollo.fail_with = 500
    async with AsyncApollo(
        "http://apollo:8080", "demo", cache_dir=tmp_path, http_client=fake_http
    ) as client:
        with pytest.raises(ApolloError) as info:
            await client.refresh("application")
        assert info.value.status_code == 500
        assert await client.get("timeout") == "10"
        assert len(fake_apollo.requests) == 1


async def test_cache_caught_up(
    fake_apollo: FakeApollo, fake_http: httpx.AsyncClient, tmp_path: Path
) -> None:
    path = write_cache(tmp_path, "application", "r1", {"timeout": "30", "name": "demo"})
    fake_apollo.fail_with = 500
    changes: list[dict[str, str]] = []
    async with AsyncApollo(
        "http://apollo:8080", "demo", cache_dir=tmp_path, http_client=fake_http
    ) as client:
        client.add_listener(lambda ns, old, new: changes.append(dict(new)))
        before = await client.namespace()
        fake_apollo.fail_with = None
        await client.refresh()
        assert fake_apollo.config_requests[-1].url.params["releaseKey"] == "r1"
        assert await client.namespace() is before
        fake_apollo.publish("application", "r2", {"timeout": "60"})
        await client.refresh()
        assert await client.get("timeout") == "60"
    assert changes == [{"timeout": "60"}]
    assert parse_cache(path.read_bytes()).release_key == "r2"
    assert os.listdir(tmp_path) == [path.name]


async def test_cache_corrupt(
    fake_apollo: FakeApollo,
    fake_http: httpx.AsyncClient,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    write_cache(tmp_path, "application", "r0", {"timeout": "10"}).write_bytes(b"\x00")
    fake_apollo.fail_with = 500
    with caplog.at_level(logging.WARNING, logger="papollo"):
        async with AsyncApollo(
            "http://apollo:8080", "demo", cache_dir=tmp_path, http_client=fake_http
        ) as client:
            with pytest.raises(ApolloError) as info:
                await client.get("timeout")
    assert info.value.status_code == 500
    assert "ignoring unreadable config cache" in caplog.text


async def test_cache_write_failure_logged(
    fake_http: httpx.AsyncClient, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    not_a_dir = tmp_path / "file"
    not_a_dir.touch()
    with caplog.at_level(logging.WARNING, logger="papollo"):
        async with AsyncApollo(
            "http://apollo:8080", "demo", cache_dir=not_a_dir, http_client=fake_http
        ) as client:
            assert await client.get("timeout") == "30"
    assert "failed to write config cache" in caplog.text


@pytest.mark.usefixtures("fast_retry")
async def test_cache_watch_catches_up(fake_apollo: FakeApollo, tmp_path: Path) -> None:
    path = write_cache(tmp_path, "application", "r1", {"timeout": "30", "name": "demo"})
    fake_apollo.fail_with = 500
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake_apollo.async_handler))
    async with (
        http,
        AsyncApollo(
            "http://apollo:8080",
            "demo",
            watch=True,
            cache_dir=tmp_path,
            http_client=http,
            watch_http_client=http,
        ) as client,
    ):
        assert await client.get("timeout") == "30"
        await async_wait_until(lambda: fake_apollo.poll_requests)
        notifications = json.loads(fake_apollo.poll_requests[0].url.params["notifications"])
        assert notifications == [{"namespaceName": "application", "notificationId": -1}]
        fake_apollo.publish("application", "r2", {"timeout": "60"})
        fake_apollo.fail_with = None
        await async_wait_until(lambda: client._snapshots["application"].release_key == "r2")
        assert await client.get("timeout") == "60"
    assert parse_cache(path.read_bytes()).release_key == "r2"
