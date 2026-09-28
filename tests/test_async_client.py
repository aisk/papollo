import asyncio
import logging
from collections.abc import AsyncIterator, Mapping

import httpx
import pytest

from papollo import ApolloError, AsyncApollo

from .conftest import App, Clock, FakeApollo, ResponseRecorder, requires_fork, run_in_child


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
