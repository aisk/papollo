import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest

from papollo import ApolloError, AsyncApolloClient

from .conftest import App, FakeApollo, ResponseRecorder


@pytest.fixture
async def client(app: App, recorder: ResponseRecorder) -> AsyncIterator[AsyncApolloClient]:
    async with (
        httpx.AsyncClient(event_hooks={"response": [recorder.async_hook]}) as http,
        AsyncApolloClient(app.config_url, app.app_id, http_client=http) as client,
    ):
        yield client


async def test_get(client: AsyncApolloClient) -> None:
    assert await client.get("timeout") == "30"
    assert await client.get("missing") is None
    assert await client.get("missing", "1") == "1"
    assert await client.namespace("app.json") == {"content": '{"a": 1}'}


async def test_properties_suffix(client: AsyncApolloClient, recorder: ResponseRecorder) -> None:
    assert await client.namespace("application") is await client.namespace("application.properties")
    assert recorder.statuses == [200]


async def test_refresh(app: App, client: AsyncApolloClient, recorder: ResponseRecorder) -> None:
    before = await client.namespace()
    await client.refresh()
    assert await client.namespace() is before
    app.publish("application", "timeout=60")
    await client.refresh()
    assert await client.get("timeout") == "60"
    assert before["timeout"] == "30"
    assert recorder.statuses == [200, 304, 200]


async def test_refresh_loads_unloaded_namespace(
    client: AsyncApolloClient, recorder: ResponseRecorder
) -> None:
    await client.refresh("app.json")
    await client.namespace("app.json")
    assert recorder.statuses == [200]


async def test_refresh_all_only_touches_loaded(
    client: AsyncApolloClient, recorder: ResponseRecorder
) -> None:
    await client.refresh()
    assert recorder.statuses == []


async def test_missing_namespace(client: AsyncApolloClient) -> None:
    with pytest.raises(ApolloError) as info:
        await client.namespace("nope")
    assert info.value.status_code == 404


async def test_access_key(app: App) -> None:
    secret = app.portal.enable_access_key(app.app_id)
    async with AsyncApolloClient(app.config_url, app.app_id, secret=secret) as client:
        assert await client.get("timeout") == "30"


# The tests below need no server, or a failure a real server can not produce on demand.


async def test_network_error_wrapped() -> None:
    async with AsyncApolloClient("http://127.0.0.1:1", "demo") as client:
        with pytest.raises(ApolloError) as info:
            await client.get("timeout")
    assert isinstance(info.value.__cause__, httpx.ConnectError)


async def test_owned_http_client_closed() -> None:
    client = AsyncApolloClient("http://apollo:8080", "demo")
    await client.aclose()
    assert client._http.is_closed


@pytest.fixture
async def fake_client(fake_apollo: FakeApollo) -> AsyncIterator[AsyncApolloClient]:
    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(fake_apollo.handler)) as http,
        AsyncApolloClient("http://apollo:8080", "demo", http_client=http) as client,
    ):
        yield client


async def test_refresh_failure_keeps_cache(
    fake_client: AsyncApolloClient, fake_apollo: FakeApollo
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
    async with AsyncApolloClient("http://apollo:8080", "demo", http_client=http) as client:
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
    async with AsyncApolloClient("http://apollo:8080", "demo", http_client=http) as client:
        slow = asyncio.create_task(client.namespace("application"))
        # app.json must not wait behind the blocked application load
        assert await asyncio.wait_for(client.get("content", namespace="app.json"), 1) == '{"a": 1}'
        gate.set()
        await slow
