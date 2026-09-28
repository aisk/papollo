import asyncio

import httpx
import pytest

from papollo import ApolloError, AsyncApolloClient

from .conftest import FakeApollo


@pytest.fixture
async def client(apollo: FakeApollo):
    http = httpx.AsyncClient(transport=httpx.MockTransport(apollo.handler))
    async with AsyncApolloClient("http://apollo:8080/", "demo", http_client=http) as client:
        yield client
    await http.aclose()


async def test_refresh_loads_unloaded_namespace(
    client: AsyncApolloClient, apollo: FakeApollo
) -> None:
    await client.refresh("app.json")
    await client.namespace("app.json")
    assert len(apollo.requests) == 1


async def test_refresh_failure_keeps_cache(client: AsyncApolloClient, apollo: FakeApollo) -> None:
    await client.namespace()
    await client.namespace("app.json")
    apollo.fail_with = 500
    with pytest.raises(ApolloError):
        await client.refresh()
    assert len(apollo.requests) == 4
    assert await client.get("timeout") == "30"


async def test_concurrent_first_load_fetches_once(apollo: FakeApollo) -> None:
    async def slow_handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.01)
        return apollo.handler(request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(slow_handler))
    async with AsyncApolloClient("http://apollo:8080", "demo", http_client=http) as client:
        results = await asyncio.gather(*(client.get("timeout") for _ in range(5)))
    assert results == ["30"] * 5
    assert len(apollo.requests) == 1


async def test_refresh_all_only_touches_loaded(
    client: AsyncApolloClient, apollo: FakeApollo
) -> None:
    await client.refresh()
    assert apollo.requests == []


async def test_network_error_wrapped() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with AsyncApolloClient("http://apollo:8080", "demo", http_client=http) as client:
        with pytest.raises(ApolloError) as info:
            await client.get("timeout")
    assert isinstance(info.value.__cause__, httpx.ConnectError)
    assert not http.is_closed
    await http.aclose()


async def test_namespaces_load_independently(apollo: FakeApollo) -> None:
    gate = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/application"):
            await gate.wait()
        return apollo.handler(request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with AsyncApolloClient("http://apollo:8080", "demo", http_client=http) as client:
        slow = asyncio.create_task(client.namespace("application"))
        # app.json must not wait behind the blocked application load
        assert await asyncio.wait_for(client.get("content", namespace="app.json"), 1) == '{"a": 1}'
        gate.set()
        await slow
