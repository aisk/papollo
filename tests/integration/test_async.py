from collections.abc import AsyncIterator

import httpx
import pytest

from papollo import ApolloError, AsyncApolloClient

from .conftest import App, StatusRecorder


@pytest.fixture
def recorder() -> StatusRecorder:
    return StatusRecorder()


@pytest.fixture
async def client(app: App, recorder: StatusRecorder) -> AsyncIterator[AsyncApolloClient]:
    async with (
        httpx.AsyncClient(event_hooks={"response": [recorder.async_hook]}) as http,
        AsyncApolloClient(app.config_url, app.app_id, http_client=http) as client,
    ):
        yield client


async def test_get(client: AsyncApolloClient) -> None:
    assert await client.get("timeout") == "30"
    assert await client.get("missing", "1") == "1"
    assert await client.namespace("app.json") == {"content": '{"a": 1}'}


async def test_refresh(app: App, client: AsyncApolloClient, recorder: StatusRecorder) -> None:
    before = await client.namespace()
    await client.refresh()
    assert await client.namespace() is before
    app.publish("application", "timeout=60")
    await client.refresh()
    assert await client.get("timeout") == "60"
    assert recorder.statuses == [200, 304, 200]


async def test_missing_namespace(client: AsyncApolloClient) -> None:
    with pytest.raises(ApolloError) as info:
        await client.namespace("nope")
    assert info.value.status_code == 404


async def test_access_key(app: App) -> None:
    secret = app.portal.enable_access_key(app.app_id)
    async with AsyncApolloClient(app.config_url, app.app_id, secret=secret) as client:
        assert await client.get("timeout") == "30"
