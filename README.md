# papollo

Python client for [Apollo](https://github.com/apolloconfig/apollo) config center, with sync and async support.

```python
from papollo import ApolloClient

client = ApolloClient("http://apollo-config:8080", "demo-app")

client.get("timeout")                          # "30", or None if missing
client.get("timeout", "10")                    # with default
client.get("db.url", namespace="database")
client.namespace("app.json")["content"]        # non-properties namespace
client.refresh()                               # pull latest releases
```

```python
from papollo import AsyncApolloClient

client = AsyncApolloClient("http://apollo-config:8080", "demo-app")

await client.get("timeout")
await client.refresh()
```

Namespaces are fetched on first access and then served from memory. `refresh()` refetches every
loaded namespace, and `refresh(name)` also loads a namespace that was not loaded yet, which is handy
for failing fast at startup. Failures raise `ApolloError` and keep the previously cached config.

Other options: `cluster`, `secret` (access key), `ip` and `label` (gray release), `timeout`, and
`http_client` to bring your own `httpx.Client` / `httpx.AsyncClient`. When `timeout` is not set, the
httpx client's own timeout is used. `ip` is not detected automatically, so IP based gray release
only works when it is set.

An `AsyncApolloClient` is bound to the event loop it is first used in.

A client is meant to live as long as the process, usually as a module level object. It holds an
httpx connection pool, so call `close()` (or `await client.aclose()`) if you create short lived
clients. Both also work as context managers.

## Testing

`uv run pytest` runs the unit tests. Integration tests under `tests/integration` run against a real
Apollo and are skipped unless `APOLLO_CONFIG_URL` is set. The easiest server is the
[quick start](https://github.com/apolloconfig/apollo-quick-start) all in one jar with an in memory
H2 database:

```sh
SPRING_PROFILES_ACTIVE=github,database-discovery,auth SPRING_PROFILES_GROUP_GITHUB=h2 \
LOGGING_FILE_NAME=/tmp/apollo.log java -jar apollo-all-in-one.jar

APOLLO_CONFIG_URL=http://localhost:8080 uv run pytest
```

The portal at `http://localhost:8070` is used to create apps and publish releases. It can be changed
with `APOLLO_PORTAL_URL`, `APOLLO_PORTAL_USER`, `APOLLO_PORTAL_PASSWORD` and `APOLLO_ENV`.
