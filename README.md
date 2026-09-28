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
