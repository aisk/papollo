# papollo

Python client for [Apollo](https://github.com/apolloconfig/apollo) config center, with sync and async support.

```python
from papollo import Apollo

client = Apollo("http://apollo-config:8080", "demo-app")

client.get("timeout")                          # "30", or None if missing
client.get("timeout", "10")                    # with default
client.get("db.url", namespace="database")
client.namespace("app.json")["content"]        # non-properties namespace
client.refresh()                               # pull latest releases
```

```python
from papollo import AsyncApollo

client = AsyncApollo("http://apollo-config:8080", "demo-app")

await client.get("timeout")
await client.refresh()
```

A client is meant to live as long as the process, usually as a module level object. It holds an
httpx connection pool, so call `close()` (or `await client.aclose()`) if you create short lived
clients. Both also work as context managers. An `AsyncApollo` is bound to the event loop it
is first used in.

Clients survive `os.fork()`, so one created before gunicorn or a multiprocessing pool forks its
workers keeps working in them. A child keeps the configs already loaded and gets a new connection
pool, as sharing the parent's sockets would mix up responses. An `AsyncApollo` can then be used in
the child's own event loop. An `http_client` you pass in is left alone, create it after the fork.

Namespaces are fetched on first access and then served from memory. `refresh()` refetches every
loaded namespace, and `refresh(name)` also loads a namespace that was not loaded yet, which is handy
for failing fast at startup. Failures raise `ApolloError` and keep the previously cached config.

To pick up new releases without calling `refresh()`, set `max_age` in seconds. A read of a
namespace older than that refetches it first, which is cheap when nothing changed as the server
answers 304. There is no background thread, a namespace nobody reads is not refetched. While one
reader refreshes, others are served the cached config. If the refresh fails the cached config is
served and a warning is logged to the `papollo` logger, and the next try waits another `max_age`.
Apollo suggests polling no more often than every 30 seconds.

Other options: `cluster`, `secret` (access key), `ip` and `label` (gray release), `timeout`, and
`http_client` to bring your own `httpx.Client` / `httpx.AsyncClient`. When `timeout` is not set, the
httpx client's own timeout is used. `ip` is not detected automatically, so IP based gray release
only works when it is set.

## Testing

Most tests run against a real Apollo at `http://localhost:8080`, the rest cover pure functions and
failures a real server can not produce on demand. The easiest server is the
[quick start](https://github.com/apolloconfig/apollo-quick-start) all in one jar with an in memory
H2 database:

```sh
SPRING_PROFILES_ACTIVE=github,database-discovery,auth SPRING_PROFILES_GROUP_GITHUB=h2 \
LOGGING_FILE_NAME=/tmp/apollo.log java -jar apollo-all-in-one.jar

uv run pytest
```

Tests that need Apollo are skipped when it is not reachable. Set `PAPOLLO_REQUIRE_APOLLO=1` to make
them fail instead, which is what CI should do. The portal at `http://localhost:8070` is used to
create apps and publish releases. Use `APOLLO_CONFIG_URL`, `APOLLO_PORTAL_URL`,
`APOLLO_PORTAL_USER`, `APOLLO_PORTAL_PASSWORD` and `APOLLO_ENV` to point elsewhere.
