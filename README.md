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

To get new releases within a second or so, pass `watch=True`. The client then long polls Apollo's
notification endpoint for every loaded namespace, and refetches a namespace as soon as the server
reports a new release for it. Polling starts with the first read or `refresh()`, not when the client
is created, so a client created at import time in a process that never reads it runs nothing. An
`Apollo` polls in a daemon thread, an `AsyncApollo` in a task of its event loop. A namespace loaded
later aborts the poll in flight so the next one covers it too. The long poll uses its own httpx
client, as the server holds each request for up to 60 seconds, never the `http_client` you pass
in, so settings like `verify` or a proxy given to that client do not apply to it. Failures are
logged as warnings to the `papollo` logger and retried after a delay doubling from 1 up to 120
seconds. `close()` and `aclose()` stop polling right away. The poller keeps the client alive until
then, so always close a watching client that does not live as long as the process. `max_age` can be
combined with `watch` as a fallback in case notifications are lost.

```python
client = Apollo("http://apollo-config:8080", "demo-app", watch=True)

@client.add_listener
def on_change(namespace, old, new):
    print(namespace, old.get("timeout"), "->", new.get("timeout"))
```

Listeners registered with `add_listener()` are called with the namespace name and its old and new
configurations whenever a loaded namespace changes, whether through `watch`, `refresh()` or
`max_age`. They are not called on first load, nor when a refetch finds nothing changed. An
`Apollo` calls them in the thread that fetched the change, a listener may read the client. An
`AsyncApollo` also accepts coroutine functions and awaits them. Exceptions raised by a listener are
logged and do not stop other listeners. `remove_listener()` unregisters one.

Polling does not survive `os.fork()`, a child starts its own on its next read. If the parent is
already polling when it forks, Python 3.12 and later emit a `DeprecationWarning` because the process
has a thread. papollo resets its own state in the child, but other libraries may not, so prefer to
read config only in the workers, for example from gunicorn's `post_fork` hook, and not in the
master process.

To start while the config service is down, set `cache_dir`, like the Java client's local cache.
Every new release fetched is also written to `{app_id}+{cluster}+{namespace}.json` in that
directory, which is created if missing. Files are replaced atomically, so processes can share a
directory, and on Unix are only readable by their owner as configs may hold secrets. File names do
not include the server URL, `ip` or `label`, so clients of different environments or gray releases
should use separate directories. When a namespace fails to load because the server is unreachable or
answers with a 5xx, a read serves the cached file instead and logs a warning. Errors such as a wrong
`secret`, an unknown namespace or a server URL without `http://` are raised as usual, a stale cache
would only hide them. `refresh(name)` loads the cached file too but still raises, so a fail fast
check at startup notices, and reads after it are served from the cache. From then on the namespace
counts as loaded with the cached release, which `refresh()`, `max_age` or `watch` refetch like any
other, and listeners are called when the server has a newer one. A cache file that can not be read
is ignored and failing to write one never fails a fetch, both are logged as warnings.

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
