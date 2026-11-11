# papollo

Python client for [Apollo](https://github.com/apolloconfig/apollo) config center, with sync and
async support.

```sh
pip install papollo
```

```python
from papollo import Apollo

client = Apollo("http://apollo-config:8080", "demo-app")

client.get("timeout")  # "30", or None if missing
client.get("timeout", "10")  # with default
client.get("db.url", namespace="database")
client.namespace("app.json")["content"]  # non-properties namespace
client.refresh()  # pull latest releases
```

`AsyncApollo` has the same API, with `await` and `aclose()`. Namespaces are fetched on first access
and then served from memory. Failures raise `ApolloError` and keep the config already loaded. A
client is meant to live as long as the process, otherwise close it or use it as a context manager.

## Updates

Pass `watch=True` to long poll Apollo and pick up new releases within a second or so. Polling runs
in a daemon thread (or a task of the event loop for `AsyncApollo`) and starts on the first read.
Alternatively, `max_age` makes a read refetch a namespace older than that many seconds.

```python
client = Apollo("http://apollo-config:8080", "demo-app", watch=True)


@client.add_listener
def on_change(namespace, old, new):
    print(namespace, old.get("timeout"), "->", new.get("timeout"))
```

Listeners are called whenever a loaded namespace changes, no matter how the change was fetched.
`AsyncApollo` also accepts coroutine functions.

## Options

- `cluster`, `secret` (access key), `ip` and `label` (gray release), `timeout`
- `cache_dir` writes every release to disk and serves it when the config service is unreachable
  at first load, like the Java client's local cache. Use a separate directory per environment.
- `http_client` and `watch_http_client` to bring your own `httpx` clients, for example to set
  `verify` or a proxy. The watch client should be used for nothing else.

Clients survive `os.fork()`, so one created before gunicorn forks its workers keeps working in them.

## Testing

Most tests need a real Apollo at `http://localhost:8080`, such as the
[quick start](https://github.com/apolloconfig/apollo-quick-start) all in one jar, and are skipped
when it is not reachable. Set `PAPOLLO_REQUIRE_APOLLO=1` to make them fail instead.

```sh
uv run pytest
```
