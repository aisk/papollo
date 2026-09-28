"""Keep clients usable in a child process created by ``os.fork()``.

A forked child inherits the parent's locks, possibly held by a thread that does not exist in the
child, and the sockets of the parent's connection pool, which sibling children would then share.
Clients register here and reset that state in the child, keeping the configs already loaded.
"""

import os
import weakref
from typing import Protocol


class _Forkable(Protocol):
    def _after_fork_in_child(self) -> None: ...


_clients: "weakref.WeakSet[_Forkable]" = weakref.WeakSet()


def track(client: _Forkable) -> None:
    _clients.add(client)


def _after_fork_in_child() -> None:
    for client in list(_clients):
        client._after_fork_in_child()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)
