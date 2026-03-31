from __future__ import annotations

from enum import Enum
from typing import Any

from . import _pgqrs
from ._pgqrs import *  # noqa: F403
from .decorators import WorkflowDef

__doc__ = _pgqrs.__doc__
__all__ = list(getattr(_pgqrs, "__all__", []))
__all__.append("WorkflowDef")
__all__.append("DurabilityMode")


class DurabilityMode(str, Enum):
    DURABLE = "durable"
    LOCAL = "local"


class DequeueBuilder:
    def __init__(self, inner: Any):
        self._inner = inner

    def worker(self, consumer: Any) -> "DequeueBuilder":
        self._inner.worker(consumer)
        return self

    def batch(self, size: int) -> "DequeueBuilder":
        self._inner.batch(size)
        return self

    def from_queue(self, queue: str) -> "DequeueBuilder":
        self._inner.from_queue(queue)
        return self

    def poll_interval(self, interval_ms: int) -> "DequeueBuilder":
        self._inner.poll_interval(interval_ms)
        return self

    def at(self, time: str) -> "DequeueBuilder":
        self._inner.at(time)
        return self

    def handle(self, handler: Any) -> "DequeueBuilder":
        self._inner.handle(handler)
        return self

    def handle_batch(self, handler: Any) -> "DequeueBuilder":
        self._inner.handle_batch(handler)
        return self

    def handle_workflow(self, handler: Any) -> "DequeueBuilder":
        self._inner.handle_workflow(handler)
        return self

    @property
    def queue(self) -> Any:
        return self._inner.queue

    @property
    def batch_size(self) -> Any:
        return self._inner.batch_size

    @property
    def poll_interval_ms(self) -> Any:
        return self._inner.poll_interval_ms

    def has_worker(self) -> bool:
        return self._inner.has_worker()

    async def poll(self, store: Any):
        # Underlying Rust binding returns an awaitable Future; wrap in a coroutine.
        return await self._inner.poll(store)


def dequeue(*args: Any, **kwargs: Any):
    """Overload-style dispatch.

    - `dequeue()` returns a fluent DequeueBuilder.
    - `dequeue(consumer, batch_size)` retains the legacy function API.
    """

    if not args and not kwargs:
        return DequeueBuilder(_pgqrs.dequeue_builder())

    return _pgqrs.dequeue(*args, **kwargs)
