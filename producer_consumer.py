"""Simple producer-consumer example using threads and a shared buffer."""
from __future__ import annotations

from dataclasses import dataclass
import time
from threading import Condition, Event, Thread
from threading import Lock
from typing import Callable, Generic, Iterable, List, Optional, TypeVar

T = TypeVar("T")


class QueueClosed(RuntimeError):
    """Raised when someone tries to read from a closed and empty buffer."""


class BoundedBuffer(Generic[T]):
    """Bounded buffer that blocks writers when full and readers when empty."""

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._items: List[T] = []
        self._closed = False
        self._cond = Condition()

    def put(self, item: T, *, timeout: Optional[float] = None) -> None:
        """Add an item, waiting for space, or fail if closed or timed out."""
        with self._cond:
            if self._closed:
                raise QueueClosed("buffer is closed")

            # Wait if the buffer is full.
            end_time = time.monotonic() + timeout if timeout is not None else None

            while len(self._items) >= self._capacity and not self._closed:
                remaining = None if end_time is None else end_time - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("put timed out waiting for space")
                self._cond.wait(timeout=remaining)

            if self._closed:
                raise QueueClosed("buffer is closed")

            self._items.append(item)
            self._cond.notify_all()

    def get(self, *, timeout: Optional[float] = None) -> T:
        """Take an item, waiting if empty, or fail if closed and drained."""
        with self._cond:
            end_time = time.monotonic() + timeout if timeout is not None else None

            while not self._items and not self._closed:
                remaining = None if end_time is None else end_time - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("get timed out waiting for item")
                self._cond.wait(timeout=remaining)

            if not self._items and self._closed:
                raise QueueClosed("buffer closed and drained")

            item = self._items.pop(0)
            self._cond.notify_all()
            return item

    def close(self) -> None:
        """Stop new puts and wake any waiting threads."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def size(self) -> int:
        with self._cond:
            return len(self._items)


@dataclass
class SourceContainer(Generic[T]):
    """Holds items that a producer will send."""

    items: Iterable[T]


class DestinationContainer(Generic[T]):
    """Stores items that consumers take out of the buffer."""

    def __init__(self) -> None:
        self._items: List[T] = []
        self._lock = Lock()

    def add(self, item: T) -> None:
        with self._lock:
            self._items.append(item)

    def snapshot(self) -> List[T]:
        """Return a copy of the stored items."""
        with self._lock:
            return list(self._items)


class Producer(Thread, Generic[T]):
    """Producer thread that reads from a source and pushes into the buffer."""

    def __init__(
        self,
        source: SourceContainer[T],
        buffer: BoundedBuffer[T],
        *,
        close_on_complete: bool = True,
        name: Optional[str] = None,
    ) -> None:
        super().__init__(name=name)
        self._source = source
        self._buffer = buffer
        self._close_on_complete = close_on_complete
        self._stop_event = Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        try:
            for item in self._source.items:
                if self._stop_event.is_set():
                    break
                self._buffer.put(item)
        finally:
            if self._close_on_complete:
                self._buffer.close()


class Consumer(Thread, Generic[T]):
    """Consumer thread that pulls from the buffer and stores results."""

    def __init__(
        self,
        buffer: BoundedBuffer[T],
        destination: DestinationContainer[T],
        *,
        process_fn: Optional[Callable[[T], T]] = None,
        name: Optional[str] = None,
    ) -> None:
        super().__init__(name=name)
        self._buffer = buffer
        self._destination = destination
        self._process_fn = process_fn
        self._stop_event = Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                item = self._buffer.get(timeout=0.1)
            except TimeoutError:
                continue
            except QueueClosed:
                break

            if self._process_fn:
                item = self._process_fn(item)
            self._destination.add(item)


def run_demo() -> None:
    """
    Minimal demo for manual execution:
        python producer_consumer.py
    """
    source = SourceContainer(items=range(10))
    buffer: BoundedBuffer[int] = BoundedBuffer(capacity=3)
    destination: DestinationContainer[int] = DestinationContainer()

    producer = Producer(source, buffer, close_on_complete=True, name="producer")
    consumer = Consumer(buffer, destination, name="consumer")

    producer.start()
    consumer.start()

    producer.join()
    consumer.join()

    print("Destination contents:", destination.snapshot())


if __name__ == "__main__":
    run_demo()
