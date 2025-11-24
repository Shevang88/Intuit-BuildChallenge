import time
import unittest
from threading import Thread

from producer_consumer import (
    BoundedBuffer,
    Consumer,
    DestinationContainer,
    Producer,
    QueueClosed,
    SourceContainer,
)


class BoundedBufferTests(unittest.TestCase):
    def test_put_get_order_preserved(self) -> None:
        buffer: BoundedBuffer[int] = BoundedBuffer(capacity=3)
        buffer.put(1)
        buffer.put(2)
        buffer.put(3)

        self.assertEqual(buffer.get(), 1)
        self.assertEqual(buffer.get(), 2)
        self.assertEqual(buffer.get(), 3)

    def test_put_times_out_when_full(self) -> None:
        buffer: BoundedBuffer[int] = BoundedBuffer(capacity=1)
        buffer.put(10)
        with self.assertRaises(TimeoutError):
            buffer.put(20, timeout=0.05)

    def test_get_times_out_when_empty(self) -> None:
        buffer: BoundedBuffer[int] = BoundedBuffer(capacity=1)
        with self.assertRaises(TimeoutError):
            buffer.get(timeout=0.05)

    def test_get_raises_after_close_and_empty(self) -> None:
        buffer: BoundedBuffer[int] = BoundedBuffer(capacity=1)
        buffer.close()
        with self.assertRaises(QueueClosed):
            buffer.get(timeout=0.05)

    def test_put_raises_when_closed(self) -> None:
        buffer: BoundedBuffer[int] = BoundedBuffer(capacity=1)
        buffer.close()
        with self.assertRaises(QueueClosed):
            buffer.put(1, timeout=0.05)


class ProducerConsumerTests(unittest.TestCase):
    def test_moves_all_items(self) -> None:
        source = SourceContainer(items=range(5))
        buffer: BoundedBuffer[int] = BoundedBuffer(capacity=2)
        destination: DestinationContainer[int] = DestinationContainer()

        producer = Producer(source, buffer, close_on_complete=True, name="producer-test")
        consumer = Consumer(buffer, destination, name="consumer-test")

        producer.start()
        consumer.start()

        producer.join(timeout=1)
        consumer.join(timeout=1)

        self.assertFalse(producer.is_alive(), "producer should finish")
        self.assertFalse(consumer.is_alive(), "consumer should finish after buffer closes")
        self.assertEqual(destination.snapshot(), list(range(5)))

    def test_process_fn_applies_transformation(self) -> None:
        source = SourceContainer(items=[1, 2, 3])
        buffer: BoundedBuffer[int] = BoundedBuffer(capacity=2)
        destination: DestinationContainer[int] = DestinationContainer()

        consumer = Consumer(buffer, destination, process_fn=lambda x: x * 2, name="consumer-transform")
        producer = Producer(source, buffer, close_on_complete=True, name="producer-transform")

        consumer.start()
        producer.start()

        producer.join(timeout=1)
        consumer.join(timeout=1)

        self.assertEqual(destination.snapshot(), [2, 4, 6])

    def test_multiple_producers_and_consumers(self) -> None:
        producer_count = 10
        consumer_count = 10
        items_per_producer = 5

        buffer: BoundedBuffer[tuple[str, int]] = BoundedBuffer(capacity=3)
        destination: DestinationContainer[tuple[str, int]] = DestinationContainer()

        producers = []
        expected = []
        for idx in range(producer_count):
            name = f"p{idx}"
            items = [(name, i) for i in range(items_per_producer)]
            expected.extend(items)
            producers.append(
                Producer(
                    SourceContainer(items=items),
                    buffer,
                    close_on_complete=False,
                    name=f"producer-{name}",
                )
            )

        consumers = [
            Consumer(buffer, destination, name=f"consumer-{i}") for i in range(consumer_count)
        ]

        for t in consumers + producers:
            t.start()

        for t in producers:
            t.join(timeout=2)
            self.assertFalse(t.is_alive(), f"{t.name} should finish")

        buffer.close()

        for t in consumers:
            t.join(timeout=2)
            self.assertFalse(t.is_alive(), f"{t.name} should finish after close")

        received = sorted(destination.snapshot())
        self.assertEqual(received, sorted(expected))

    def test_heavy_throughput_under_contention(self) -> None:
        producer_count = 10
        consumer_count = 10
        items_per_producer = 20

        buffer: BoundedBuffer[int] = BoundedBuffer(capacity=4)
        destination: DestinationContainer[int] = DestinationContainer()

        producers = [
            Producer(
                SourceContainer(items=[(p * items_per_producer) + i for i in range(items_per_producer)]),
                buffer,
                close_on_complete=False,
                name=f"producer-{p}",
            )
            for p in range(producer_count)
        ]
        consumers = [
            Consumer(buffer, destination, name=f"consumer-{i}") for i in range(consumer_count)
        ]

        for t in consumers + producers:
            t.start()

        for t in producers:
            t.join(timeout=2)
            self.assertFalse(t.is_alive(), f"{t.name} should finish")

        buffer.close()

        for t in consumers:
            t.join(timeout=2)
            self.assertFalse(t.is_alive(), f"{t.name} should finish after close")

        received = destination.snapshot()
        expected_count = producer_count * items_per_producer
        self.assertEqual(len(received), expected_count)
        self.assertEqual(sorted(received), list(range(expected_count)))

    def test_producer_blocks_until_space_available(self) -> None:
        buffer: BoundedBuffer[int] = BoundedBuffer(capacity=1)
        buffer.put(1)

        durations = []

        def put_second() -> None:
            start = time.monotonic()
            buffer.put(2)
            durations.append(time.monotonic() - start)

        t = Thread(target=put_second, name="blocking-put")
        t.start()

        time.sleep(0.05)
        self.assertTrue(t.is_alive(), "producer should block when buffer is full")
        self.assertEqual(buffer.size, 1)

        self.assertEqual(buffer.get(), 1)
        t.join(timeout=0.5)
        self.assertFalse(t.is_alive(), "producer should finish after space frees")

        self.assertTrue(durations, "duration should be recorded")
        self.assertGreaterEqual(durations[0], 0.04)
        self.assertEqual(buffer.get(), 2)

    def test_consumer_stop_exits_even_when_waiting(self) -> None:
        buffer: BoundedBuffer[int] = BoundedBuffer(capacity=1)
        destination: DestinationContainer[int] = DestinationContainer()
        consumer = Consumer(buffer, destination, name="consumer-stop")

        consumer.start()
        time.sleep(0.05)
        consumer.stop()
        consumer.join(timeout=0.5)

        self.assertFalse(consumer.is_alive())
        self.assertEqual(destination.snapshot(), [])


if __name__ == "__main__":
    unittest.main()
