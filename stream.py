import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import TracebackType
from typing import Callable, Type
from uuid import uuid4

## User Code
SECONDS = 0
points_per_batch = 10


def read_source() -> dict:
    global SECONDS
    points = [
        {"timestamp": {"seconds": SECONDS, "nanos": i}, "value": random.random()} for i in range(points_per_batch)
    ]
    batch = {"channel": "streaming-test-channel-0", "tags": {}, "points": {"type": "double", "double": points}}
    SECONDS += 1
    return batch


## End user code

## Nominal Stream Client


class NominalWriteStream:
    """Nominal Stream to write non-blocking messages to a data source ID.

    Args:
    ----
        data_source_id (str): Where to write the data.
        batch_size (int): How big the batch can get before writing to Nominal. Default 10
        max_wait_sec (int): How long a batch can exist before being flushed to Nominal

    Examples:
    --------
        Standard Usage:
        ```py
        with NominalWriteStream("source-id") as stream:
            stream.enqueue({"ts": 0, "message": "hello1"})
            stream.enqueue({"ts": 1, "message": "hello2"})
        ```

        Without a context manager:
        ```py
        stream = NominalWriteStream("source-id)
        stream.enqueue({"ts": 0, "message": "hello1"})
        stream.enqueue({"ts": 1, "message": "hello2"})
        stream.close()
        ```

    """

    def __init__(self, data_source_id: str, batch_size: int = 10, max_wait_sec: int = 5):
        """Create the stream."""
        self.data_source_id = data_source_id
        self.batch_size = batch_size
        self.max_wait_sec = max_wait_sec
        self._executor = ThreadPoolExecutor()
        self._batch = []
        self._batch_lock = threading.Lock()
        self._last_batch_time = time.time()
        self._running = True

        self._timeout_thread = threading.Thread(target=self._process_timeout_batches, daemon=True)
        self._timeout_thread.start()

        self.sink = f"sink_{uuid4()}.jsonl"
        print(f"Writing to {self.sink}")

    def __enter__(self) -> "NominalWriteStream":
        """Create the stream as a context manager."""
        return self

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> bool | None:
        """Leave the context manager. Close all running threads."""
        self.close()

    def _write_sink(self, batch: list[dict]) -> None:
        """Threaded entrypoint to write to the sink in the threadpool."""
        sleep_time = random.randint(0, 4) + 0.3  # some major fluctuation in request latency
        time.sleep(sleep_time)  # simulate some network request lag
        with open(self.sink, "a") as sink:
            for message in batch:  # just for ease of writing. in the real impl we'd of course send the full batch
                json.dump(message, sink)
                sink.write("\n")

    def enqueue(self, message: dict) -> None:
        """Add a message to the queue.

        The message will not be immediately sent to Nominal. Only after the batch size is full or the timeout occurs.
        """
        with self._batch_lock:
            self._batch.append({"message": message, "dataSourceRid": self.data_source_id})

            if len(self._batch) >= self.batch_size:
                self._flush_batch()

    def _flush_batch(self):
        if self._batch:
            self._executor.submit(self._write_sink, self._batch)
            self._batch = []
            self._last_batch_time = time.time()

    def _process_timeout_batches(self):
        while self._running:
            time.sleep(self.max_wait_sec / 10)
            with self._batch_lock:
                if self._batch and (time.time() - self._last_batch_time) >= self.max_wait_sec:
                    self._flush_batch()

    def close(self, wait=True) -> None:
        """Close the Nominal Stream.

        Stop the process timeout thread
        Flush any remaining batches
        """
        self._running = False
        self._timeout_thread.join()

        with self._batch_lock:
            self._flush_batch()

        self._executor.shutdown(wait=wait, cancel_futures=not wait)


def write_stream(source: Callable[[], dict], data_source_id: str, push_freq_sec: float | int = 1) -> NominalWriteStream:
    """Start a write stream based on a callable source.

    Args:
    ----
        source (Callable[[], dict]): A callable function that returns a dictionary of data to write to the source.
            The callable must not take any parameters. If it does, use a `functools.partial` to pre-fill the params
        data_source_id (str): The source in nominal to write to.
        push_freq_sec (int): The frequency with which to read from the source and write to the sink. Default 1.

    Returns:
    -------
        NominalWriteStream: The write task running the process. To end the stream, call stream.stop()

    """
    stream = NominalWriteStream(source, data_source_id, push_freq_sec)
    print(f"Pushing to sink {stream.sink}")
    return stream


if __name__ == "__main__":
    # Read at a higher frequency than the sink can handle. We'll ensure that no messages are dropped
    with NominalWriteStream("source-1") as stream:
        for _ in range(1_000):
            data = read_source()
            stream.enqueue(data)
            time.sleep(0.001)  # 1000hz
        print("Stopping...")
    print("Testing...")
    with open(stream.sink, "r") as f:
        data = [json.loads(line) for line in f.readlines()]
    batch_seconds = [row["message"]["points"]["double"][0]["timestamp"]["seconds"] for row in data]
    assert sorted(batch_seconds) == list(range(len(data)))
    assert SECONDS == len(batch_seconds)  # ensure we didn't drop any records from the sink
    print("Success!")
