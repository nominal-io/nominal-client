import random
import requests
import sys
import time
from typing import Callable
from threading import Thread, Event
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4
import json


## User Code
seconds = 0
points_per_batch = 10

def read_source() -> dict:
    global seconds
    points = [{'timestamp': {'seconds': seconds, 'nanos': i}, 'value': random.random()} for i in range(points_per_batch)]
    batch = {'channel': 'streaming-test-channel-0', 'tags': {}, 'points': {'type': 'double', 'double': points}}
    seconds += 1
    return batch
## End user code


## Nominal client
class NominalWriteStream:
    def __init__(self, source: Callable[[], dict], data_source_id: str, push_freq_sec: float | int) -> None:
        """Create the write stream, save the stop event."""
        self.data_source_id = data_source_id
        self.push_freq_sec = push_freq_sec
        self.source = source
        self.sink = f"sink_{uuid4()}.jsonl"
        self.stop_event = Event()
        self.pool = ThreadPoolExecutor(max_workers=10)
        self.thread = Thread(target=self.worker, args=(self.stop_event,))
        self.thread.start()

    def _write_sink(self, write_req_data: dict) -> None:
        """Threaded entrypoint to write to the sync in the pool."""
        time.sleep(0.5) # simulate some network request lag
        with open(self.sink, "a") as sink:
            json.dump(write_req_data, sink)
            sink.write('\n')

    def worker(self, stop_event: Event) -> None:
        """Worker process."""
        while not stop_event.is_set():
            source_data = self.source()
            write_req_data = {"batches": [source_data], "dataSourceRid": self.data_source_id}
            self.pool.submit(self._write_sink, write_req_data)
            time.sleep(self.push_freq_sec)

    def stop(self, cancel_running: bool = False) -> None:
        """Gracefully kill the thread.
        
        Args:
            cancel_running (bool): If True, cancel the running threads writing to the data source.
        """
        self.stop_event.set()
        self.pool.shutdown(cancel_futures=cancel_running)
        self.thread.join()



def write_stream(source: Callable[[], dict], data_source_id: str, push_freq_sec: float | int = 1) -> NominalWriteStream:
    """Start a write stream based on a callable source.
    
    Args:
        source (Callable[[], dict]): A callable function that returns a dictionary of data to write to the source.
            The callable must not take any parameters. If it does, use a `functools.partial` to pre-fill the params
        data_source_id (str): The source in nominal to write to.
        push_freq_sec (int): The frequency with which to read from the source and write to the sink. Default 1.
    Returns:
        NominalWriteStream: The write task running the process. To end the stream, call stream.stop()
    """
    stream = NominalWriteStream(source, data_source_id, push_freq_sec)
    print(f"Pushing to sink {stream.sink}")
    return stream


if __name__ == "__main__":
    # Read at a higher frequency than the sink can handle. We'll ensure that no messages are dropped
    stream = write_stream(read_source, "data-id1", push_freq_sec=0.1)
    time.sleep(2)
    stream.stop()
    with open(stream.sink, "r") as f:
        data = [json.loads(line) for line in f.readlines()]
    seconds = [row["batches"][0]["points"]["double"][0]["timestamp"]["seconds"] for row in data]
    assert seconds == list(range(len(data)))
    print("Success!")
    