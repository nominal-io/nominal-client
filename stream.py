import random
import requests
import sys
import time
from typing import Callable
from threading import Thread, Event
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
        self.thread = Thread(target=self.worker, args=(self.stop_event,))
        self.thread.start()


    def worker(self, stop_event: Event) -> None:
        """Worker process."""
        while not stop_event.is_set():
            source_data = self.source()
            write_req_data = {"batches": [source_data], "dataSourceRid": self.data_source_id}
            with open(self.sink, "a") as sink:
                json.dump(write_req_data, sink)
                sink.write('\n')
            time.sleep(self.push_freq_sec)

    def stop(self) -> None:
        """Gracefully kill the thread."""
        self.stop_event.set()
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







# TOKEN='<REDACTED>'
# BASE_URL='https://api-staging.gov.nominal.io/api'

# headers = {'Authorization': 'Bearer %s' % (TOKEN), 'Content-type': 'application/json'}

# data_source_rid='ri.nominal.gov-staging.datasource.17591376-973d-442b-9a1d-11a65cf91f7d'

# write_url = '%s/storage/writer/v1' % (BASE_URL)



# while True:
#     points = [{'timestamp': {'seconds': seconds, 'nanos': i}, 'value': random.random()} for i in range(points_per_batch)]

#     batch = {'channel': 'streaming-test-channel-0', 'tags': {}, 'points': {'type': 'double', 'double': points}}
#     print(batch)
#     write_req_data = {"batches": [batch], "dataSourceRid": data_source_rid}

#     write_response = requests.post(write_url, json=write_req_data, headers=headers)

#     print(write_response.status_code)
#     seconds += 1

#     time.sleep(1)