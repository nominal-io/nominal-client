import pathlib
from datetime import datetime, timezone

from nominal_streaming import NominalDatasetStream

# Streaming to a file needs no API credentials. The same enqueue calls you
# would use against Nominal write an Avro stream to disk instead.
with NominalDatasetStream().to_file(pathlib.Path("flight_backup.avro")) as stream:
    stream.enqueue(
        "altitude", datetime.now(timezone.utc), 1250.5, tags={"vehicle_id": "sn-001"}
    )

    # Whole batches at once, with timestamps in epoch nanoseconds
    stream.enqueue_batch(
        "airspeed",
        [1_757_500_000_000_000_000, 1_757_500_100_000_000_000],
        [42.0, 42.5],
        tags={"vehicle_id": "sn-001"},
    )

    # Several channels sharing one timestamp
    stream.enqueue_from_dict(
        1_757_500_200_000_000_000,
        {"altitude": 1252.0, "fault_code": "E07"},
        tags={"vehicle_id": "sn-001"},
    )
