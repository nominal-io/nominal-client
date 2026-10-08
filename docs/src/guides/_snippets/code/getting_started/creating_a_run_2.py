import datetime

run = client.create_run(
    ...,
    # Initialize start time to now, as it will always be greater than data
    # we are ingesting from a flight test that happened in the past
    start=datetime.datetime.now(),
)

# ... normalize data from flight test ...

# derived directly from the data uploaded to nominal---
# can look at the start and end of the provided timestamp_column
data_start_time = datetime.datetime(...)
data_end_time = datetime.datetime(...)

# ... upload data to nominal ...

# Update bounds of run based on the earlier / latest of the start and end
# times respectively from the existing run, and
run.update(
    start=min(run.start, data_start_time),
    end=data_end_time if run.end is None else max(run.end, data_end_time),
)
