video_file.poll_until_ingestion_completed()

bounds = video_file.bounds
if bounds is None:
    raise ValueError("Ingested video has no timestamp bounds")

start_time = bounds.start
end_time = bounds.end
