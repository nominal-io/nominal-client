dataset = client.get_dataset(
    "ri.catalog.cerulean-staging.dataset.d6c6e12c-05b3-4bb0-9f45-97609b7c9da2"
)

for video_file in dataset.list_video_files():
    print(video_file.name, video_file.media_duration_seconds, "seconds")
