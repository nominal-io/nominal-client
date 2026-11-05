---
myst:
  html_meta:
    description: "Upload a video topic from an MCAP file as a channel on a dataset."
---

# MCAP video ingest

{.lead}
Upload a video topic from an [MCAP](https://mcap.dev/) file with {py:meth}`~nominal.core.Dataset.add_mcap_video`.
The topic must use the `foxglove.CompressedVideo` schema with H.264 frames. Frame times come from the file.

The examples also need `pip install mcap huggingface_hub`.

```{include} /guides/_snippets/connect-the-client.md
```

## Find the video topics

This example downloads a sample flight simulator recording and lists its video topics with the `mcap` library:

```python
from huggingface_hub import hf_hub_download
from mcap.reader import make_reader

path = hf_hub_download(repo_id="nominal-io/xplane", filename="xplane.mcap", repo_type="dataset")

with open(path, "rb") as f:
    summary = make_reader(f).get_summary()
    video_topics = [
        channel.topic
        for channel in summary.channels.values()
        if summary.schemas[channel.schema_id].name == "foxglove.CompressedVideo"
    ]
print(video_topics)
```

## Upload a topic

```python
dataset = client.create_dataset("X-Plane simulation")
file = dataset.add_mcap_video(path, channel="cockpit_cam", topic="video")
file.poll_until_ingestion_completed()
```

To review it, attach the dataset to a run as in [Video ingest](/guides/video/video-ingest.md#review-it-in-a-run).
For the file's protobuf-encoded topics, use {py:meth}`~nominal.core.Dataset.add_mcap`.
