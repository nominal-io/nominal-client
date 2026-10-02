---
myst:
  html_meta:
    description: "Upload an MCAP video topic to the Nominal platform"
---

# MCAP video ingest in Python

{.lead}
Upload an MCAP video topic to the Nominal platform

:::{note}

Our MCAP video ingestion supports channels that use the `foxglove.CompressedVideo` schema for H.264-encoded frames.
:::

This guide will walk you through the process of downloading a sample MCAP file, inspecting it to find video topics, and
ingesting the video into Nominal.

## Prerequisites

Make sure you have the following Python packages installed:
 - huggingface_hub
 - mcap
 - Nominal

You can install them all using:
```shell
pip3 install huggingface_hub mcap nominal
```

## Step 1: Download the Sample MCAP File

First, download a sample MCAP file using the huggingface_hub library.
```python
from huggingface_hub import hf_hub_download

video_path = hf_hub_download(
    repo_id="nominal-io/xplane",
    filename="xplane.mcap",
    repo_type='dataset'
)

print(f"File saved to: {video_path}")
```

## Step 2: Inspect the MCAP file to find video topics

Next, use the mcap library to inspect the MCAP file and identify available video topics.
```python
from mcap.reader import make_reader

with open(video_path, "rb") as f:
    reader = make_reader(f)

    schemas = reader.get_summary().schemas
    channels = reader.get_summary().channels

    def schema_name(channel):
        schema = schemas.get(channel.schema_id)
        if not schema:
            return None
        return schema.name

    video_topics = [
        channel.topic for channel in channels.values()
        if schema_name(channel) == "foxglove.CompressedVideo"
    ]

    print("Video topics: ", video_topics)
```
This script will output the video topics available in the MCAP file, which you'll use in the next step.

## Step 3: Connect to Nominal

Before ingesting the video, ensure you're connected to Nominal.

```{include} /guides/_snippets/python/auth.md
```

## Step 4: Ingest the Video

Finally, upload the video to Nominal using a topic from the list.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

# Video is stored as a channel on a dataset; per-frame timestamps come from the MCAP topic
dataset = client.create_dataset(name="X-Plane Simulation")
video_file = dataset.add_mcap_video(
    video_path,
    channel="cockpit_cam",
    topic="video",
)
print(video_file)
```

:::{note}

Standalone `Video` objects (`client.create_video()`, `Video.add_mcap()`) are deprecated in favor of
video channels on a dataset. Using them emits a `LegacyVideoDeprecationWarning`.
:::
