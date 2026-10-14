from datetime import datetime, timedelta

from nominal.core import EventType, NominalClient

client = NominalClient.from_profile("default")
asset_rid = "<ASSET_RID>"
client.create_event(
    name="Event Name",
    type=EventType.INFO,
    start=datetime.now(),
    duration=timedelta(seconds=10),
    assets=[asset_rid],
    properties={"key": "value"},
    labels=["label1", "label2"],
)
