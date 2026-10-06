from nominal.core import EventType
from nominal.core import NominalClient
from datetime import datetime, timedelta

client = NominalClient.from_profile("demo")
asset_rid = "ri.scout.cerulean-staging.asset.bce88be4-150a-4721-b69b-7247e1febce9"
client.create_event(
    name="Event Name",
    type=EventType.INFO,
    start=datetime.now(),
    duration=timedelta(seconds=10),
    assets=[asset_rid],
    properties={"key": "value"},
    labels=["label1", "label2"],
)
