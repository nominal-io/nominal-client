from datetime import datetime, timedelta

from nominal.core import NominalClient

client = NominalClient.from_profile("default")

results = client.search_events(
    search_text="engine start",
    after=datetime.now() - timedelta(days=1),
    before=datetime.now(),
    assets=[asset_rid],
    labels=["reviewed", "critical"],
    properties={"test_phase": "burn"},
    created_by="user:abc123",
)
