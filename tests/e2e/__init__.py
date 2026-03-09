import random
from datetime import datetime, timedelta

POLL_INTERVAL = timedelta(seconds=0.1)
"""Default polling interval for `poll_until_ingestion_completed` calls in e2e tests."""


def _create_random_start_end() -> tuple[datetime, datetime]:
    random_epoch_start = int(datetime(2020, 1, 1).timestamp())
    random_epoch_end = int(datetime(2025, 1, 1).timestamp())
    epoch_start = random.randint(random_epoch_start, random_epoch_end)
    start = datetime.fromtimestamp(epoch_start)
    end = start + timedelta(hours=1)
    return start, end
