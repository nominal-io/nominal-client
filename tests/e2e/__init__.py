import random
from datetime import datetime, timedelta


def _create_random_start_end():
    random_epoch_start = int(datetime(2020, 1, 1).timestamp())
    random_epoch_end = int(datetime(2025, 1, 1).timestamp())
    epoch_start = random.randint(random_epoch_start, random_epoch_end)
    start = datetime.fromtimestamp(epoch_start)
    end = start + timedelta(hours=1)
    return start, end
