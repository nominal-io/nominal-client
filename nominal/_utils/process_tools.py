import concurrent.futures
import contextlib
from typing import Any, Generator, Literal

from typing_extensions import TypeAlias

PoolType: TypeAlias = Literal["thread", "process"]

DEFAULT_POOL_TYPE: PoolType = "thread"


@contextlib.contextmanager
def BackgroundPool(
    max_workers: int, pool_type: PoolType = DEFAULT_POOL_TYPE, **kwargs: Any
) -> Generator[concurrent.futures.Executor, None, None]:
    pool_classes = {
        "process": concurrent.futures.ProcessPoolExecutor,
        "thread": concurrent.futures.ThreadPoolExecutor,
    }
    with pool_classes[pool_type](max_workers=max_workers, **kwargs) as pool:
        yield pool
