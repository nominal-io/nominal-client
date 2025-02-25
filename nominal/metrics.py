from __future__ import annotations

from datetime import timedelta
from typing import Optional, Dict, Callable, TypeVar, Any

from nominal.core._metrics import (
    MetricsManager, 
    NullMetricsManager, 
    StreamingMetricsManager,
    timed,
    count_metric,
    timed_block
)

# Re-export the utility functions
__all__ = [
    "create_null_metrics_manager",
    "create_streaming_metrics_manager",
    "timed",
    "count_metric",
    "timed_block",
]


def create_null_metrics_manager() -> MetricsManager:
    """Create a metrics manager that discards all metrics."""
    return NullMetricsManager()


def create_streaming_metrics_manager(
    connection: 'StreamingConnection',
    prefix: str = "__nominal.metric.",
    batch_size: int = 1000,
    max_wait_seconds: int = 1,
    use_protos: bool = True
) -> MetricsManager:
    """
    Create a metrics manager that writes to a Nominal streaming connection.
    
    Args:
        connection: The streaming connection to write metrics to
        prefix: Prefix to add to all metric channel names
        batch_size: Batch size for the write stream
        max_wait_seconds: Maximum wait time for batching
        use_protos: Whether to use protobuf serialization
    """
    # Import here to avoid circular imports
    from nominal.core.connection import StreamingConnection
    
    return StreamingMetricsManager.create(
        connection=connection,
        prefix=prefix,
        batch_size=batch_size,
        max_wait_seconds=max_wait_seconds,
        use_protos=use_protos
    ) 
