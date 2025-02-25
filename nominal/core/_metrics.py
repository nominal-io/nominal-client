from __future__ import annotations

import abc
import contextlib
import functools
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Generator, Optional, Protocol, Sequence, TypeVar, TYPE_CHECKING

from typing_extensions import Self

from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

# Use TYPE_CHECKING to avoid circular imports
if TYPE_CHECKING:
    from nominal.core.connection import StreamingConnection
    from nominal.core.stream import WriteStream


class MetricsManager(Protocol):
    """Protocol defining the interface for metrics tracking."""
    
    def add_metric(
        self, 
        channel_name: str, 
        value: float, 
        timestamp: Optional[datetime | IntegralNanosecondsUTC] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Record a metric value.
        
        Args:
            channel_name: The name of the metric channel
            value: The metric value to record
            timestamp: Optional timestamp for the metric (defaults to current time)
            tags: Optional tags to associate with this metric
        """
        ...
    
    def add_timing_metric(
        self, 
        channel_name: str, 
        start_time: IntegralNanosecondsUTC,
        end_time: Optional[IntegralNanosecondsUTC] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> float:
        """
        Record a timing metric (duration between start and end).
        
        Args:
            channel_name: The name of the metric channel
            start_time: The start time in nanoseconds
            end_time: Optional end time (defaults to current time)
            tags: Optional tags to associate with this metric
            
        Returns:
            The duration in seconds
        """
        ...
    
    def close(self) -> None:
        """Close the metrics manager and flush any pending metrics."""
        ...


class NullMetricsManager:
    """A metrics manager that discards all metrics (no-op implementation)."""
    
    def __hash__(self) -> int:
        return hash(id(self))
    
    def add_metric(
        self, 
        channel_name: str, 
        value: float, 
        timestamp: Optional[datetime | IntegralNanosecondsUTC] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> None:
        """No-op implementation that discards metrics."""
        pass
    
    def add_timing_metric(
        self, 
        channel_name: str, 
        start_time: IntegralNanosecondsUTC,
        end_time: Optional[IntegralNanosecondsUTC] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> float:
        """Calculate duration but don't record the metric."""
        if end_time is None:
            end_time = time.time_ns()
        return (end_time - start_time) / 1e9
    
    def close(self) -> None:
        """No-op implementation."""
        pass


@dataclass(frozen=True)
class StreamingMetricsManager:
    """A metrics manager that writes metrics to a Nominal streaming connection."""
    
    _connection: 'StreamingConnection'
    _stream: 'WriteStream'
    _prefix: str = "__nominal.metric."
    # These need to be in a separate mutable container since the class is frozen
    _state: Dict[str, Any] = field(default_factory=lambda: {"closed": False}, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, hash=False, compare=False)
    
    def __hash__(self) -> int:
        return hash((id(self._connection), self._prefix))
    
    @property
    def _closed(self) -> bool:
        return self._state.get("closed", False)
    
    @_closed.setter
    def _closed(self, value: bool) -> None:
        self._state["closed"] = value
    
    @classmethod
    def create(
        cls, 
        connection: 'StreamingConnection', 
        prefix: str = "__nominal.metric.",
        batch_size: int = 1000,
        max_wait_seconds: int = 1,
        use_protos: bool = True
    ) -> Self:
        """
        Create a new StreamingMetricsManager.
        
        Args:
            connection: The streaming connection to write metrics to
            prefix: Prefix to add to all metric channel names
            batch_size: Batch size for the write stream
            max_wait_seconds: Maximum wait time for batching
            use_protos: Whether to use protobuf serialization
        """
        from datetime import timedelta
        
        stream = connection.get_write_stream(
            batch_size=batch_size,
            max_wait=timedelta(seconds=max_wait_seconds),
            data_format="protobuf" if use_protos else "json"
        )
        
        return cls(
            _connection=connection,
            _stream=stream,
            _prefix=prefix
        )
    
    def add_metric(
        self, 
        channel_name: str, 
        value: float, 
        timestamp: Optional[datetime | IntegralNanosecondsUTC] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> None:
        """Add a metric to the stream."""
        with self._lock:
            if self._closed:
                return
                
            if timestamp is None:
                timestamp = time.time_ns()
                
            full_channel_name = f"{self._prefix}{channel_name}"
            self._stream.enqueue(full_channel_name, timestamp, value, tags)
    
    def add_timing_metric(
        self, 
        channel_name: str, 
        start_time: IntegralNanosecondsUTC,
        end_time: Optional[IntegralNanosecondsUTC] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> float:
        """Add a timing metric to the stream."""
        if end_time is None:
            end_time = time.time_ns()
            
        duration_seconds = (end_time - start_time) / 1e9
        
        self.add_metric(
            channel_name=channel_name,
            value=duration_seconds,
            timestamp=end_time,
            tags=tags
        )
        
        return duration_seconds
    
    def close(self) -> None:
        """Close the metrics manager and the underlying stream."""
        with self._lock:
            if not self._closed:
                self._stream.close()
                self._state["closed"] = True


T = TypeVar('T')

def timed(
    metric_name: str, 
    metrics_manager: Optional[MetricsManager] = None,
    tags: Optional[Dict[str, str]] = None
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to time a function and record the duration as a metric.
    
    Args:
        metric_name: The name of the metric to record
        metrics_manager: The metrics manager to use (if None, uses the one from the client)
        tags: Optional tags to associate with this metric
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Try to get metrics manager from first arg if it's a NominalClient
            manager = metrics_manager
            if manager is None and args and hasattr(args[0], '_clients') and hasattr(args[0]._clients, 'metrics_manager'):
                manager = args[0]._clients.metrics_manager
                
            start_time = time.time_ns()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                if manager is not None:
                    end_time = time.time_ns()
                    manager.add_timing_metric(
                        channel_name=f"function.{func.__module__}.{func.__name__}.duration",
                        start_time=start_time,
                        end_time=end_time,
                        tags=tags
                    )
        return wrapper
    return decorator


def count_metric(
    metric_name: str,
    metrics_manager: Optional[MetricsManager] = None,
    tags: Optional[Dict[str, str]] = None
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to count function calls.
    
    Args:
        metric_name: The name of the metric to record
        metrics_manager: The metrics manager to use (if None, uses the one from the client)
        tags: Optional tags to associate with this metric
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Try to get metrics manager from first arg if it's a NominalClient
            manager = metrics_manager
            if manager is None and args and hasattr(args[0], '_clients') and hasattr(args[0]._clients, 'metrics_manager'):
                manager = args[0]._clients.metrics_manager
                
            try:
                result = func(*args, **kwargs)
                status = "success"
                return result
            except Exception as e:
                status = f"error.{e.__class__.__name__}"
                raise
            finally:
                if manager is not None:
                    current_tags = dict(tags or {})
                    current_tags["status"] = status
                    manager.add_metric(
                        channel_name=f"function.{func.__module__}.{func.__name__}.count",
                        value=1.0,
                        tags=current_tags
                    )
        return wrapper
    return decorator


@contextlib.contextmanager
def timed_block(
    metric_name: str,
    metrics_manager: MetricsManager,
    tags: Optional[Dict[str, str]] = None
) -> Generator[None, None, None]:
    """
    Context manager for timing code blocks.
    
    Args:
        metric_name: The name of the metric to record
        metrics_manager: The metrics manager to use
        tags: Optional tags to associate with this metric
    """
    start_time = time.time_ns()
    try:
        yield
    finally:
        end_time = time.time_ns()
        metrics_manager.add_timing_metric(
            channel_name=metric_name,
            start_time=start_time,
            end_time=end_time,
            tags=tags
        )
