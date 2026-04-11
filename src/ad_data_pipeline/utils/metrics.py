"""StatsD/Datadog metrics helpers for DAG monitoring."""

import logging
import time
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, Generator, Optional, TypeVar

logger = logging.getLogger(__name__)

try:
    import statsd
    STATSD_AVAILABLE = True
except ImportError:
    STATSD_AVAILABLE = False
    statsd = None  # type: ignore[assignment]

F = TypeVar("F", bound=Callable[..., Any])


class MetricsClient:
    """
    Metrics client wrapping StatsD for DAG and task monitoring.

    Provides:
    - Counter increments for events and errors
    - Gauge measurements for current values
    - Timing measurements for task durations
    - Context managers for automatic timing
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8125,
        prefix: str = "ad_pipeline",
        enabled: bool = True,
        sample_rate: float = 1.0,
    ) -> None:
        self.host = host
        self.port = port
        self.prefix = prefix
        self.enabled = enabled and STATSD_AVAILABLE
        self.sample_rate = sample_rate
        self._client: Optional[Any] = None
        self._logger = logging.getLogger(self.__class__.__name__)

        if self.enabled:
            try:
                self._client = statsd.StatsClient(host=host, port=port, prefix=prefix)
                self._logger.info("StatsD client initialized: %s:%d/%s", host, port, prefix)
            except Exception as e:
                self._logger.warning("Failed to initialize StatsD client: %s", e)
                self.enabled = False

    def increment(
        self,
        metric: str,
        value: int = 1,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Increment a counter metric."""
        if not self.enabled or self._client is None:
            return
        try:
            self._client.incr(self._build_metric_name(metric, tags), value, self.sample_rate)
        except Exception as e:
            self._logger.debug("Metrics error (non-fatal): %s", e)

    def decrement(self, metric: str, value: int = 1) -> None:
        """Decrement a counter metric."""
        self.increment(metric, -value)

    def gauge(
        self,
        metric: str,
        value: float,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Set a gauge metric to a specific value."""
        if not self.enabled or self._client is None:
            return
        try:
            self._client.gauge(self._build_metric_name(metric, tags), value)
        except Exception as e:
            self._logger.debug("Metrics error (non-fatal): %s", e)

    def timing(
        self,
        metric: str,
        milliseconds: float,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Record a timing measurement in milliseconds."""
        if not self.enabled or self._client is None:
            return
        try:
            self._client.timing(self._build_metric_name(metric, tags), milliseconds)
        except Exception as e:
            self._logger.debug("Metrics error (non-fatal): %s", e)

    @contextmanager
    def timer(
        self,
        metric: str,
        tags: Optional[dict[str, str]] = None,
    ) -> Generator[None, None, None]:
        """Context manager that records elapsed time for a code block."""
        start = time.monotonic()
        try:
            yield
        finally:
            elapsed_ms = (time.monotonic() - start) * 1000
            self.timing(metric, elapsed_ms, tags)

    def record_dag_start(self, dag_id: str, run_id: str) -> None:
        """Record DAG run start."""
        self.increment("dag.started", tags={"dag_id": dag_id})
        self._logger.info("DAG started: %s (run_id=%s)", dag_id, run_id)

    def record_dag_success(self, dag_id: str, run_id: str, duration_seconds: float) -> None:
        """Record successful DAG completion."""
        self.increment("dag.success", tags={"dag_id": dag_id})
        self.timing("dag.duration", duration_seconds * 1000, tags={"dag_id": dag_id})
        self._logger.info("DAG succeeded: %s in %.2fs", dag_id, duration_seconds)

    def record_dag_failure(self, dag_id: str, run_id: str, error: str) -> None:
        """Record DAG failure with error context."""
        self.increment("dag.failed", tags={"dag_id": dag_id})
        self._logger.error("DAG failed: %s (run_id=%s): %s", dag_id, run_id, error)

    def record_task_metrics(
        self,
        dag_id: str,
        task_id: str,
        rows_processed: int,
        bytes_processed: int,
        duration_seconds: float,
    ) -> None:
        """Record per-task processing metrics."""
        tags = {"dag_id": dag_id, "task_id": task_id}
        self.gauge("task.rows_processed", rows_processed, tags=tags)
        self.gauge("task.bytes_processed", bytes_processed, tags=tags)
        self.timing("task.duration", duration_seconds * 1000, tags=tags)

    def _build_metric_name(
        self,
        metric: str,
        tags: Optional[dict[str, str]] = None,
    ) -> str:
        """Build metric name with optional tag suffix for StatsD."""
        if not tags:
            return metric
        # Encode tags as metric name suffix (StatsD convention)
        tag_str = ".".join(f"{k}_{v}" for k, v in sorted(tags.items()))
        return f"{metric}.{tag_str}"

    def timed(self, metric: str) -> Callable[[F], F]:
        """Decorator that automatically times a function call."""
        def decorator(func: F) -> F:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                with self.timer(metric):
                    return func(*args, **kwargs)
            return wrapper  # type: ignore[return-value]
        return decorator
