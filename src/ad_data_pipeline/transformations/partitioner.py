"""S3 data partitioner: repartition by date/hour, merge small files, split large files."""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class PartitionSpec:
    """Specification for a single S3 partition."""

    year: int
    month: int
    day: int
    hour: Optional[int] = None
    event_type: Optional[str] = None

    @property
    def s3_prefix(self) -> str:
        """Return S3 prefix path for this partition."""
        parts = [f"year={self.year}", f"month={self.month:02d}", f"day={self.day:02d}"]
        if self.hour is not None:
            parts.append(f"hour={self.hour:02d}")
        if self.event_type:
            parts.append(f"event_type={self.event_type}")
        return "/".join(parts)


@dataclass
class PartitionStats:
    """Statistics for a partition after processing."""

    partition: PartitionSpec
    input_file_count: int = 0
    output_file_count: int = 0
    input_size_bytes: int = 0
    output_size_bytes: int = 0
    row_count: int = 0
    processing_time_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def compression_ratio(self) -> float:
        if self.input_size_bytes == 0:
            return 0.0
        return self.output_size_bytes / self.input_size_bytes


class S3DataPartitioner:
    """
    Manages S3 partition creation, validation, and repartitioning of ad event data.

    Handles:
    - Repartitioning raw Parquet data by date/hour columns
    - Merging small partition files below min_size_mb threshold
    - Splitting large partition files exceeding max_size_mb threshold
    - Validating partition completeness and data integrity
    """

    def __init__(
        self,
        source_bucket: str,
        destination_bucket: str,
        max_partition_size_mb: int = 512,
        min_partition_size_mb: int = 64,
        target_partition_size_mb: int = 256,
        partition_columns: Optional[list[str]] = None,
    ) -> None:
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.max_partition_size_mb = max_partition_size_mb
        self.min_partition_size_mb = min_partition_size_mb
        self.target_partition_size_mb = target_partition_size_mb
        self.partition_columns = partition_columns or ["year", "month", "day", "hour"]
        self._logger = logging.getLogger(self.__class__.__name__)

    def compute_target_partitions(
        self,
        logical_date: datetime,
        event_types: Optional[list[str]] = None,
    ) -> list[PartitionSpec]:
        """
        Compute the list of partition specs to process for a given logical date.

        Args:
            logical_date: The DAG execution date
            event_types: List of event types to partition; None means all types

        Returns:
            List of PartitionSpec objects representing target partitions
        """
        specs: list[PartitionSpec] = []
        base_types = event_types or ["impression", "click", "conversion", "bid_request", "bid_response"]

        for hour in range(24):
            for event_type in base_types:
                specs.append(
                    PartitionSpec(
                        year=logical_date.year,
                        month=logical_date.month,
                        day=logical_date.day,
                        hour=hour,
                        event_type=event_type,
                    )
                )
        return specs

    def needs_merging(self, size_bytes: int) -> bool:
        """Return True if partition is below minimum size and should be merged."""
        return size_bytes < self.min_partition_size_mb * 1024 * 1024

    def needs_splitting(self, size_bytes: int) -> bool:
        """Return True if partition exceeds maximum size and should be split."""
        return size_bytes > self.max_partition_size_mb * 1024 * 1024

    def compute_split_count(self, size_bytes: int) -> int:
        """Compute number of output files needed to reach target partition size."""
        target_bytes = self.target_partition_size_mb * 1024 * 1024
        return max(1, round(size_bytes / target_bytes))

    def build_partition_path(
        self,
        bucket: str,
        spec: PartitionSpec,
        prefix: str = "events",
    ) -> str:
        """Build full S3 path for a partition."""
        return f"s3://{bucket}/{prefix}/{spec.s3_prefix}/"

    def validate_partition_schema(
        self,
        expected_columns: list[str],
        actual_columns: list[str],
    ) -> list[str]:
        """
        Validate that actual partition columns match expected schema.

        Returns:
            List of missing column names (empty if valid)
        """
        expected_set = set(expected_columns)
        actual_set = set(actual_columns)
        missing = expected_set - actual_set
        return sorted(missing)

    def estimate_repartition_cost(
        self,
        file_count: int,
        total_size_bytes: int,
        bytes_per_second_read: float = 100 * 1024 * 1024,
        bytes_per_second_write: float = 50 * 1024 * 1024,
    ) -> dict[str, float]:
        """
        Estimate time and cost for repartitioning operation.

        Args:
            file_count: Number of input files
            total_size_bytes: Total size of input data
            bytes_per_second_read: S3 read throughput estimate
            bytes_per_second_write: S3 write throughput estimate

        Returns:
            Dict with estimated_seconds and estimated_cost_usd keys
        """
        read_seconds = total_size_bytes / bytes_per_second_read
        write_seconds = total_size_bytes / bytes_per_second_write
        total_seconds = read_seconds + write_seconds

        # S3 request pricing: $0.0004 per 1000 GET, $0.005 per 1000 PUT
        get_cost = (file_count / 1000) * 0.0004
        target_files = self.compute_split_count(total_size_bytes)
        put_cost = (target_files / 1000) * 0.005
        data_transfer_cost = (total_size_bytes / (1024**3)) * 0.09  # $0.09/GB

        return {
            "estimated_seconds": total_seconds,
            "estimated_cost_usd": get_cost + put_cost + data_transfer_cost,
            "input_files": float(file_count),
            "estimated_output_files": float(target_files),
        }
