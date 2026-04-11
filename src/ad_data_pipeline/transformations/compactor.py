"""Parquet compactor: compact small files into larger ones and deduplicate events."""

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class CompactionJob:
    """Represents a single compaction job for a partition."""

    partition_prefix: str
    input_files: list[str] = field(default_factory=list)
    output_file: Optional[str] = None
    target_size_mb: int = 256
    deduplicate: bool = True
    dedup_key_columns: list[str] = field(default_factory=lambda: ["event_id"])


@dataclass
class CompactionResult:
    """Result of a compaction job."""

    job: CompactionJob
    success: bool = False
    input_row_count: int = 0
    output_row_count: int = 0
    duplicates_removed: int = 0
    input_size_bytes: int = 0
    output_size_bytes: int = 0
    processing_time_seconds: float = 0.0
    error_message: Optional[str] = None

    @property
    def dedup_ratio(self) -> float:
        """Fraction of rows removed as duplicates."""
        if self.input_row_count == 0:
            return 0.0
        return self.duplicates_removed / self.input_row_count

    @property
    def compression_ratio(self) -> float:
        """Size reduction ratio."""
        if self.input_size_bytes == 0:
            return 0.0
        return 1.0 - (self.output_size_bytes / self.input_size_bytes)


class ParquetCompactor:
    """
    Compacts small Parquet files in S3 partitions into larger optimally-sized files.

    Strategy:
    - Groups files within a partition that are below the merge threshold
    - Reads all small files, optionally deduplicates on event_id
    - Writes single output file at target_size_mb
    - Verifies row counts before deleting originals
    """

    def __init__(
        self,
        source_bucket: str,
        min_file_size_mb: int = 64,
        target_file_size_mb: int = 256,
        max_file_size_mb: int = 512,
        dedup_key_columns: Optional[list[str]] = None,
        parquet_compression: str = "snappy",
        parquet_row_group_size: int = 128 * 1024 * 1024,
    ) -> None:
        self.source_bucket = source_bucket
        self.min_file_size_mb = min_file_size_mb
        self.target_file_size_mb = target_file_size_mb
        self.max_file_size_mb = max_file_size_mb
        self.dedup_key_columns = dedup_key_columns or ["event_id"]
        self.parquet_compression = parquet_compression
        self.parquet_row_group_size = parquet_row_group_size
        self._logger = logging.getLogger(self.__class__.__name__)

    def plan_compaction_jobs(
        self,
        partition_file_map: dict[str, list[tuple[str, int]]],
    ) -> list[CompactionJob]:
        """
        Plan compaction jobs based on current partition state.

        Args:
            partition_file_map: Maps partition prefix -> list of (file_path, size_bytes)

        Returns:
            List of CompactionJob objects for partitions that need compaction
        """
        jobs: list[CompactionJob] = []
        min_bytes = self.min_file_size_mb * 1024 * 1024

        for prefix, files in partition_file_map.items():
            small_files = [(path, size) for path, size in files if size < min_bytes]

            if len(small_files) < 2:
                # Need at least 2 small files to compact
                continue

            total_size = sum(size for _, size in small_files)
            self._logger.info(
                "Planning compaction for %s: %d small files, %d MB total",
                prefix,
                len(small_files),
                total_size // (1024 * 1024),
            )

            job = CompactionJob(
                partition_prefix=prefix,
                input_files=[path for path, _ in small_files],
                target_size_mb=self.target_file_size_mb,
                deduplicate=True,
                dedup_key_columns=self.dedup_key_columns,
            )
            jobs.append(job)

        return jobs

    def validate_compaction_result(
        self,
        result: CompactionResult,
        allow_dedup_loss_ratio: float = 0.05,
    ) -> tuple[bool, list[str]]:
        """
        Validate that a compaction result is safe to proceed with.

        Args:
            result: The compaction result to validate
            allow_dedup_loss_ratio: Maximum acceptable fraction of rows lost to dedup

        Returns:
            Tuple of (is_valid, list_of_validation_errors)
        """
        errors: list[str] = []

        if not result.success:
            errors.append(f"Compaction job failed: {result.error_message}")
            return False, errors

        if result.output_row_count <= 0:
            errors.append("Output has zero rows - refusing to delete input files")

        if result.dedup_ratio > allow_dedup_loss_ratio:
            errors.append(
                f"Dedup ratio {result.dedup_ratio:.2%} exceeds allowed threshold "
                f"{allow_dedup_loss_ratio:.2%} - possible data issue"
            )

        # Output should not be larger than input (accounting for compression)
        if result.output_size_bytes > result.input_size_bytes * 1.1:
            errors.append(
                f"Output size {result.output_size_bytes} is unexpectedly larger than "
                f"input size {result.input_size_bytes}"
            )

        return len(errors) == 0, errors

    def estimate_savings(
        self,
        jobs: list[CompactionJob],
        file_size_map: dict[str, int],
    ) -> dict[str, float]:
        """
        Estimate storage and cost savings from running compaction jobs.

        Args:
            jobs: List of planned compaction jobs
            file_size_map: Maps file path -> size in bytes

        Returns:
            Dict with total_input_bytes, estimated_savings_bytes, estimated_savings_pct
        """
        total_input = sum(
            file_size_map.get(f, 0) for job in jobs for f in job.input_files
        )
        # Parquet with Snappy typically achieves 30-50% compression on ad event data
        estimated_output = total_input * 0.65
        savings = total_input - estimated_output

        return {
            "total_input_bytes": float(total_input),
            "estimated_output_bytes": estimated_output,
            "estimated_savings_bytes": savings,
            "estimated_savings_pct": (savings / total_input * 100) if total_input > 0 else 0.0,
            "jobs_planned": float(len(jobs)),
        }
