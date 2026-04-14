"""Unit tests for ParquetCompactor."""

from __future__ import annotations

import pytest

from ad_data_pipeline.transformations.compactor import (
    CompactionJob,
    CompactionResult,
    ParquetCompactor,
)


@pytest.fixture
def compactor() -> ParquetCompactor:
    return ParquetCompactor(
        source_bucket="test-processed",
        min_file_size_mb=64,
        target_file_size_mb=256,
        max_file_size_mb=512,
    )


class TestParquetCompactor:
    def test_plan_compaction_jobs_groups_small_files(
        self, compactor: ParquetCompactor
    ) -> None:
        # 3 files all < 64 MB in same partition
        partition_file_map = {
            "events/year=2024/month=01/day=15/hour=00/": [
                ("events/.../part-0001.parquet", 10 * 1024 * 1024),
                ("events/.../part-0002.parquet", 15 * 1024 * 1024),
                ("events/.../part-0003.parquet", 20 * 1024 * 1024),
            ]
        }
        jobs = compactor.plan_compaction_jobs(partition_file_map)
        assert len(jobs) == 1
        assert jobs[0].input_file_count == 3

    def test_plan_compaction_jobs_skips_single_small_file(
        self, compactor: ParquetCompactor
    ) -> None:
        partition_file_map = {
            "events/year=2024/month=01/day=15/": [
                ("events/.../part-0001.parquet", 10 * 1024 * 1024),
            ]
        }
        jobs = compactor.plan_compaction_jobs(partition_file_map)
        assert len(jobs) == 0

    def test_plan_compaction_jobs_skips_large_files(
        self, compactor: ParquetCompactor
    ) -> None:
        # One large file, one small - not enough small files to compact
        partition_file_map = {
            "events/year=2024/month=01/": [
                ("part-large.parquet", 200 * 1024 * 1024),  # 200 MB, above min
                ("part-small.parquet", 10 * 1024 * 1024),   # 10 MB, below min
            ]
        }
        jobs = compactor.plan_compaction_jobs(partition_file_map)
        # Only 1 small file -> no compaction job
        assert len(jobs) == 0

    def test_validate_compaction_result_success(
        self, compactor: ParquetCompactor
    ) -> None:
        job = CompactionJob(partition_prefix="events/")
        result = CompactionResult(
            job=job,
            success=True,
            input_row_count=1000,
            output_row_count=990,  # 1% dedup loss
            input_size_bytes=100 * 1024 * 1024,
            output_size_bytes=65 * 1024 * 1024,
            duplicates_removed=10,
        )
        is_valid, errors = compactor.validate_compaction_result(result)
        assert is_valid is True
        assert errors == []

    def test_validate_compaction_result_failed_job(
        self, compactor: ParquetCompactor
    ) -> None:
        job = CompactionJob(partition_prefix="events/")
        result = CompactionResult(
            job=job,
            success=False,
            error_message="S3 read error",
        )
        is_valid, errors = compactor.validate_compaction_result(result)
        assert is_valid is False
        assert len(errors) == 1

    def test_validate_compaction_result_zero_output_rows(
        self, compactor: ParquetCompactor
    ) -> None:
        job = CompactionJob(partition_prefix="events/")
        result = CompactionResult(
            job=job,
            success=True,
            input_row_count=1000,
            output_row_count=0,
            input_size_bytes=100 * 1024 * 1024,
            output_size_bytes=0,
        )
        is_valid, errors = compactor.validate_compaction_result(result)
        assert is_valid is False
        assert any("zero rows" in e for e in errors)

    def test_validate_compaction_result_excessive_dedup(
        self, compactor: ParquetCompactor
    ) -> None:
        job = CompactionJob(partition_prefix="events/")
        result = CompactionResult(
            job=job,
            success=True,
            input_row_count=1000,
            output_row_count=800,  # 20% loss - too high
            duplicates_removed=200,
            input_size_bytes=100 * 1024 * 1024,
            output_size_bytes=80 * 1024 * 1024,
        )
        is_valid, errors = compactor.validate_compaction_result(result)
        assert is_valid is False
        assert any("dedup" in e.lower() for e in errors)

    def test_estimate_savings_positive(self, compactor: ParquetCompactor) -> None:
        jobs = [
            CompactionJob(
                partition_prefix="events/",
                input_files=["a.parquet", "b.parquet"],
            )
        ]
        file_size_map = {
            "a.parquet": 30 * 1024 * 1024,
            "b.parquet": 20 * 1024 * 1024,
        }
        savings = compactor.estimate_savings(jobs, file_size_map)
        assert savings["total_input_bytes"] == 50 * 1024 * 1024
        assert savings["estimated_savings_pct"] > 0

    def test_compaction_result_dedup_ratio(self) -> None:
        job = CompactionJob(partition_prefix="events/")
        result = CompactionResult(
            job=job,
            success=True,
            input_row_count=1000,
            output_row_count=950,
            duplicates_removed=50,
            input_size_bytes=100 * 1024 * 1024,
            output_size_bytes=70 * 1024 * 1024,
        )
        assert result.dedup_ratio == pytest.approx(0.05)
        assert result.compression_ratio == pytest.approx(0.30)
