"""Unit tests for S3DataPartitioner."""

from __future__ import annotations

from datetime import datetime

import pytest

from ad_data_pipeline.transformations.partitioner import (
    PartitionSpec,
    S3DataPartitioner,
)


@pytest.fixture
def partitioner() -> S3DataPartitioner:
    return S3DataPartitioner(
        source_bucket="test-raw",
        destination_bucket="test-processed",
        max_partition_size_mb=512,
        min_partition_size_mb=64,
        target_partition_size_mb=256,
    )


class TestPartitionSpec:
    def test_s3_prefix_with_hour_and_event_type(self) -> None:
        spec = PartitionSpec(year=2024, month=1, day=15, hour=8, event_type="impression")
        assert spec.s3_prefix == "year=2024/month=01/day=15/hour=08/event_type=impression"

    def test_s3_prefix_without_optional_fields(self) -> None:
        spec = PartitionSpec(year=2024, month=12, day=31)
        assert spec.s3_prefix == "year=2024/month=12/day=31"

    def test_s3_prefix_zero_pads_month_and_day(self) -> None:
        spec = PartitionSpec(year=2024, month=3, day=5, hour=2)
        assert "month=03" in spec.s3_prefix
        assert "day=05" in spec.s3_prefix
        assert "hour=02" in spec.s3_prefix


class TestS3DataPartitioner:
    def test_compute_target_partitions_returns_correct_count(
        self, partitioner: S3DataPartitioner
    ) -> None:
        logical_date = datetime(2024, 1, 15)
        event_types = ["impression", "click"]
        specs = partitioner.compute_target_partitions(logical_date, event_types)
        # 24 hours * 2 event types = 48 specs
        assert len(specs) == 48

    def test_compute_target_partitions_default_event_types(
        self, partitioner: S3DataPartitioner
    ) -> None:
        logical_date = datetime(2024, 1, 15)
        specs = partitioner.compute_target_partitions(logical_date)
        # 24 hours * 5 default event types = 120
        assert len(specs) == 120

    def test_compute_target_partitions_correct_date(
        self, partitioner: S3DataPartitioner
    ) -> None:
        logical_date = datetime(2024, 6, 20)
        specs = partitioner.compute_target_partitions(logical_date, ["impression"])
        assert all(s.year == 2024 for s in specs)
        assert all(s.month == 6 for s in specs)
        assert all(s.day == 20 for s in specs)

    def test_needs_merging_below_threshold(self, partitioner: S3DataPartitioner) -> None:
        assert partitioner.needs_merging(30 * 1024 * 1024) is True  # 30 MB

    def test_needs_merging_above_threshold(self, partitioner: S3DataPartitioner) -> None:
        assert partitioner.needs_merging(100 * 1024 * 1024) is False  # 100 MB

    def test_needs_splitting_above_threshold(self, partitioner: S3DataPartitioner) -> None:
        assert partitioner.needs_splitting(600 * 1024 * 1024) is True  # 600 MB

    def test_needs_splitting_below_threshold(self, partitioner: S3DataPartitioner) -> None:
        assert partitioner.needs_splitting(400 * 1024 * 1024) is False  # 400 MB

    def test_compute_split_count_round_trip(self, partitioner: S3DataPartitioner) -> None:
        # 512 MB should split into 2 files of 256 MB each
        count = partitioner.compute_split_count(512 * 1024 * 1024)
        assert count == 2

    def test_compute_split_count_minimum_one(self, partitioner: S3DataPartitioner) -> None:
        count = partitioner.compute_split_count(10 * 1024 * 1024)
        assert count >= 1

    def test_build_partition_path(self, partitioner: S3DataPartitioner) -> None:
        spec = PartitionSpec(year=2024, month=1, day=15, hour=8, event_type="click")
        path = partitioner.build_partition_path("my-bucket", spec, prefix="events")
        assert path.startswith("s3://my-bucket/events/")
        assert "year=2024" in path

    def test_validate_partition_schema_no_missing(
        self, partitioner: S3DataPartitioner
    ) -> None:
        missing = partitioner.validate_partition_schema(
            expected_columns=["event_id", "timestamp", "user_id"],
            actual_columns=["event_id", "timestamp", "user_id", "extra_col"],
        )
        assert missing == []

    def test_validate_partition_schema_with_missing(
        self, partitioner: S3DataPartitioner
    ) -> None:
        missing = partitioner.validate_partition_schema(
            expected_columns=["event_id", "timestamp", "user_id"],
            actual_columns=["event_id", "timestamp"],
        )
        assert "user_id" in missing

    def test_estimate_repartition_cost_returns_dict(
        self, partitioner: S3DataPartitioner
    ) -> None:
        result = partitioner.estimate_repartition_cost(
            file_count=100,
            total_size_bytes=1024 * 1024 * 1024,  # 1 GB
        )
        assert "estimated_seconds" in result
        assert "estimated_cost_usd" in result
        assert result["estimated_cost_usd"] >= 0
        assert result["estimated_seconds"] > 0
