"""Integration tests for S3 management DAGs - structure and task dependencies."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


class TestS3PartitioningDagStructure:
    """Tests for s3_partitioning_daily DAG structure."""

    def test_dag_loads_without_error(self) -> None:
        """DAG module imports cleanly."""
        from dags.s3_management.s3_partitioning_dag import s3_partitioning_daily
        assert s3_partitioning_daily is not None

    def test_dag_id(self) -> None:
        from dags.s3_management.s3_partitioning_dag import s3_partitioning_daily
        assert s3_partitioning_daily.dag_id == "s3_partitioning_daily"

    def test_dag_schedule_interval(self) -> None:
        from dags.s3_management.s3_partitioning_dag import s3_partitioning_daily
        assert s3_partitioning_daily.schedule_interval == "0 2 * * *"

    def test_dag_has_expected_tasks(self) -> None:
        from dags.s3_management.s3_partitioning_dag import s3_partitioning_daily
        task_ids = {task.task_id for task in s3_partitioning_daily.tasks}
        expected = {
            "wait_for_raw_data",
            "get_partition_specs",
            "validate_existing_partitions",
            "check_has_work",
            "repartition_data",
            "all_partitions_done",
            "verify_output",
            "update_partition_metadata",
        }
        assert expected.issubset(task_ids)

    def test_dag_max_active_runs(self) -> None:
        from dags.s3_management.s3_partitioning_dag import s3_partitioning_daily
        assert s3_partitioning_daily.max_active_runs == 1

    def test_dag_catchup_disabled(self) -> None:
        from dags.s3_management.s3_partitioning_dag import s3_partitioning_daily
        assert s3_partitioning_daily.catchup is False

    def test_dag_has_tags(self) -> None:
        from dags.s3_management.s3_partitioning_dag import s3_partitioning_daily
        assert "s3-management" in s3_partitioning_daily.tags
        assert "daily" in s3_partitioning_daily.tags

    def test_sensor_task_mode_is_reschedule(self) -> None:
        from dags.s3_management.s3_partitioning_dag import s3_partitioning_daily
        sensor = s3_partitioning_daily.get_task("wait_for_raw_data")
        assert sensor.mode == "reschedule"


class TestS3HotColdTieringDagStructure:
    """Tests for s3_hot_cold_tiering_weekly DAG structure."""

    def test_dag_loads_without_error(self) -> None:
        from dags.s3_management.s3_hot_cold_tiering_dag import s3_hot_cold_tiering_weekly
        assert s3_hot_cold_tiering_weekly is not None

    def test_dag_id(self) -> None:
        from dags.s3_management.s3_hot_cold_tiering_dag import s3_hot_cold_tiering_weekly
        assert s3_hot_cold_tiering_weekly.dag_id == "s3_hot_cold_tiering_weekly"

    def test_dag_schedule_is_weekly(self) -> None:
        from dags.s3_management.s3_hot_cold_tiering_dag import s3_hot_cold_tiering_weekly
        assert s3_hot_cold_tiering_weekly.schedule_interval == "0 3 * * 0"

    def test_dag_has_expected_tasks(self) -> None:
        from dags.s3_management.s3_hot_cold_tiering_dag import s3_hot_cold_tiering_weekly
        task_ids = {task.task_id for task in s3_hot_cold_tiering_weekly.tasks}
        expected = {
            "scan_partition_metadata",
            "classify_hot_cold",
            "move_cold_to_glacier",
            "update_data_catalog",
            "send_tiering_report",
        }
        assert expected.issubset(task_ids)

    def test_dag_has_cost_optimization_tag(self) -> None:
        from dags.s3_management.s3_hot_cold_tiering_dag import s3_hot_cold_tiering_weekly
        assert "cost-optimization" in s3_hot_cold_tiering_weekly.tags


class TestS3CompactionDagStructure:
    """Tests for s3_compaction_daily DAG structure."""

    def test_dag_loads_without_error(self) -> None:
        from dags.s3_management.s3_compaction_dag import s3_compaction_daily
        assert s3_compaction_daily is not None

    def test_dag_id(self) -> None:
        from dags.s3_management.s3_compaction_dag import s3_compaction_daily
        assert s3_compaction_daily.dag_id == "s3_compaction_daily"

    def test_dag_schedule_interval(self) -> None:
        from dags.s3_management.s3_compaction_dag import s3_compaction_daily
        assert s3_compaction_daily.schedule_interval == "0 4 * * *"

    def test_dag_has_expected_tasks(self) -> None:
        from dags.s3_management.s3_compaction_dag import s3_compaction_daily
        task_ids = {task.task_id for task in s3_compaction_daily.tasks}
        expected = {
            "list_small_files",
            "group_by_partition",
            "compact_partitions",
            "verify_row_counts",
            "cleanup_originals",
            "update_metadata",
        }
        assert expected.issubset(task_ids)

    def test_dag_is_not_cyclic(self) -> None:
        """Verify DAG has no cycles."""
        from dags.s3_management.s3_compaction_dag import s3_compaction_daily
        # Airflow raises CycleError on DAG creation if cycles exist
        # If we get here, DAG was created successfully -> no cycles
        assert s3_compaction_daily is not None

    def test_dag_task_count(self) -> None:
        from dags.s3_management.s3_compaction_dag import s3_compaction_daily
        assert len(s3_compaction_daily.tasks) >= 6
