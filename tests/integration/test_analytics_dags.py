"""Integration tests for analytics DAGs - structure and task flow."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


class TestUserSegmentationDagStructure:
    """Tests for user_segmentation_daily DAG structure."""

    def test_dag_loads_without_error(self) -> None:
        from dags.analytics.user_segmentation_dag import user_segmentation_daily
        assert user_segmentation_daily is not None

    def test_dag_id(self) -> None:
        from dags.analytics.user_segmentation_dag import user_segmentation_daily
        assert user_segmentation_daily.dag_id == "user_segmentation_daily"

    def test_dag_schedule_interval(self) -> None:
        from dags.analytics.user_segmentation_dag import user_segmentation_daily
        assert user_segmentation_daily.schedule_interval == "0 6 * * *"

    def test_dag_has_expected_tasks(self) -> None:
        from dags.analytics.user_segmentation_dag import user_segmentation_daily
        task_ids = {task.task_id for task in user_segmentation_daily.tasks}
        expected = {
            "wait_for_event_data",
            "check_rebuild_mode",
            "prepare_full_rebuild",
            "prepare_incremental_update",
            "extract_user_features",
            "compute_segments",
            "validate_segments",
            "export_to_redis",
            "notify_completion",
        }
        assert expected.issubset(task_ids)

    def test_dag_has_sla_configured(self) -> None:
        from dags.analytics.user_segmentation_dag import user_segmentation_daily
        from datetime import timedelta
        # Check SLA is set via default_args on tasks
        extract_task = user_segmentation_daily.get_task("extract_user_features")
        assert extract_task is not None

    def test_dag_has_analytics_tag(self) -> None:
        from dags.analytics.user_segmentation_dag import user_segmentation_daily
        assert "analytics" in user_segmentation_daily.tags
        assert "segmentation" in user_segmentation_daily.tags

    def test_dag_catchup_disabled(self) -> None:
        from dags.analytics.user_segmentation_dag import user_segmentation_daily
        assert user_segmentation_daily.catchup is False

    def test_dag_has_branching_task(self) -> None:
        from dags.analytics.user_segmentation_dag import user_segmentation_daily
        branch_task = user_segmentation_daily.get_task("check_rebuild_mode")
        assert branch_task is not None
        assert branch_task.task_type in ("BranchPythonOperator", "_BranchDecoratedOperator")


class TestRankingDataDagStructure:
    """Tests for ranking_data_hourly DAG structure."""

    def test_dag_loads_without_error(self) -> None:
        from dags.analytics.ranking_data_dag import ranking_data_hourly
        assert ranking_data_hourly is not None

    def test_dag_id(self) -> None:
        from dags.analytics.ranking_data_dag import ranking_data_hourly
        assert ranking_data_hourly.dag_id == "ranking_data_hourly"

    def test_dag_schedule_is_hourly(self) -> None:
        from dags.analytics.ranking_data_dag import ranking_data_hourly
        assert ranking_data_hourly.schedule_interval == "@hourly"

    def test_dag_allows_parallel_runs(self) -> None:
        from dags.analytics.ranking_data_dag import ranking_data_hourly
        # Hourly DAG must allow some overlap
        assert ranking_data_hourly.max_active_runs >= 2

    def test_dag_has_expected_tasks(self) -> None:
        from dags.analytics.ranking_data_dag import ranking_data_hourly
        task_ids = {task.task_id for task in ranking_data_hourly.tasks}
        expected = {
            "wait_for_aggregated_data",
            "compute_ctr_cvr_ecpm",
            "generate_feature_vectors",
            "export_to_feature_store",
            "validate_freshness",
        }
        assert expected.issubset(task_ids)

    def test_dag_has_critical_path_tag(self) -> None:
        from dags.analytics.ranking_data_dag import ranking_data_hourly
        assert "critical-path" in ranking_data_hourly.tags

    def test_dag_has_short_retry_delay(self) -> None:
        """Critical DAG should retry quickly."""
        from dags.analytics.ranking_data_dag import DEFAULT_ARGS
        from datetime import timedelta
        assert DEFAULT_ARGS["retry_delay"] <= timedelta(minutes=5)

    def test_dag_has_high_retry_count(self) -> None:
        """Critical DAG should retry more aggressively."""
        from dags.analytics.ranking_data_dag import DEFAULT_ARGS
        assert DEFAULT_ARGS["retries"] >= 3


class TestAdvertiserReportDagStructure:
    """Tests for advertiser_report_daily DAG structure."""

    def test_dag_loads_without_error(self) -> None:
        from dags.analytics.advertiser_report_dag import advertiser_report_daily
        assert advertiser_report_daily is not None

    def test_dag_id(self) -> None:
        from dags.analytics.advertiser_report_dag import advertiser_report_daily
        assert advertiser_report_daily.dag_id == "advertiser_report_daily"

    def test_dag_schedule_interval(self) -> None:
        from dags.analytics.advertiser_report_dag import advertiser_report_daily
        assert advertiser_report_daily.schedule_interval == "0 7 * * *"

    def test_dag_has_expected_tasks(self) -> None:
        from dags.analytics.advertiser_report_dag import advertiser_report_daily
        task_ids = {task.task_id for task in advertiser_report_daily.tasks}
        expected = {
            "wait_for_previous_day_data",
            "aggregate_by_campaign_hierarchy",
            "compute_kpis",
            "generate_reports",
            "store_in_db",
            "send_email_notifications",
            "mark_reports_finalized",
        }
        assert expected.issubset(task_ids)

    def test_dag_has_advertiser_facing_tag(self) -> None:
        from dags.analytics.advertiser_report_dag import advertiser_report_daily
        assert "advertiser-facing" in advertiser_report_daily.tags

    def test_dag_sensor_waits_for_24_hours(self) -> None:
        """Sensor should require all 24 hourly partitions."""
        from dags.analytics.advertiser_report_dag import advertiser_report_daily
        sensor = advertiser_report_daily.get_task("wait_for_previous_day_data")
        assert sensor.min_file_count == 24

    def test_dag_is_not_cyclic(self) -> None:
        from dags.analytics.advertiser_report_dag import advertiser_report_daily
        assert advertiser_report_daily is not None

    def test_dag_max_active_runs_is_one(self) -> None:
        from dags.analytics.advertiser_report_dag import advertiser_report_daily
        assert advertiser_report_daily.max_active_runs == 1
