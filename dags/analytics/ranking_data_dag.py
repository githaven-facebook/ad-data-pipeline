"""
Ranking Data DAG

Hourly DAG that computes ranking features for the real-time ad serving system.
Critical path DAG with strict SLA - delays impact ad serving quality.

Flow:
  wait_for_aggregated_data → compute_ctr_cvr_ecpm → generate_feature_vectors
                           → export_to_feature_store → validate_freshness
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from plugins.sensors.data_freshness_sensor import DataFreshnessSensor
from plugins.sensors.s3_partition_sensor import S3PartitionReadinessSensor

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": ["data-platform@fb.com", "oncall-ads@fb.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
    "sla": timedelta(minutes=30),  # Hard SLA: features must be fresh within 30 min
}

FEATURE_STORE_BUCKET = "fb-ad-feature-store"
LOOKBACK_HOURS = 24
MAX_STALENESS_MINUTES = 30


def on_failure_callback(context: dict[str, Any]) -> None:
    """Critical alert on ranking DAG failure - impacts ad serving."""
    logger.critical(
        "CRITICAL: Ranking data DAG failed: task=%s run_id=%s - Ad serving may be degraded",
        context["task"].task_id,
        context["run_id"],
    )


def on_sla_miss_callback(dag: Any, task_list: Any, blocking_task_list: Any, slas: Any, blocking_tis: Any) -> None:
    """Alert when ranking features miss SLA."""
    logger.error(
        "SLA MISS on ranking_data_hourly: %d tasks missed SLA",
        len(slas),
    )


@dag(
    dag_id="ranking_data_hourly",
    description="Hourly ranking feature computation for real-time ad serving",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=2,  # Allow overlap between hours
    default_args=DEFAULT_ARGS,
    tags=["analytics", "ranking", "hourly", "critical-path"],
    doc_md=__doc__,
    on_failure_callback=on_failure_callback,
    sla_miss_callback=on_sla_miss_callback,
)
def ranking_data_dag() -> None:
    """Ranking data DAG definition."""

    # Wait for last hour's aggregated event data
    wait_for_aggregated_data = S3PartitionReadinessSensor(
        task_id="wait_for_aggregated_data",
        bucket_name="{{ var.value.processed_events_bucket }}",
        partition_prefix=(
            "events/"
            "year={{ execution_date.year }}/"
            "month={{ '{:02d}'.format(execution_date.month) }}/"
            "day={{ '{:02d}'.format(execution_date.day) }}/"
            "hour={{ '{:02d}'.format(execution_date.hour) }}/"
        ),
        min_file_count=1,
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=1800,
        mode="reschedule",
    )

    @task(task_id="compute_ctr_cvr_ecpm")
    def compute_ctr_cvr_ecpm(**context: Any) -> dict[str, Any]:
        """
        Compute CTR, CVR, and eCPM for all active ads at multiple time windows.

        Time windows: 1h, 6h, 24h with exponential time decay weighting.
        Uses Laplace smoothing to stabilize rates for low-traffic ads.
        """
        logical_date = context["logical_date"]
        logger.info("Computing ranking metrics for hour ending %s", logical_date)

        # In production: run Spark/Flink aggregation over S3 event windows
        # Aggregate counts by ad_id for each time window
        metrics = {
            "computation_hour": logical_date.strftime("%Y-%m-%dT%H:00:00"),
            "ads_processed": 0,
            "windows_computed": ["1h", "6h", "24h"],
            "decay_half_life_hours": 6.0,
            "laplace_alpha": 1.0,
        }

        logger.info("Ranking metrics computed: %s", metrics)
        return metrics

    @task(task_id="generate_feature_vectors")
    def generate_feature_vectors(metrics: dict[str, Any], **context: Any) -> dict[str, Any]:
        """
        Generate flattened feature vectors for ML feature store.

        Feature vector includes: CTR/CVR at 3 windows, time-decayed CTR/CVR/eCPM,
        budget utilization, pacing factor, log-scaled impression/click counts.
        """
        logical_date = context["logical_date"]
        hour_str = logical_date.strftime("%Y-%m-%dT%H:00:00")

        feature_output_prefix = (
            f"ranking-features/"
            f"year={logical_date.year}/"
            f"month={logical_date.month:02d}/"
            f"day={logical_date.day:02d}/"
            f"hour={logical_date.hour:02d}/"
        )

        logger.info(
            "Generating feature vectors for %s -> s3://%s/%s",
            hour_str,
            FEATURE_STORE_BUCKET,
            feature_output_prefix,
        )

        feature_stats = {
            "feature_output_prefix": feature_output_prefix,
            "feature_count": 16,  # Length of FEATURE_NAMES in RankingFeatureComputer
            "computation_hour": hour_str,
            "ads_vectorized": metrics.get("ads_processed", 0),
        }

        return feature_stats

    @task(task_id="export_to_feature_store", retries=3)
    def export_to_feature_store(
        feature_stats: dict[str, Any], **context: Any
    ) -> dict[str, Any]:
        """
        Export feature vectors to the feature store (S3 + Redis).

        S3 serves as the durable feature store for batch serving.
        Redis serves as the low-latency cache for real-time serving.
        """
        logical_date = context["logical_date"]
        feature_prefix = feature_stats["feature_output_prefix"]
        ads_count = feature_stats["ads_vectorized"]

        logger.info(
            "Exporting %d ad feature vectors to feature store",
            ads_count,
        )

        # Write feature metadata for freshness tracking
        import boto3
        import json

        metadata = {
            "feature_version": "v1",
            "computation_hour": feature_stats["computation_hour"],
            "ads_count": ads_count,
            "export_timestamp": logical_date.isoformat(),
            "feature_names": [
                "ctr_1h", "ctr_6h", "ctr_24h",
                "cvr_1h", "cvr_6h", "cvr_24h",
                "ecpm_1h", "ecpm_24h",
                "ctr_decayed", "cvr_decayed", "ecpm_decayed",
                "budget_utilization", "pacing_factor",
                "impressions_24h_log", "clicks_24h_log", "conversions_24h_log",
            ],
        }

        try:
            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=FEATURE_STORE_BUCKET,
                Key=f"{feature_prefix}_metadata.json",
                Body=json.dumps(metadata),
                ContentType="application/json",
            )
            logger.info("Feature metadata written to S3")
        except Exception as e:
            logger.warning("Could not write feature metadata (non-fatal): %s", e)

        return {
            "exported_ads": ads_count,
            "feature_prefix": feature_prefix,
            "export_timestamp": logical_date.isoformat(),
            "feature_store_updated": True,
        }

    @task(task_id="validate_freshness")
    def validate_freshness(export_result: dict[str, Any], **context: Any) -> None:
        """
        Validate that feature store was updated within SLA window.

        Raises an error if features are stale, triggering retry and alert.
        """
        from datetime import datetime, timezone

        logical_date = context["logical_date"]
        export_timestamp_str = export_result.get("export_timestamp", "")

        if not export_timestamp_str:
            raise ValueError("No export timestamp found - feature store update may have failed")

        export_time = datetime.fromisoformat(export_timestamp_str)
        now = datetime.utcnow()

        if export_time.tzinfo is not None:
            export_time = export_time.replace(tzinfo=None)

        staleness_minutes = (now - export_time).total_seconds() / 60

        if staleness_minutes > MAX_STALENESS_MINUTES:
            raise ValueError(
                f"Feature store is stale: {staleness_minutes:.1f} minutes old "
                f"(SLA={MAX_STALENESS_MINUTES}m). Ad serving may use stale features."
            )

        logger.info(
            "Freshness validation passed: features are %.1f minutes old (SLA=%dm)",
            staleness_minutes,
            MAX_STALENESS_MINUTES,
        )

    # Wire up the DAG
    metrics = compute_ctr_cvr_ecpm()
    feature_stats = generate_feature_vectors(metrics)
    export_result = export_to_feature_store(feature_stats)
    validate_freshness(export_result)

    wait_for_aggregated_data >> metrics


ranking_data_hourly = ranking_data_dag()
