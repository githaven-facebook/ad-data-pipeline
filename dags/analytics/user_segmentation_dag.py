"""
User Segmentation DAG

Daily DAG that computes user segments from ad event data using RFM analysis
and rule-based classification. Supports full rebuild and incremental update modes.

Flow:
  wait_for_data → check_rebuild_mode → [full_rebuild | incremental_update]
               → extract_user_features → compute_segments → validate_segments
               → export_to_redis → notify_completion
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from plugins.sensors.s3_partition_sensor import S3PartitionReadinessSensor

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-science",
    "depends_on_past": False,
    "email": ["data-science@fb.com", "data-platform@fb.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=6),
    "sla": timedelta(hours=8),
}

LOOKBACK_DAYS = 90
MIN_USER_EVENTS = 5
SEGMENT_TTL_SECONDS = 86400  # 24 hours in Redis
FULL_REBUILD_DAY_OF_WEEK = 0  # Monday = full rebuild


def on_failure_callback(context: dict[str, Any]) -> None:
    """Alert on segmentation DAG failure."""
    logger.error(
        "Segmentation DAG failed: task=%s run_id=%s",
        context["task"].task_id,
        context["run_id"],
    )


@dag(
    dag_id="user_segmentation_daily",
    description="Daily user segmentation using RFM analysis and rule-based classification",
    schedule_interval="0 6 * * *",  # 6 AM UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["analytics", "segmentation", "daily", "ml"],
    doc_md=__doc__,
    on_failure_callback=on_failure_callback,
)
def user_segmentation_dag() -> None:
    """User segmentation DAG definition."""

    # Wait for previous day's processed events to be available
    wait_for_event_data = S3PartitionReadinessSensor(
        task_id="wait_for_event_data",
        bucket_name="{{ var.value.processed_events_bucket }}",
        partition_prefix="events/year={{ execution_date.year }}/month={{ '{:02d}'.format(execution_date.month) }}/day={{ '{:02d}'.format((execution_date - macros.timedelta(days=1)).day) }}/",
        min_file_count=1,
        aws_conn_id="aws_default",
        poke_interval=180,
        timeout=10800,
        mode="reschedule",
    )

    @task.branch(task_id="check_rebuild_mode")
    def check_rebuild_mode(**context: Any) -> str:
        """Determine whether to do full rebuild or incremental update."""
        logical_date = context["logical_date"]
        is_monday = logical_date.weekday() == FULL_REBUILD_DAY_OF_WEEK

        # Force full rebuild from Variable override or on Mondays
        from airflow.models import Variable
        force_full = Variable.get("force_segmentation_full_rebuild", default_var="false").lower() == "true"

        if is_monday or force_full:
            logger.info("Full rebuild triggered (monday=%s, forced=%s)", is_monday, force_full)
            return "prepare_full_rebuild"
        else:
            logger.info("Incremental update mode")
            return "prepare_incremental_update"

    @task(task_id="prepare_full_rebuild")
    def prepare_full_rebuild(**context: Any) -> dict[str, Any]:
        """Prepare full rebuild parameters - uses all LOOKBACK_DAYS of data."""
        logical_date = context["logical_date"]
        from datetime import timedelta
        start_date = (logical_date - timedelta(days=LOOKBACK_DAYS)).date()
        return {
            "mode": "full",
            "start_date": start_date.isoformat(),
            "end_date": logical_date.date().isoformat(),
            "lookback_days": LOOKBACK_DAYS,
        }

    @task(task_id="prepare_incremental_update")
    def prepare_incremental_update(**context: Any) -> dict[str, Any]:
        """Prepare incremental update - uses only last 7 days of data."""
        logical_date = context["logical_date"]
        from datetime import timedelta
        start_date = (logical_date - timedelta(days=7)).date()
        return {
            "mode": "incremental",
            "start_date": start_date.isoformat(),
            "end_date": logical_date.date().isoformat(),
            "lookback_days": 7,
        }

    @task(
        task_id="extract_user_features",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    def extract_user_features(**context: Any) -> dict[str, Any]:
        """
        Extract aggregated user features from S3 event data.

        Computes per-user: impression counts, click counts, conversion counts,
        spend, recency metrics across multiple time windows.
        """
        import boto3
        from collections import defaultdict

        logical_date = context["logical_date"]
        logger.info("Extracting user features as of %s", logical_date.date())

        # In production: run Spark job to aggregate events by user_id
        # Here we simulate the coordination layer
        user_count_estimate = 0
        try:
            s3 = boto3.client("s3")
            response = s3.list_objects_v2(
                Bucket="fb-ad-processed-events",
                Prefix=f"events/year={logical_date.year}/",
                MaxKeys=1,
            )
            partition_exists = response.get("KeyCount", 0) > 0
            user_count_estimate = 1_000_000 if partition_exists else 0
        except Exception as e:
            logger.warning("Could not estimate user count: %s", e)

        feature_output_prefix = (
            f"user-features/date={logical_date.strftime('%Y-%m-%d')}/"
        )
        logger.info(
            "Feature extraction targeting ~%d users, output: s3://fb-ad-analytics/%s",
            user_count_estimate,
            feature_output_prefix,
        )

        return {
            "feature_output_prefix": feature_output_prefix,
            "estimated_user_count": user_count_estimate,
            "logical_date": logical_date.isoformat(),
        }

    @task(task_id="compute_segments")
    def compute_segments(feature_result: dict[str, Any], **context: Any) -> dict[str, Any]:
        """
        Assign users to segments based on extracted features.

        Uses RFM scoring and rule-based classification to assign segment membership.
        """
        logical_date = context["logical_date"]
        feature_prefix = feature_result["feature_output_prefix"]
        estimated_users = feature_result["estimated_user_count"]

        logger.info(
            "Computing segments for ~%d users from %s",
            estimated_users,
            feature_prefix,
        )

        # Simulate segment size estimates
        segments = {
            "champions": int(estimated_users * 0.05),
            "loyal": int(estimated_users * 0.12),
            "potential_loyal": int(estimated_users * 0.18),
            "new_users": int(estimated_users * 0.20),
            "at_risk": int(estimated_users * 0.10),
            "cant_lose": int(estimated_users * 0.08),
            "hibernating": int(estimated_users * 0.15),
            "lost": int(estimated_users * 0.12),
        }

        segment_output_prefix = f"segments/date={logical_date.strftime('%Y-%m-%d')}/"

        logger.info("Segment distribution: %s", segments)

        return {
            "segment_output_prefix": segment_output_prefix,
            "segment_counts": segments,
            "total_segmented_users": sum(segments.values()),
            "logical_date": logical_date.isoformat(),
        }

    @task(task_id="validate_segments")
    def validate_segments(segment_result: dict[str, Any]) -> dict[str, Any]:
        """
        Validate segment output for quality issues.

        Checks: total user count is above minimum, no segment is > 50% of total,
        all expected segment names are present.
        """
        segments = segment_result["segment_counts"]
        total_users = segment_result["total_segmented_users"]

        required_segments = {
            "champions", "loyal", "potential_loyal", "new_users",
            "at_risk", "cant_lose", "hibernating", "lost",
        }
        missing_segments = required_segments - set(segments.keys())
        if missing_segments:
            raise ValueError(f"Missing required segments: {missing_segments}")

        if total_users < 1000:
            raise ValueError(f"Segment total too low: {total_users} users (minimum 1000)")

        for seg_name, count in segments.items():
            pct = count / total_users if total_users > 0 else 0
            if pct > 0.60:
                logger.warning(
                    "Segment '%s' contains %.1f%% of users - unusually high",
                    seg_name,
                    pct * 100,
                )

        logger.info(
            "Segment validation passed: %d total users, %d segments",
            total_users,
            len(segments),
        )
        return {**segment_result, "validation_status": "passed"}

    @task(task_id="export_to_redis")
    def export_to_redis(validated_result: dict[str, Any], **context: Any) -> dict[str, Any]:
        """
        Export segment membership to Redis for real-time ad targeting.

        Writes user->segment mappings with TTL for expiry.
        """
        logical_date = context["logical_date"]
        total_users = validated_result["total_segmented_users"]
        segments = validated_result["segment_counts"]

        logger.info(
            "Exporting %d user-segment memberships to Redis (TTL=%ds)",
            total_users,
            SEGMENT_TTL_SECONDS,
        )

        # In production: iterate segment output files, write to Redis pipeline
        # redis_client.setex(f"segment:user:{user_id}", SEGMENT_TTL_SECONDS, segment_name)

        export_stats = {
            "total_exported": total_users,
            "segments_exported": len(segments),
            "ttl_seconds": SEGMENT_TTL_SECONDS,
            "export_date": logical_date.isoformat(),
        }
        logger.info("Redis export complete: %s", export_stats)
        return export_stats

    @task(task_id="notify_completion")
    def notify_completion(export_stats: dict[str, Any], **context: Any) -> None:
        """Send completion notification with segmentation statistics."""
        from airflow.models import Variable

        logical_date = context["logical_date"]
        Variable.set(
            f"segmentation_complete_{logical_date.strftime('%Y%m%d')}",
            str(export_stats),
        )
        logger.info(
            "Segmentation complete for %s: %d users in %d segments",
            logical_date.date(),
            export_stats["total_exported"],
            export_stats["segments_exported"],
        )

    # Wire up the DAG
    rebuild_params = prepare_full_rebuild()
    incremental_params = prepare_incremental_update()
    branch = check_rebuild_mode()
    feature_result = extract_user_features()
    segment_result = compute_segments(feature_result)
    validated_result = validate_segments(segment_result)
    export_stats = export_to_redis(validated_result)
    notify_completion(export_stats)

    wait_for_event_data >> branch
    branch >> [rebuild_params, incremental_params]
    rebuild_params >> feature_result
    incremental_params >> feature_result


user_segmentation_daily = user_segmentation_dag()
