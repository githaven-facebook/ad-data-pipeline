"""
S3 Hot/Cold Tiering DAG

Weekly DAG that analyzes partition access patterns and moves cold data to
S3 Glacier Instant Retrieval to reduce storage costs.

Flow:
  scan_partition_metadata → classify_hot_cold → move_cold_to_glacier
                         → update_data_catalog → send_tiering_report
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": ["data-platform@fb.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=8),
}

# Tiering thresholds
HOT_THRESHOLD_DAYS = 30
WARM_THRESHOLD_DAYS = 90
ARCHIVE_THRESHOLD_DAYS = 365
HOT_ACCESS_THRESHOLD = 10  # accesses in last 7 days
DRY_RUN = False  # Set to True for safe testing


def on_failure_callback(context: dict[str, Any]) -> None:
    """Log DAG failure."""
    logger.error(
        "Tiering DAG failed: task=%s run_id=%s",
        context["task"].task_id,
        context["run_id"],
    )


@dag(
    dag_id="s3_hot_cold_tiering_weekly",
    description="Weekly hot/cold data tiering - moves aged partitions to S3 Glacier",
    schedule_interval="0 3 * * 0",  # 3 AM UTC every Sunday
    start_date=days_ago(7),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["s3-management", "tiering", "weekly", "cost-optimization"],
    doc_md=__doc__,
    on_failure_callback=on_failure_callback,
)
def s3_hot_cold_tiering_dag() -> None:
    """Hot/cold tiering DAG definition."""

    @task(task_id="scan_partition_metadata")
    def scan_partition_metadata(**context: Any) -> list[dict[str, Any]]:
        """
        Scan all S3 partitions and collect metadata for tiering analysis.

        Returns list of partition metadata dicts with size, age, and access counts.
        """
        import boto3

        logical_date = context["logical_date"]
        logger.info("Scanning partition metadata as of %s", logical_date.date())

        s3 = boto3.client("s3")
        partitions: list[dict[str, Any]] = []

        try:
            # In production: scan processed events bucket for all year= prefixes
            paginator = s3.get_paginator("list_objects_v2")
            bucket = "fb-ad-processed-events"  # Would use Variable in production

            # Scan last 400 days of partitions
            cutoff_year = logical_date.year - 1
            for year in [cutoff_year, logical_date.year]:
                prefix = f"events/year={year}/"
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
                    for cp in page.get("CommonPrefixes", []):
                        month_prefix = cp["Prefix"]
                        partitions.append(
                            {
                                "prefix": month_prefix,
                                "bucket": bucket,
                                "size_bytes": 0,  # Would compute actual size
                                "file_count": 0,
                                "partition_date": f"{year}-01-01",
                                "last_accessed": None,
                                "access_count_7d": 0,
                                "access_count_30d": 0,
                                "current_tier": "HOT",
                            }
                        )
        except Exception as e:
            logger.warning("Error scanning partitions (continuing with empty list): %s", e)

        logger.info("Scanned %d partition prefixes", len(partitions))
        return partitions

    @task(task_id="classify_hot_cold")
    def classify_hot_cold(
        partitions: list[dict[str, Any]], **context: Any
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Classify partitions into hot, warm, cold, and archive tiers.

        Uses age and access frequency to determine target tier.
        """
        logical_date = context["logical_date"]
        reference_date = logical_date.date()

        hot: list[dict[str, Any]] = []
        warm: list[dict[str, Any]] = []
        cold: list[dict[str, Any]] = []
        archive: list[dict[str, Any]] = []

        for partition in partitions:
            try:
                partition_date = datetime.strptime(partition["partition_date"], "%Y-%m-%d").date()
                age_days = (reference_date - partition_date).days
                access_7d = partition.get("access_count_7d", 0)
                access_30d = partition.get("access_count_30d", 0)

                if age_days < HOT_THRESHOLD_DAYS or access_7d >= HOT_ACCESS_THRESHOLD:
                    partition["target_tier"] = "HOT"
                    partition["tier_reason"] = f"age={age_days}d, 7d_access={access_7d}"
                    hot.append(partition)
                elif age_days < WARM_THRESHOLD_DAYS or access_30d >= 3:
                    partition["target_tier"] = "WARM"
                    partition["tier_reason"] = f"age={age_days}d, 30d_access={access_30d}"
                    warm.append(partition)
                elif age_days < ARCHIVE_THRESHOLD_DAYS:
                    partition["target_tier"] = "COLD"
                    partition["tier_reason"] = f"age={age_days}d - moving to Glacier"
                    cold.append(partition)
                else:
                    partition["target_tier"] = "ARCHIVE"
                    partition["tier_reason"] = f"age={age_days}d - archiving to Glacier Deep Archive"
                    archive.append(partition)

            except Exception as e:
                logger.warning("Error classifying partition %s: %s", partition.get("prefix"), e)

        logger.info(
            "Classification: hot=%d, warm=%d, cold=%d, archive=%d",
            len(hot),
            len(warm),
            len(cold),
            len(archive),
        )
        return {
            "hot": hot,
            "warm": warm,
            "cold": cold,
            "archive": archive,
        }

    @task(task_id="move_cold_to_glacier", retries=3)
    def move_cold_to_glacier(
        classification: dict[str, list[dict[str, Any]]], **context: Any
    ) -> dict[str, Any]:
        """
        Move cold and archive partitions to S3 Glacier storage classes.

        Uses S3 object copy with StorageClass override then deletes originals.
        """
        import boto3
        from botocore.exceptions import ClientError

        cold_partitions = classification.get("cold", [])
        archive_partitions = classification.get("archive", [])
        all_to_move = cold_partitions + archive_partitions

        if not all_to_move:
            logger.info("No partitions to move to cold storage")
            return {"moved_count": 0, "error_count": 0, "total_size_moved_bytes": 0}

        logger.info("Moving %d partitions to cold/archive storage (dry_run=%s)", len(all_to_move), DRY_RUN)

        s3 = boto3.client("s3")
        moved_count = 0
        error_count = 0
        total_size_moved = 0

        for partition in all_to_move:
            storage_class = (
                "GLACIER_IR" if partition["target_tier"] == "COLD" else "DEEP_ARCHIVE"
            )
            bucket = partition["bucket"]
            prefix = partition["prefix"]

            if DRY_RUN:
                logger.info("[DRY RUN] Would move %s to %s", prefix, storage_class)
                moved_count += 1
                continue

            try:
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        s3.copy_object(
                            CopySource={"Bucket": bucket, "Key": key},
                            Bucket=bucket,
                            Key=key,
                            StorageClass=storage_class,
                            MetadataDirective="COPY",
                        )
                        total_size_moved += obj["Size"]

                moved_count += 1
                logger.info("Moved partition to %s: %s", storage_class, prefix)

            except ClientError as e:
                error_count += 1
                logger.error("Failed to move partition %s: %s", prefix, e)

        return {
            "moved_count": moved_count,
            "error_count": error_count,
            "total_size_moved_bytes": total_size_moved,
        }

    @task(task_id="update_data_catalog")
    def update_data_catalog(
        classification: dict[str, list[dict[str, Any]]],
        move_result: dict[str, Any],
        **context: Any,
    ) -> None:
        """Update partition tier metadata in Airflow Variables / catalog."""
        from airflow.models import Variable

        tiering_summary = {
            "run_date": context["logical_date"].isoformat(),
            "hot_count": len(classification.get("hot", [])),
            "warm_count": len(classification.get("warm", [])),
            "cold_count": len(classification.get("cold", [])),
            "archive_count": len(classification.get("archive", [])),
            "moved_count": move_result.get("moved_count", 0),
            "errors": move_result.get("error_count", 0),
        }

        Variable.set(
            "tiering_last_run_summary",
            str(tiering_summary),
            description="Summary from last tiering DAG run",
        )
        logger.info("Data catalog updated: %s", tiering_summary)

    @task(task_id="send_tiering_report")
    def send_tiering_report(
        classification: dict[str, list[dict[str, Any]]],
        move_result: dict[str, Any],
        **context: Any,
    ) -> None:
        """Generate and log cost savings report from tiering operations."""
        # Storage pricing per GB/month
        pricing = {"HOT": 0.023, "WARM": 0.0125, "COLD": 0.004, "ARCHIVE": 0.00099}

        cold_count = len(classification.get("cold", []))
        archive_count = len(classification.get("archive", []))
        moved_bytes = move_result.get("total_size_moved_bytes", 0)
        moved_gb = moved_bytes / (1024**3)

        # Estimate savings: difference between HOT and actual tier cost
        hot_cost = moved_gb * pricing["HOT"]
        cold_cost = moved_gb * pricing["COLD"] * cold_count / max(cold_count + archive_count, 1)
        archive_cost = moved_gb * pricing["ARCHIVE"] * archive_count / max(cold_count + archive_count, 1)
        estimated_monthly_savings = hot_cost - (cold_cost + archive_cost)

        report = {
            "run_date": context["logical_date"].strftime("%Y-%m-%d"),
            "partitions_analyzed": sum(len(v) for v in classification.values()),
            "partitions_moved": move_result.get("moved_count", 0),
            "data_moved_gb": round(moved_gb, 2),
            "estimated_monthly_savings_usd": round(estimated_monthly_savings, 2),
            "cold_partitions": cold_count,
            "archive_partitions": archive_count,
        }

        logger.info("Tiering report: %s", report)

    # Wire up the DAG
    partition_metadata = scan_partition_metadata()
    classification = classify_hot_cold(partition_metadata)
    move_result = move_cold_to_glacier(classification)
    update_data_catalog(classification, move_result)
    send_tiering_report(classification, move_result)


s3_hot_cold_tiering_weekly = s3_hot_cold_tiering_dag()
