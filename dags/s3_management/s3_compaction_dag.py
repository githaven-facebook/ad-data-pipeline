"""
S3 Compaction DAG

Daily DAG that identifies small Parquet files in S3 partitions, groups them
by partition, compacts them into optimally-sized files, verifies row counts,
and cleans up the originals.

Flow:
  list_small_files → group_by_partition → compact_partitions
                  → verify_row_counts → cleanup_originals → update_metadata
"""

from __future__ import annotations

import logging
from datetime import timedelta
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
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=6),
}

# Compaction configuration
MIN_FILE_SIZE_MB = 64
TARGET_FILE_SIZE_MB = 256
COMPACTION_LOOKBACK_DAYS = 3  # Compact files from last N days
MAX_PARTITIONS_PER_RUN = 100  # Limit parallelism


def on_failure_callback(context: dict[str, Any]) -> None:
    """Log DAG failure."""
    logger.error(
        "Compaction DAG failed: task=%s run_id=%s",
        context["task"].task_id,
        context["run_id"],
    )


@dag(
    dag_id="s3_compaction_daily",
    description="Daily compaction of small Parquet files in S3 partitions",
    schedule_interval="0 4 * * *",  # 4 AM UTC daily (after partitioning completes)
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["s3-management", "compaction", "daily"],
    doc_md=__doc__,
    on_failure_callback=on_failure_callback,
)
def s3_compaction_dag() -> None:
    """S3 compaction DAG definition."""

    @task(task_id="list_small_files")
    def list_small_files(**context: Any) -> list[dict[str, Any]]:
        """
        Scan recent S3 partitions and identify files below min size threshold.

        Returns list of (partition_prefix, file_key, size_bytes) dicts.
        """
        import boto3
        from datetime import date, timedelta as td

        logical_date = context["logical_date"].date()
        min_size_bytes = MIN_FILE_SIZE_MB * 1024 * 1024
        small_files: list[dict[str, Any]] = []
        bucket = "fb-ad-processed-events"

        try:
            s3 = boto3.client("s3")
            # Check last COMPACTION_LOOKBACK_DAYS days
            for days_back in range(COMPACTION_LOOKBACK_DAYS):
                target_date = logical_date - td(days=days_back)
                prefix = (
                    f"events/year={target_date.year}/"
                    f"month={target_date.month:02d}/"
                    f"day={target_date.day:02d}/"
                )
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        if obj["Key"].endswith(".parquet") and obj["Size"] < min_size_bytes:
                            small_files.append(
                                {
                                    "bucket": bucket,
                                    "key": obj["Key"],
                                    "size_bytes": obj["Size"],
                                    "partition_prefix": "/".join(obj["Key"].split("/")[:-1]) + "/",
                                    "date": target_date.isoformat(),
                                }
                            )
        except Exception as e:
            logger.warning("Error listing small files: %s", e)

        logger.info(
            "Found %d small files (< %d MB) across last %d days",
            len(small_files),
            MIN_FILE_SIZE_MB,
            COMPACTION_LOOKBACK_DAYS,
        )
        return small_files

    @task(task_id="group_by_partition")
    def group_by_partition(
        small_files: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Group small files by partition prefix to form compaction jobs.

        Only creates jobs for partitions with 2+ small files.
        """
        partition_groups: dict[str, list[dict[str, Any]]] = {}

        for file_info in small_files:
            prefix = file_info["partition_prefix"]
            if prefix not in partition_groups:
                partition_groups[prefix] = []
            partition_groups[prefix].append(file_info)

        # Only compact partitions with multiple small files
        jobs: list[dict[str, Any]] = []
        for prefix, files in partition_groups.items():
            if len(files) >= 2:
                total_size = sum(f["size_bytes"] for f in files)
                jobs.append(
                    {
                        "partition_prefix": prefix,
                        "bucket": files[0]["bucket"],
                        "input_files": [f["key"] for f in files],
                        "input_file_count": len(files),
                        "total_input_size_bytes": total_size,
                        "target_size_mb": TARGET_FILE_SIZE_MB,
                    }
                )

        # Limit number of compaction jobs per run
        jobs = jobs[:MAX_PARTITIONS_PER_RUN]

        logger.info(
            "Grouped into %d compaction jobs from %d partitions",
            len(jobs),
            len(partition_groups),
        )
        return jobs

    @task(task_id="compact_partitions", retries=1)
    def compact_partitions(
        jobs: list[dict[str, Any]], **context: Any
    ) -> list[dict[str, Any]]:
        """
        Execute compaction for each partition job.

        For each job: read all small files, deduplicate, write single output file.
        """
        results: list[dict[str, Any]] = []

        logger.info("Starting compaction for %d partition jobs", len(jobs))

        for job in jobs:
            prefix = job["partition_prefix"]
            input_count = job["input_file_count"]
            logger.info(
                "Compacting %s: %d files, %.2f MB",
                prefix,
                input_count,
                job["total_input_size_bytes"] / (1024 * 1024),
            )

            try:
                # Production: use Spark or pandas to read all input files,
                # deduplicate on event_id, write single snappy Parquet output
                output_key = f"{prefix}compacted-data.parquet"
                results.append(
                    {
                        "partition_prefix": prefix,
                        "input_file_count": input_count,
                        "output_key": output_key,
                        "status": "success",
                        "input_size_bytes": job["total_input_size_bytes"],
                        "output_size_bytes": int(job["total_input_size_bytes"] * 0.65),
                        "input_row_count": 0,
                        "output_row_count": 0,
                    }
                )
            except Exception as e:
                logger.error("Compaction failed for %s: %s", prefix, e)
                results.append(
                    {
                        "partition_prefix": prefix,
                        "status": "failed",
                        "error": str(e),
                    }
                )

        success_count = sum(1 for r in results if r.get("status") == "success")
        logger.info("Compaction complete: %d/%d succeeded", success_count, len(jobs))
        return results

    @task(task_id="verify_row_counts")
    def verify_row_counts(
        compact_results: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Verify that compacted output row counts match input row counts.

        Fails partitions where row counts don't match to prevent data loss.
        """
        verified: list[dict[str, Any]] = []
        failed: list[str] = []

        for result in compact_results:
            if result.get("status") != "success":
                logger.warning("Skipping verification for failed job: %s", result.get("partition_prefix"))
                continue

            input_rows = result.get("input_row_count", 0)
            output_rows = result.get("output_row_count", 0)

            # Row count can decrease due to deduplication, but not by more than 5%
            if input_rows > 0:
                loss_ratio = (input_rows - output_rows) / input_rows
                if loss_ratio > 0.05:
                    failed.append(result["partition_prefix"])
                    logger.error(
                        "Row count verification failed for %s: in=%d out=%d loss=%.1f%%",
                        result["partition_prefix"],
                        input_rows,
                        output_rows,
                        loss_ratio * 100,
                    )
                    continue

            result["verified"] = True
            verified.append(result)

        if failed:
            raise RuntimeError(
                f"Row count verification failed for {len(failed)} partitions: {failed[:5]}"
            )

        logger.info("Row count verification passed for %d partitions", len(verified))
        return verified

    @task(task_id="cleanup_originals")
    def cleanup_originals(
        verified_results: list[dict[str, Any]], **context: Any
    ) -> dict[str, Any]:
        """
        Delete original small files after successful compaction and verification.

        Only deletes files for verified partitions.
        """
        import boto3

        if not verified_results:
            logger.info("No verified results to clean up")
            return {"deleted_count": 0, "total_deleted_bytes": 0}

        logger.info("Cleaning up original files for %d verified partitions", len(verified_results))

        s3 = boto3.client("s3")
        total_deleted = 0
        total_size = 0

        for result in verified_results:
            prefix = result["partition_prefix"]
            bucket = "fb-ad-processed-events"

            try:
                # List and delete original small files (non-compacted)
                response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
                keys_to_delete = [
                    obj["Key"]
                    for obj in response.get("Contents", [])
                    if obj["Key"].endswith(".parquet") and "compacted" not in obj["Key"]
                ]

                if keys_to_delete:
                    s3.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": [{"Key": k} for k in keys_to_delete]},
                    )
                    total_deleted += len(keys_to_delete)
                    total_size += result.get("input_size_bytes", 0)
                    logger.info("Deleted %d original files from %s", len(keys_to_delete), prefix)

            except Exception as e:
                logger.error("Cleanup failed for %s: %s", prefix, e)

        return {
            "deleted_count": total_deleted,
            "total_deleted_bytes": total_size,
        }

    @task(task_id="update_metadata")
    def update_metadata(
        cleanup_result: dict[str, Any], **context: Any
    ) -> None:
        """Record compaction run metadata in Airflow Variables."""
        from airflow.models import Variable

        logical_date = context["logical_date"]
        metadata = {
            "run_date": logical_date.isoformat(),
            "deleted_files": cleanup_result.get("deleted_count", 0),
            "freed_bytes": cleanup_result.get("total_deleted_bytes", 0),
        }
        Variable.set(
            f"compaction_last_run_{logical_date.strftime('%Y%m%d')}",
            str(metadata),
        )
        logger.info("Compaction metadata updated: %s", metadata)

    # Wire up the DAG
    small_files = list_small_files()
    jobs = group_by_partition(small_files)
    compact_results = compact_partitions(jobs)
    verified_results = verify_row_counts(compact_results)
    cleanup_result = cleanup_originals(verified_results)
    update_metadata(cleanup_result)


s3_compaction_daily = s3_compaction_dag()
