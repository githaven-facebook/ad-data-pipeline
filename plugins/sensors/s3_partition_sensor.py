"""Sensor that waits for an S3 partition to be available with completeness check."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Sequence

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

if TYPE_CHECKING:
    from airflow.utils.context import Context

logger = logging.getLogger(__name__)


class S3PartitionReadinessSensor(S3KeySensor):
    """
    Waits for an S3 partition to be present and optionally checks completeness.

    Extends S3KeySensor to additionally verify:
    - Minimum number of files is present in the partition
    - Minimum total size threshold is met
    - A sentinel/success marker file exists (e.g., _SUCCESS)

    Args:
        bucket_name: S3 bucket name
        partition_prefix: S3 key prefix for the partition
        min_file_count: Minimum number of Parquet files required (default 1)
        min_size_bytes: Minimum total partition size in bytes (default 0)
        check_success_marker: Whether to check for _SUCCESS marker file
        aws_conn_id: Airflow AWS connection ID
        poke_interval: Seconds between poke attempts (default 60)
        timeout: Max seconds to wait before failing (default 3600)
    """

    template_fields: Sequence[str] = ("bucket_name", "partition_prefix")

    def __init__(
        self,
        bucket_name: str,
        partition_prefix: str,
        min_file_count: int = 1,
        min_size_bytes: int = 0,
        check_success_marker: bool = False,
        aws_conn_id: str = "aws_default",
        poke_interval: float = 60.0,
        timeout: float = 3600.0,
        **kwargs: Any,
    ) -> None:
        # S3KeySensor requires bucket_key; we set it to the partition prefix
        super().__init__(
            bucket_key=f"s3://{bucket_name}/{partition_prefix}",
            bucket_name=bucket_name,
            aws_conn_id=aws_conn_id,
            poke_interval=poke_interval,
            timeout=timeout,
            wildcard_match=False,
            **kwargs,
        )
        self.partition_prefix = partition_prefix
        self.min_file_count = min_file_count
        self.min_size_bytes = min_size_bytes
        self.check_success_marker = check_success_marker

    def poke(self, context: Context) -> bool:
        """
        Check partition readiness.

        Returns:
            True if partition is ready, False to keep waiting
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        self.log.info(
            "Poking S3 partition: s3://%s/%s",
            self.bucket_name,
            self.partition_prefix,
        )

        hook = S3Hook(aws_conn_id=self.aws_conn_id)

        # Check success marker if required
        if self.check_success_marker:
            success_key = f"{self.partition_prefix.rstrip('/')}/_SUCCESS"
            if not hook.check_for_key(key=success_key, bucket_name=self.bucket_name):
                self.log.info("Success marker not yet present: %s", success_key)
                return False

        # List Parquet files in partition
        keys = hook.list_keys(
            bucket_name=self.bucket_name,
            prefix=self.partition_prefix,
        ) or []
        parquet_keys = [k for k in keys if k.endswith(".parquet")]

        if len(parquet_keys) < self.min_file_count:
            self.log.info(
                "Partition not ready: found %d/%d required Parquet files",
                len(parquet_keys),
                self.min_file_count,
            )
            return False

        # Check total size if threshold is set
        if self.min_size_bytes > 0:
            total_size = 0
            for key in parquet_keys:
                obj = hook.get_key(key=key, bucket_name=self.bucket_name)
                total_size += obj.content_length

            if total_size < self.min_size_bytes:
                self.log.info(
                    "Partition not ready: size %d bytes < required %d bytes",
                    total_size,
                    self.min_size_bytes,
                )
                return False

        self.log.info(
            "Partition ready: s3://%s/%s (%d files)",
            self.bucket_name,
            self.partition_prefix,
            len(parquet_keys),
        )
        return True
