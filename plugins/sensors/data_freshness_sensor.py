"""Sensor that checks data freshness against expected SLA."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Optional, Sequence

from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

logger = logging.getLogger(__name__)


class DataFreshnessSensor(BaseSensorOperator):
    """
    Checks that data in an S3 partition meets freshness SLA requirements.

    Determines freshness by:
    1. Listing files in the S3 partition
    2. Finding the most recently modified file
    3. Verifying the modification time is within the SLA window

    Args:
        bucket_name: S3 bucket to check
        partition_prefix: S3 prefix of the partition to check
        max_staleness_minutes: Maximum allowed data staleness in minutes
        aws_conn_id: Airflow AWS connection ID
        poke_interval: Seconds between freshness checks (default 120)
        timeout: Maximum seconds to wait (default 1800 = 30 min)
    """

    template_fields: Sequence[str] = ("bucket_name", "partition_prefix")
    ui_color = "#e8f5e9"

    def __init__(
        self,
        bucket_name: str,
        partition_prefix: str,
        max_staleness_minutes: int = 60,
        aws_conn_id: str = "aws_default",
        poke_interval: float = 120.0,
        timeout: float = 1800.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            **kwargs,
        )
        self.bucket_name = bucket_name
        self.partition_prefix = partition_prefix
        self.max_staleness_minutes = max_staleness_minutes
        self.aws_conn_id = aws_conn_id

    def poke(self, context: Context) -> bool:
        """
        Check if the partition's data is fresh enough.

        Returns:
            True if data is within SLA window, False to keep waiting
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        self.log.info(
            "Checking data freshness for s3://%s/%s (max_staleness=%dm)",
            self.bucket_name,
            self.partition_prefix,
            self.max_staleness_minutes,
        )

        hook = S3Hook(aws_conn_id=self.aws_conn_id)
        keys = hook.list_keys(
            bucket_name=self.bucket_name,
            prefix=self.partition_prefix,
        ) or []

        if not keys:
            self.log.info("No objects found at prefix %s", self.partition_prefix)
            return False

        # Find most recently modified object
        latest_modified: Optional[datetime] = None
        for key in keys:
            obj = hook.get_key(key=key, bucket_name=self.bucket_name)
            obj_modified = obj.last_modified
            if obj_modified.tzinfo is not None:
                # Make timezone-naive for comparison
                obj_modified = obj_modified.replace(tzinfo=None)
            if latest_modified is None or obj_modified > latest_modified:
                latest_modified = obj_modified

        if latest_modified is None:
            return False

        now = datetime.utcnow()
        staleness = now - latest_modified
        max_staleness = timedelta(minutes=self.max_staleness_minutes)
        is_fresh = staleness <= max_staleness

        self.log.info(
            "Data freshness: last_modified=%s, staleness=%.1fm, sla=%dm, fresh=%s",
            latest_modified.isoformat(),
            staleness.total_seconds() / 60,
            self.max_staleness_minutes,
            is_fresh,
        )

        return is_fresh
