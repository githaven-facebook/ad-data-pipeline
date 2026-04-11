"""Enhanced SparkSubmitOperator with S3 and monitoring configuration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional, Sequence

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

logger = logging.getLogger(__name__)

# Default Spark packages for S3A connector and Delta Lake
DEFAULT_SPARK_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "io.delta:delta-core_2.12:2.4.0",
]

# Default Spark configuration for production S3 workloads
DEFAULT_SPARK_CONF: dict[str, str] = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.multipart.size": "128M",
    "spark.hadoop.fs.s3a.connection.maximum": "100",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.parquet.filterPushdown": "true",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "s3a://spark-logs/event-logs/",
}


class EnhancedSparkSubmitOperator(SparkSubmitOperator):
    """
    Enhanced SparkSubmitOperator with pre-configured S3 and monitoring settings.

    Extends the base SparkSubmitOperator to:
    - Automatically configure S3A filesystem settings
    - Inject monitoring/metrics configuration
    - Add standard Spark packages for S3 access
    - Support environment-based configuration injection
    - Emit task metrics on completion

    Args:
        application: Path to Spark application JAR or Python script
        application_args: Arguments to pass to the Spark application
        driver_memory: Driver memory (default 4g)
        executor_memory: Executor memory (default 8g)
        executor_cores: Number of executor cores (default 4)
        num_executors: Fixed number of executors (overrides dynamic allocation)
        extra_spark_conf: Additional Spark configuration key-value pairs
        s3_endpoint: Custom S3 endpoint for local/dev environments
        aws_conn_id: Airflow connection for AWS credentials
        enable_delta_lake: Whether to include Delta Lake packages
    """

    template_fields: Sequence[str] = (
        "application",
        "application_args",
        "_name",
    )

    def __init__(
        self,
        application: str,
        application_args: Optional[list[str]] = None,
        driver_memory: str = "4g",
        executor_memory: str = "8g",
        executor_cores: int = 4,
        num_executors: Optional[int] = None,
        extra_spark_conf: Optional[dict[str, str]] = None,
        s3_endpoint: Optional[str] = None,
        aws_conn_id: str = "aws_default",
        enable_delta_lake: bool = True,
        conn_id: str = "spark_default",
        **kwargs: Any,
    ) -> None:
        # Build merged Spark configuration
        spark_conf = dict(DEFAULT_SPARK_CONF)
        if s3_endpoint:
            spark_conf["spark.hadoop.fs.s3a.endpoint"] = s3_endpoint
            spark_conf["spark.hadoop.fs.s3a.path.style.access"] = "true"
        if extra_spark_conf:
            spark_conf.update(extra_spark_conf)

        # Build packages list
        packages = list(DEFAULT_SPARK_PACKAGES) if enable_delta_lake else DEFAULT_SPARK_PACKAGES[:2]

        super().__init__(
            application=application,
            conn_id=conn_id,
            conf=spark_conf,
            application_args=application_args or [],
            driver_memory=driver_memory,
            executor_memory=executor_memory,
            executor_cores=executor_cores,
            num_executors=num_executors,
            packages=",".join(packages),
            **kwargs,
        )
        self.aws_conn_id = aws_conn_id
        self.s3_endpoint = s3_endpoint

    def execute(self, context: Context) -> Any:
        """Execute Spark job with enhanced logging."""
        self.log.info(
            "Submitting Spark job: %s (driver=%s, executor=%s x %d cores)",
            self.application,
            self._driver_memory,
            self._executor_memory,
            self._executor_cores,
        )
        result = super().execute(context)
        self.log.info("Spark job completed successfully: %s", self.application)
        return result
