"""SparkSession factory with S3 and Parquet configurations."""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None  # type: ignore[assignment]


class SparkSessionFactory:
    """
    Factory for creating SparkSession instances configured for S3 and Parquet.

    Handles:
    - S3A connector configuration with proper credentials
    - Parquet read/write optimizations
    - Dynamic resource allocation
    - Monitoring and metrics configuration
    """

    def __init__(
        self,
        app_name: str,
        master: str = "local[*]",
        driver_memory: str = "4g",
        executor_memory: str = "8g",
        executor_cores: int = 4,
        max_executors: int = 20,
        shuffle_partitions: int = 200,
        dynamic_allocation: bool = True,
        s3_endpoint: Optional[str] = None,
        aws_region: str = "us-east-1",
        log_level: str = "WARN",
    ) -> None:
        self.app_name = app_name
        self.master = master
        self.driver_memory = driver_memory
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.max_executors = max_executors
        self.shuffle_partitions = shuffle_partitions
        self.dynamic_allocation = dynamic_allocation
        self.s3_endpoint = s3_endpoint
        self.aws_region = aws_region
        self.log_level = log_level
        self._logger = logging.getLogger(self.__class__.__name__)

    def create_session(self) -> "SparkSession":  # type: ignore[name-defined]
        """
        Create and configure a SparkSession.

        Returns:
            Configured SparkSession instance

        Raises:
            RuntimeError: If PySpark is not installed
        """
        if not PYSPARK_AVAILABLE:
            raise RuntimeError("PySpark is not installed. Install pyspark to use SparkSessionFactory.")

        self._logger.info("Creating SparkSession: app=%s, master=%s", self.app_name, self.master)

        builder = (
            SparkSession.builder.appName(self.app_name)
            .master(self.master)
            .config("spark.driver.memory", self.driver_memory)
            .config("spark.executor.memory", self.executor_memory)
            .config("spark.executor.cores", str(self.executor_cores))
            .config("spark.sql.shuffle.partitions", str(self.shuffle_partitions))
        )

        # Dynamic allocation
        if self.dynamic_allocation:
            builder = (
                builder.config("spark.dynamicAllocation.enabled", "true")
                .config("spark.dynamicAllocation.minExecutors", "1")
                .config("spark.dynamicAllocation.maxExecutors", str(self.max_executors))
                .config("spark.dynamicAllocation.initialExecutors", "2")
            )

        # S3A connector configuration
        builder = self._configure_s3(builder)

        # Parquet optimizations
        builder = self._configure_parquet(builder)

        # Monitoring
        builder = self._configure_monitoring(builder)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(self.log_level)

        self._logger.info("SparkSession created successfully")
        return spark

    def _configure_s3(self, builder: "SparkSession.Builder") -> "SparkSession.Builder":  # type: ignore[name-defined]
        """Apply S3A filesystem configuration."""
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
            .config("spark.hadoop.fs.s3a.multipart.size", "128M")
            .config("spark.hadoop.fs.s3a.connection.maximum", "100")
            .config("spark.hadoop.fs.s3a.threads.max", "64")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
            .config("spark.hadoop.fs.s3a.path.style.access", "false")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        )

        if self.s3_endpoint:
            builder = builder.config("spark.hadoop.fs.s3a.endpoint", self.s3_endpoint)

        return builder

    def _configure_parquet(self, builder: "SparkSession.Builder") -> "SparkSession.Builder":  # type: ignore[name-defined]
        """Apply Parquet read/write optimizations."""
        return (
            builder.config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.parquet.mergeSchema", "false")
            .config("spark.sql.parquet.filterPushdown", "true")
            .config("spark.sql.parquet.enableVectorizedReader", "true")
            .config("spark.sql.parquet.recordLevelFilter.enabled", "true")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
        )

    def _configure_monitoring(self, builder: "SparkSession.Builder") -> "SparkSession.Builder":  # type: ignore[name-defined]
        """Apply Spark monitoring and metrics configuration."""
        return (
            builder.config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "/tmp/spark-events")
            .config("spark.metrics.conf.*.sink.statsd.class",
                    "org.apache.spark.metrics.sink.StatsdSink")
            .config("spark.ui.enabled", "true")
        )

    @classmethod
    def from_settings(cls, app_name: str, settings: object) -> "SparkSessionFactory":
        """
        Create factory instance from application Settings object.

        Args:
            app_name: Spark application name
            settings: Settings instance with Spark configuration

        Returns:
            Configured SparkSessionFactory
        """
        return cls(
            app_name=app_name,
            master=getattr(settings, "spark_master", "local[*]"),
            driver_memory=getattr(settings, "spark_driver_memory", "4g"),
            executor_memory=getattr(settings, "spark_executor_memory", "8g"),
            executor_cores=getattr(settings, "spark_executor_cores", 4),
            max_executors=getattr(settings, "spark_max_executors", 20),
            shuffle_partitions=getattr(settings, "spark_shuffle_partitions", 200),
            dynamic_allocation=getattr(settings, "spark_dynamic_allocation", True),
            s3_endpoint=getattr(settings, "spark_s3_endpoint", None),
            aws_region=getattr(settings, "aws_region", "us-east-1"),
        )
