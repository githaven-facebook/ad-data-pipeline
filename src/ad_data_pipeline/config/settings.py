"""Application settings using pydantic-settings for environment-based configuration."""

from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class SparkConfig(BaseSettings):
    """Spark cluster configuration."""

    model_config = SettingsConfigDict(env_prefix="SPARK_")

    master: str = Field(default="local[*]", description="Spark master URL")
    driver_memory: str = Field(default="4g", description="Driver memory allocation")
    executor_memory: str = Field(default="8g", description="Executor memory allocation")
    executor_cores: int = Field(default=4, description="Number of executor cores")
    max_executors: int = Field(default=20, description="Maximum number of executors")
    dynamic_allocation: bool = Field(default=True, description="Enable dynamic allocation")
    shuffle_partitions: int = Field(default=200, description="Number of shuffle partitions")
    default_parallelism: int = Field(default=100, description="Default parallelism")
    s3_endpoint: Optional[str] = Field(default=None, description="Custom S3 endpoint for localstack")


class Settings(BaseSettings):
    """Main application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="AD_PIPELINE_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # AWS configuration
    aws_region: str = Field(default="us-east-1", description="AWS region")
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS access key ID")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS secret access key")

    # S3 bucket names
    raw_events_bucket: str = Field(default="fb-ad-raw-events", description="Raw ad events S3 bucket")
    processed_events_bucket: str = Field(
        default="fb-ad-processed-events", description="Processed events S3 bucket"
    )
    cold_storage_bucket: str = Field(default="fb-ad-cold-storage", description="Cold storage S3 Glacier bucket")
    analytics_bucket: str = Field(default="fb-ad-analytics", description="Analytics output S3 bucket")
    feature_store_bucket: str = Field(default="fb-ad-feature-store", description="Feature store S3 bucket")

    # Data retention and tiering
    hot_data_retention_days: int = Field(default=30, description="Days to keep data in hot storage")
    cold_data_retention_days: int = Field(default=365, description="Days to keep data in cold storage")
    max_partition_size_mb: int = Field(default=512, description="Maximum partition file size in MB")
    min_partition_size_mb: int = Field(default=64, description="Minimum partition size before compaction")
    compaction_target_size_mb: int = Field(default=256, description="Target size for compacted partitions")

    # Partition scheme
    partition_columns: list[str] = Field(
        default=["year", "month", "day", "hour"],
        description="S3 partition column names",
    )
    event_types: list[str] = Field(
        default=["impression", "click", "conversion", "bid_request", "bid_response"],
        description="Valid ad event types",
    )

    # Airflow / pipeline config
    dag_default_retries: int = Field(default=3, description="Default retry count for DAG tasks")
    dag_retry_delay_minutes: int = Field(default=5, description="Minutes to wait between retries")
    task_timeout_hours: int = Field(default=4, description="Default task timeout in hours")
    data_freshness_sla_minutes: int = Field(default=30, description="Data freshness SLA in minutes")

    # Analytics config
    segmentation_lookback_days: int = Field(default=90, description="Lookback window for user segmentation")
    ranking_features_lookback_hours: int = Field(default=24, description="Lookback for ranking feature computation")
    report_lookback_days: int = Field(default=1, description="Days of data included in advertiser reports")
    min_user_events_for_segment: int = Field(default=5, description="Minimum events to include user in segment")

    # Redis config
    redis_host: str = Field(default="localhost", description="Redis host for segment storage")
    redis_port: int = Field(default=6379, description="Redis port")
    redis_db: int = Field(default=0, description="Redis database index")
    segment_ttl_seconds: int = Field(default=86400, description="TTL for user segment data in Redis")

    # Monitoring
    statsd_host: str = Field(default="localhost", description="StatsD host")
    statsd_port: int = Field(default=8125, description="StatsD port")
    statsd_prefix: str = Field(default="ad_pipeline", description="StatsD metric prefix")

    # Alerting
    pagerduty_integration_key: Optional[str] = Field(default=None, description="PagerDuty integration key")
    slack_webhook_url: Optional[str] = Field(default=None, description="Slack webhook URL for alerts")
    alert_email_recipients: list[str] = Field(
        default=["data-platform@fb.com"],
        description="Email recipients for pipeline alerts",
    )

    @field_validator("hot_data_retention_days", "cold_data_retention_days", mode="before")
    @classmethod
    def validate_positive_days(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("Retention days must be positive")
        return v

    @field_validator("aws_region", mode="before")
    @classmethod
    def validate_aws_region(cls, v: str) -> str:
        valid_regions = [
            "us-east-1",
            "us-east-2",
            "us-west-1",
            "us-west-2",
            "eu-west-1",
            "eu-central-1",
            "ap-southeast-1",
        ]
        if v not in valid_regions:
            raise ValueError(f"Invalid AWS region: {v}. Must be one of {valid_regions}")
        return v


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings instance."""
    return Settings()
