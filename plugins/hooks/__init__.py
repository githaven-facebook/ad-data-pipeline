"""Custom Airflow hooks for ad data pipeline."""

from plugins.hooks.custom_s3_hook import CustomS3Hook

__all__ = ["CustomS3Hook"]
