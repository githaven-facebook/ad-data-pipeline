"""S3 helper utilities: list partitions, check existence, get sizes, copy/move objects."""

import logging
from dataclasses import dataclass
from typing import Any, Iterator, Optional

import boto3
from botocore.exceptions import ClientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


@dataclass
class S3Object:
    """Represents an S3 object with key metadata."""

    bucket: str
    key: str
    size_bytes: int
    last_modified: Any
    etag: str = ""
    storage_class: str = "STANDARD"

    @property
    def s3_uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"

    @property
    def size_mb(self) -> float:
        return self.size_bytes / (1024 * 1024)


@dataclass
class PartitionInfo:
    """Information about an S3 partition."""

    prefix: str
    file_count: int
    total_size_bytes: int
    objects: list[S3Object]

    @property
    def total_size_mb(self) -> float:
        return self.total_size_bytes / (1024 * 1024)

    @property
    def avg_file_size_bytes(self) -> float:
        return self.total_size_bytes / self.file_count if self.file_count > 0 else 0.0


class S3Utils:
    """
    S3 helper utilities for partition management and object operations.

    Provides retry-backed methods for:
    - Listing partition contents
    - Checking partition existence and completeness
    - Getting partition size estimates
    - Copying and moving objects between buckets/prefixes
    """

    DEFAULT_MAX_KEYS = 1000

    def __init__(
        self,
        aws_region: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        max_retries: int = 3,
    ) -> None:
        self.aws_region = aws_region
        self.endpoint_url = endpoint_url
        self.max_retries = max_retries
        self._client = boto3.client(
            "s3",
            region_name=aws_region,
            endpoint_url=endpoint_url,
        )
        self._resource = boto3.resource(
            "s3",
            region_name=aws_region,
            endpoint_url=endpoint_url,
        )
        self._logger = logging.getLogger(self.__class__.__name__)

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def list_partition_objects(
        self,
        bucket: str,
        prefix: str,
        suffix: str = ".parquet",
    ) -> list[S3Object]:
        """
        List all objects within a partition prefix.

        Args:
            bucket: S3 bucket name
            prefix: Partition prefix (e.g., 'events/year=2024/month=01/day=15/')
            suffix: File extension filter (default '.parquet')

        Returns:
            List of S3Object for each matching file
        """
        objects: list[S3Object] = []
        paginator = self._client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(suffix):
                    objects.append(
                        S3Object(
                            bucket=bucket,
                            key=obj["Key"],
                            size_bytes=obj["Size"],
                            last_modified=obj["LastModified"],
                            etag=obj.get("ETag", "").strip('"'),
                            storage_class=obj.get("StorageClass", "STANDARD"),
                        )
                    )

        self._logger.debug("Listed %d objects in s3://%s/%s", len(objects), bucket, prefix)
        return objects

    def get_partition_info(self, bucket: str, prefix: str) -> PartitionInfo:
        """Get aggregated info about a partition prefix."""
        objects = self.list_partition_objects(bucket, prefix)
        total_size = sum(o.size_bytes for o in objects)
        return PartitionInfo(
            prefix=prefix,
            file_count=len(objects),
            total_size_bytes=total_size,
            objects=objects,
        )

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def partition_exists(self, bucket: str, prefix: str) -> bool:
        """Check if a partition prefix contains any objects."""
        response = self._client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return response.get("KeyCount", 0) > 0

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def get_partition_size_bytes(self, bucket: str, prefix: str) -> int:
        """Get total size in bytes of all objects in a partition."""
        total = 0
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                total += obj["Size"]
        return total

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
        storage_class: str = "STANDARD",
    ) -> bool:
        """
        Copy a single S3 object to a new location.

        Args:
            source_bucket: Source S3 bucket
            source_key: Source object key
            dest_bucket: Destination bucket
            dest_key: Destination object key
            storage_class: Destination storage class

        Returns:
            True if copy succeeded
        """
        try:
            self._client.copy_object(
                CopySource={"Bucket": source_bucket, "Key": source_key},
                Bucket=dest_bucket,
                Key=dest_key,
                StorageClass=storage_class,
            )
            self._logger.debug(
                "Copied s3://%s/%s -> s3://%s/%s",
                source_bucket,
                source_key,
                dest_bucket,
                dest_key,
            )
            return True
        except ClientError as e:
            self._logger.error("Failed to copy object: %s", e)
            raise

    def move_object(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
        storage_class: str = "STANDARD",
    ) -> bool:
        """Copy then delete source object (S3 has no native move)."""
        self.copy_object(source_bucket, source_key, dest_bucket, dest_key, storage_class)
        self._client.delete_object(Bucket=source_bucket, Key=source_key)
        return True

    def delete_objects(self, bucket: str, keys: list[str]) -> int:
        """
        Batch delete objects from S3.

        Args:
            bucket: S3 bucket
            keys: List of object keys to delete

        Returns:
            Number of successfully deleted objects
        """
        if not keys:
            return 0

        deleted_count = 0
        # S3 batch delete supports up to 1000 objects per request
        for i in range(0, len(keys), 1000):
            batch = keys[i : i + 1000]
            response = self._client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in batch]},
            )
            deleted_count += len(response.get("Deleted", []))
            errors = response.get("Errors", [])
            if errors:
                self._logger.error("S3 delete errors: %s", errors)

        return deleted_count

    def list_partitions_by_date(
        self,
        bucket: str,
        base_prefix: str,
        year: int,
        month: Optional[int] = None,
        day: Optional[int] = None,
    ) -> list[str]:
        """
        List partition prefixes matching a date pattern.

        Args:
            bucket: S3 bucket name
            base_prefix: Base prefix before date partitions
            year: Year to filter
            month: Month to filter (optional)
            day: Day to filter (optional)

        Returns:
            List of matching partition prefix strings
        """
        prefix = f"{base_prefix}/year={year}/"
        if month is not None:
            prefix += f"month={month:02d}/"
        if day is not None:
            prefix += f"day={day:02d}/"

        prefixes: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for common_prefix in page.get("CommonPrefixes", []):
                prefixes.append(common_prefix["Prefix"])

        return prefixes

    def iter_objects_by_prefix(
        self,
        bucket: str,
        prefix: str,
    ) -> Iterator[S3Object]:
        """Iterate over all S3 objects with a given prefix."""
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield S3Object(
                    bucket=bucket,
                    key=obj["Key"],
                    size_bytes=obj["Size"],
                    last_modified=obj["LastModified"],
                    etag=obj.get("ETag", "").strip('"'),
                    storage_class=obj.get("StorageClass", "STANDARD"),
                )
