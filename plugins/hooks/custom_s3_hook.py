"""Enhanced S3 hook with retry logic, multipart upload, and compression support."""

from __future__ import annotations

import gzip
import io
import logging
import os
import time
from typing import Any, Iterator, Optional

import boto3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Minimum size for multipart upload (5 MB AWS minimum)
MULTIPART_THRESHOLD_BYTES = 5 * 1024 * 1024
# Chunk size for multipart upload (64 MB)
MULTIPART_CHUNK_SIZE_BYTES = 64 * 1024 * 1024


class CustomS3Hook(S3Hook):
    """
    Enhanced S3 hook with production-ready features:
    - Automatic retry with exponential backoff
    - Multipart upload for large files
    - Transparent gzip compression/decompression
    - Batch operations with progress logging
    - Upload/download throughput metrics

    Inherits all standard S3Hook functionality and extends it.
    """

    def __init__(
        self,
        aws_conn_id: str = "aws_default",
        max_retries: int = 3,
        retry_base_delay_seconds: float = 1.0,
        retry_max_delay_seconds: float = 30.0,
        multipart_threshold_bytes: int = MULTIPART_THRESHOLD_BYTES,
        multipart_chunk_size_bytes: int = MULTIPART_CHUNK_SIZE_BYTES,
        **kwargs: Any,
    ) -> None:
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.max_retries = max_retries
        self.retry_base_delay_seconds = retry_base_delay_seconds
        self.retry_max_delay_seconds = retry_max_delay_seconds
        self.multipart_threshold_bytes = multipart_threshold_bytes
        self.multipart_chunk_size_bytes = multipart_chunk_size_bytes
        self._logger = logging.getLogger(self.__class__.__name__)

    def upload_file_with_retry(
        self,
        local_path: str,
        bucket: str,
        key: str,
        compress: bool = False,
        storage_class: str = "STANDARD",
        extra_args: Optional[dict[str, Any]] = None,
    ) -> bool:
        """
        Upload a local file to S3 with automatic retry and optional compression.

        Args:
            local_path: Path to local file to upload
            bucket: Destination S3 bucket
            key: Destination S3 key
            compress: If True, gzip compress before uploading (appends .gz to key)
            storage_class: S3 storage class
            extra_args: Additional boto3 upload arguments

        Returns:
            True if upload succeeded

        Raises:
            ClientError: After max_retries exhausted
        """
        file_size = os.path.getsize(local_path)
        upload_key = f"{key}.gz" if compress else key

        args: dict[str, Any] = {"StorageClass": storage_class}
        if extra_args:
            args.update(extra_args)

        self._logger.info(
            "Uploading %s -> s3://%s/%s (%.2f MB, compressed=%s)",
            local_path,
            bucket,
            upload_key,
            file_size / (1024 * 1024),
            compress,
        )

        for attempt in range(1, self.max_retries + 1):
            try:
                client = self.get_conn()

                if compress:
                    self._upload_compressed(client, local_path, bucket, upload_key, args)
                elif file_size >= self.multipart_threshold_bytes:
                    self._multipart_upload(client, local_path, bucket, upload_key, args)
                else:
                    client.upload_file(
                        Filename=local_path,
                        Bucket=bucket,
                        Key=upload_key,
                        ExtraArgs=args,
                    )

                self._logger.info("Upload succeeded on attempt %d: s3://%s/%s", attempt, bucket, upload_key)
                return True

            except ClientError as e:
                if attempt == self.max_retries:
                    self._logger.error(
                        "Upload failed after %d attempts: %s",
                        self.max_retries,
                        e,
                    )
                    raise
                delay = min(
                    self.retry_base_delay_seconds * (2 ** (attempt - 1)),
                    self.retry_max_delay_seconds,
                )
                self._logger.warning(
                    "Upload attempt %d failed, retrying in %.1fs: %s",
                    attempt,
                    delay,
                    e,
                )
                time.sleep(delay)

        return False

    def download_file_with_retry(
        self,
        bucket: str,
        key: str,
        local_path: str,
        decompress: bool = False,
    ) -> str:
        """
        Download an S3 object to a local path with retry logic.

        Args:
            bucket: Source S3 bucket
            key: Source S3 key
            local_path: Local destination path
            decompress: If True, decompress gzip on download

        Returns:
            Local path of downloaded file
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                client = self.get_conn()
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                if decompress and key.endswith(".gz"):
                    obj = client.get_object(Bucket=bucket, Key=key)
                    compressed_data = obj["Body"].read()
                    with open(local_path, "wb") as f:
                        f.write(gzip.decompress(compressed_data))
                else:
                    client.download_file(Bucket=bucket, Key=key, Filename=local_path)

                self._logger.info("Downloaded s3://%s/%s -> %s", bucket, key, local_path)
                return local_path

            except ClientError as e:
                if attempt == self.max_retries:
                    raise
                delay = min(
                    self.retry_base_delay_seconds * (2 ** (attempt - 1)),
                    self.retry_max_delay_seconds,
                )
                self._logger.warning("Download attempt %d failed, retrying in %.1fs: %s", attempt, delay, e)
                time.sleep(delay)

        return local_path

    def iter_keys_by_prefix(
        self,
        bucket: str,
        prefix: str,
        suffix: str = "",
        page_size: int = 1000,
    ) -> Iterator[str]:
        """
        Iterate over all S3 keys with a given prefix, yielding one at a time.

        Args:
            bucket: S3 bucket name
            prefix: Key prefix to filter
            suffix: Optional suffix filter (e.g., '.parquet')
            page_size: Number of keys per paginator page

        Yields:
            S3 key strings
        """
        client = self.get_conn()
        paginator = client.get_paginator("list_objects_v2")

        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            PaginationConfig={"PageSize": page_size},
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not suffix or key.endswith(suffix):
                    yield key

    def _upload_compressed(
        self,
        client: Any,
        local_path: str,
        bucket: str,
        key: str,
        extra_args: dict[str, Any],
    ) -> None:
        """Compress file in memory and upload."""
        with open(local_path, "rb") as f:
            data = f.read()

        compressed = gzip.compress(data, compresslevel=6)
        buf = io.BytesIO(compressed)
        extra_args_with_encoding = {**extra_args, "ContentEncoding": "gzip"}
        client.upload_fileobj(buf, bucket, key, ExtraArgs=extra_args_with_encoding)

    def _multipart_upload(
        self,
        client: Any,
        local_path: str,
        bucket: str,
        key: str,
        extra_args: dict[str, Any],
    ) -> None:
        """Perform multipart upload for large files."""
        mpu = client.create_multipart_upload(Bucket=bucket, Key=key, **extra_args)
        upload_id = mpu["UploadId"]
        parts: list[dict[str, Any]] = []

        try:
            with open(local_path, "rb") as f:
                part_number = 1
                while True:
                    chunk = f.read(self.multipart_chunk_size_bytes)
                    if not chunk:
                        break
                    response = client.upload_part(
                        Bucket=bucket,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk,
                    )
                    parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                    self._logger.debug("Uploaded part %d for %s", part_number, key)
                    part_number += 1

            client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        except Exception:
            client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            raise
