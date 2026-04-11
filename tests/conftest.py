"""Shared test fixtures for ad data pipeline test suite."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Generator
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Event fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_ad_event_data() -> dict[str, Any]:
    """Return a valid AdEvent data dict."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "impression",
        "timestamp": datetime(2024, 1, 15, 12, 0, 0),
        "user_id": "user_abc123",
        "session_id": "sess_xyz789",
        "ad_id": "ad_001",
        "campaign_id": "camp_001",
        "adset_id": "adset_001",
        "advertiser_id": "adv_001",
        "publisher_id": "pub_001",
        "placement_id": "place_001",
        "device_type": "mobile",
        "country_code": "US",
        "platform": "ios",
        "revenue": 0.0015,
        "cost": 0.001,
        "metadata": {},
    }


@pytest.fixture
def sample_ssp_event_data(sample_ad_event_data: dict[str, Any]) -> dict[str, Any]:
    """Return a valid SSPEvent data dict."""
    return {
        **sample_ad_event_data,
        "event_type": "impression",
        "floor_price": 0.5,
        "winning_bid": 1.2,
        "auction_id": "auction_abc",
        "deal_id": None,
        "inventory_type": "display",
        "viewability_score": 0.85,
    }


@pytest.fixture
def sample_user_action_data() -> dict[str, Any]:
    """Return a valid UserAction data dict."""
    return {
        "action_id": str(uuid.uuid4()),
        "user_id": "user_abc123",
        "ad_id": "ad_001",
        "campaign_id": "camp_001",
        "advertiser_id": "adv_001",
        "action_type": "purchase",
        "action_timestamp": datetime(2024, 1, 15, 13, 0, 0),
        "click_timestamp": datetime(2024, 1, 15, 12, 30, 0),
        "impression_timestamp": datetime(2024, 1, 15, 12, 0, 0),
        "attribution_window_days": 7,
        "revenue_value": 49.99,
        "currency": "USD",
        "is_view_through": False,
        "metadata": {},
    }


# ---------------------------------------------------------------------------
# Mock S3 fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_s3_client() -> Generator[MagicMock, None, None]:
    """Provide a mock boto3 S3 client."""
    with patch("boto3.client") as mock_client_factory:
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {
                    "Key": "events/year=2024/month=01/day=15/hour=00/event_type=impression/part-0001.parquet",
                    "Size": 10 * 1024 * 1024,  # 10 MB
                    "LastModified": datetime(2024, 1, 15, 1, 30, 0, tzinfo=timezone.utc),
                    "StorageClass": "STANDARD",
                    "ETag": '"abc123"',
                },
                {
                    "Key": "events/year=2024/month=01/day=15/hour=00/event_type=impression/part-0002.parquet",
                    "Size": 8 * 1024 * 1024,  # 8 MB
                    "LastModified": datetime(2024, 1, 15, 1, 35, 0, tzinfo=timezone.utc),
                    "StorageClass": "STANDARD",
                    "ETag": '"def456"',
                },
            ],
            "KeyCount": 2,
        }
        mock_client.get_paginator.return_value = MagicMock(
            paginate=MagicMock(
                return_value=[
                    {
                        "Contents": [
                            {
                                "Key": "events/year=2024/month=01/day=15/data.parquet",
                                "Size": 50 * 1024 * 1024,
                                "LastModified": datetime(2024, 1, 15, 2, 0, 0, tzinfo=timezone.utc),
                                "StorageClass": "STANDARD",
                                "ETag": '"ghi789"',
                            }
                        ],
                        "CommonPrefixes": [],
                    }
                ]
            )
        )
        yield mock_client


@pytest.fixture
def mock_s3_resource() -> Generator[MagicMock, None, None]:
    """Provide a mock boto3 S3 resource."""
    with patch("boto3.resource") as mock_resource_factory:
        mock_resource = MagicMock()
        mock_resource_factory.return_value = mock_resource
        yield mock_resource


# ---------------------------------------------------------------------------
# Settings fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def test_settings() -> Any:
    """Return Settings with test-safe defaults."""
    from ad_data_pipeline.config.settings import Settings

    return Settings(
        aws_region="us-east-1",
        raw_events_bucket="test-raw-events",
        processed_events_bucket="test-processed-events",
        cold_storage_bucket="test-cold-storage",
        analytics_bucket="test-analytics",
        feature_store_bucket="test-feature-store",
        hot_data_retention_days=30,
        cold_data_retention_days=365,
    )
