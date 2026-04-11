"""Pydantic models for ad system events."""

from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    """Valid ad event types."""

    IMPRESSION = "impression"
    CLICK = "click"
    CONVERSION = "conversion"
    BID_REQUEST = "bid_request"
    BID_RESPONSE = "bid_response"
    VIEW = "view"
    ENGAGEMENT = "engagement"


class DeviceType(str, Enum):
    """Device type classifications."""

    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"
    CTV = "ctv"
    UNKNOWN = "unknown"


class AdEvent(BaseModel):
    """Base model for all ad system events."""

    event_id: UUID = Field(..., description="Unique event identifier")
    event_type: EventType = Field(..., description="Type of ad event")
    timestamp: datetime = Field(..., description="Event occurrence timestamp (UTC)")
    user_id: Optional[str] = Field(default=None, description="Hashed user identifier")
    session_id: Optional[str] = Field(default=None, description="User session identifier")
    ad_id: str = Field(..., description="Advertisement identifier")
    campaign_id: str = Field(..., description="Campaign identifier")
    adset_id: str = Field(..., description="Ad set identifier")
    advertiser_id: str = Field(..., description="Advertiser account identifier")
    publisher_id: str = Field(..., description="Publisher/inventory source identifier")
    placement_id: str = Field(..., description="Placement identifier")
    device_type: DeviceType = Field(default=DeviceType.UNKNOWN, description="User device type")
    country_code: str = Field(default="US", description="ISO 3166-1 alpha-2 country code")
    platform: Optional[str] = Field(default=None, description="Platform identifier (ios, android, web)")
    revenue: float = Field(default=0.0, description="Revenue associated with this event")
    cost: float = Field(default=0.0, description="Cost associated with this event")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional event metadata")

    @field_validator("country_code", mode="before")
    @classmethod
    def normalize_country_code(cls, v: str) -> str:
        return v.upper()

    @field_validator("revenue", "cost", mode="before")
    @classmethod
    def validate_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("Revenue and cost must be non-negative")
        return v

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: str,
        }


class SSPEvent(AdEvent):
    """Supply-Side Platform event model."""

    floor_price: float = Field(default=0.0, description="Publisher floor price in CPM")
    winning_bid: Optional[float] = Field(default=None, description="Winning bid price in CPM")
    auction_id: str = Field(..., description="RTB auction identifier")
    deal_id: Optional[str] = Field(default=None, description="PMP deal identifier")
    inventory_type: str = Field(default="display", description="Inventory type (display, video, native)")
    viewability_score: Optional[float] = Field(default=None, ge=0.0, le=1.0, description="Viewability prediction score")

    @field_validator("floor_price", "winning_bid", mode="before")
    @classmethod
    def validate_price(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v < 0:
            raise ValueError("Price must be non-negative")
        return v


class DSPEvent(AdEvent):
    """Demand-Side Platform event model."""

    bid_price: float = Field(default=0.0, description="DSP bid price in CPM")
    clearing_price: Optional[float] = Field(default=None, description="Auction clearing price in CPM")
    targeting_data: dict[str, Any] = Field(default_factory=dict, description="Audience targeting segments used")
    frequency_cap: Optional[int] = Field(default=None, description="Frequency cap applied")
    frequency_count: int = Field(default=0, description="Current impression frequency for this user")
    budget_remaining: Optional[float] = Field(default=None, description="Remaining campaign budget")
    pacing_factor: float = Field(default=1.0, ge=0.0, le=2.0, description="Budget pacing factor")


class UserAction(BaseModel):
    """Model for user post-click/view actions and conversions."""

    action_id: UUID = Field(..., description="Unique action identifier")
    user_id: str = Field(..., description="Hashed user identifier")
    ad_id: str = Field(..., description="Advertisement that triggered this action")
    campaign_id: str = Field(..., description="Campaign identifier")
    advertiser_id: str = Field(..., description="Advertiser identifier")
    action_type: str = Field(..., description="Type of conversion action (purchase, signup, app_install, etc)")
    action_timestamp: datetime = Field(..., description="When the action occurred (UTC)")
    click_timestamp: Optional[datetime] = Field(default=None, description="Associated click timestamp")
    impression_timestamp: Optional[datetime] = Field(default=None, description="Associated impression timestamp")
    attribution_window_days: int = Field(default=7, description="Attribution window in days")
    revenue_value: float = Field(default=0.0, description="Conversion revenue value")
    currency: str = Field(default="USD", description="Revenue currency code")
    is_view_through: bool = Field(default=False, description="True if view-through conversion")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional action metadata")

    @field_validator("revenue_value", mode="before")
    @classmethod
    def validate_revenue(cls, v: float) -> float:
        if v < 0:
            raise ValueError("Revenue value must be non-negative")
        return v
