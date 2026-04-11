"""Pydantic models for user segmentation."""

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class SegmentOperator(str, Enum):
    """Comparison operators for segment rules."""

    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    GREATER_THAN_OR_EQUAL = "gte"
    LESS_THAN_OR_EQUAL = "lte"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    BETWEEN = "between"


class SegmentLogic(str, Enum):
    """Logic operators for combining segment criteria."""

    AND = "AND"
    OR = "OR"


class RFMTier(str, Enum):
    """RFM (Recency, Frequency, Monetary) tier classifications."""

    CHAMPIONS = "champions"
    LOYAL = "loyal"
    POTENTIAL_LOYAL = "potential_loyal"
    NEW_USERS = "new_users"
    AT_RISK = "at_risk"
    CANT_LOSE = "cant_lose"
    HIBERNATING = "hibernating"
    LOST = "lost"


class SegmentRule(BaseModel):
    """A single rule within a segment criteria."""

    field: str = Field(..., description="Field name to evaluate (e.g., 'click_count', 'country_code')")
    operator: SegmentOperator = Field(..., description="Comparison operator")
    value: Any = Field(..., description="Value to compare against")
    value_end: Optional[Any] = Field(default=None, description="End value for BETWEEN operator")

    @field_validator("value_end", mode="before")
    @classmethod
    def validate_between_has_end_value(cls, v: Optional[Any], info: Any) -> Optional[Any]:
        # value_end is only required when operator is BETWEEN
        return v


class SegmentCriteria(BaseModel):
    """A set of rules combined with AND/OR logic."""

    logic: SegmentLogic = Field(default=SegmentLogic.AND, description="How to combine the rules")
    rules: list[SegmentRule] = Field(default_factory=list, description="List of segment rules")
    nested_criteria: list["SegmentCriteria"] = Field(
        default_factory=list, description="Nested criteria groups"
    )

    def is_empty(self) -> bool:
        """Return True if no rules or nested criteria are defined."""
        return len(self.rules) == 0 and len(self.nested_criteria) == 0


class UserSegment(BaseModel):
    """Represents a user segment with its definition and metadata."""

    segment_id: str = Field(..., description="Unique segment identifier")
    segment_name: str = Field(..., description="Human-readable segment name")
    description: Optional[str] = Field(default=None, description="Segment description")
    criteria: SegmentCriteria = Field(..., description="Segment membership criteria")
    rfm_tier: Optional[RFMTier] = Field(default=None, description="RFM tier assignment")
    user_count: int = Field(default=0, ge=0, description="Number of users in this segment")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Segment creation timestamp")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last update timestamp")
    is_active: bool = Field(default=True, description="Whether segment is currently active")
    lookback_days: int = Field(default=90, ge=1, description="Lookback period in days for segment computation")
    tags: list[str] = Field(default_factory=list, description="Classification tags for the segment")
    feature_vector: Optional[dict[str, float]] = Field(
        default=None, description="Aggregated feature vector for ML use"
    )

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }


class UserSegmentMembership(BaseModel):
    """Records a user's membership in a segment."""

    user_id: str = Field(..., description="Hashed user identifier")
    segment_id: str = Field(..., description="Segment identifier")
    score: float = Field(default=1.0, ge=0.0, le=1.0, description="Membership score (0=low, 1=high)")
    computed_at: datetime = Field(default_factory=datetime.utcnow, description="When membership was computed")
    expires_at: Optional[datetime] = Field(default=None, description="When membership expires")
    features: dict[str, float] = Field(
        default_factory=dict, description="Feature values that determined membership"
    )
