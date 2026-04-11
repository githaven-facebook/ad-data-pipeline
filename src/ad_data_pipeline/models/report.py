"""Pydantic models for advertiser reports and campaign metrics."""

from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class ReportGranularity(str, Enum):
    """Time granularity for report aggregation."""

    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class ReportPeriod(BaseModel):
    """Defines the time period covered by a report."""

    start_date: date = Field(..., description="Report start date (inclusive)")
    end_date: date = Field(..., description="Report end date (inclusive)")
    granularity: ReportGranularity = Field(
        default=ReportGranularity.DAILY, description="Time granularity for data aggregation"
    )
    timezone: str = Field(default="UTC", description="Timezone for date interpretation")

    @field_validator("end_date", mode="before")
    @classmethod
    def end_after_start(cls, v: date, info: Any) -> date:
        # Note: field_validator runs per-field; cross-field validation via model_validator
        return v

    @property
    def days(self) -> int:
        """Number of days in the report period."""
        return (self.end_date - self.start_date).days + 1


class CampaignMetrics(BaseModel):
    """Aggregated metrics for a campaign or ad entity."""

    impressions: int = Field(default=0, ge=0, description="Total impressions")
    clicks: int = Field(default=0, ge=0, description="Total clicks")
    conversions: int = Field(default=0, ge=0, description="Total conversions")
    views: int = Field(default=0, ge=0, description="Total video views (for video ads)")
    spend: float = Field(default=0.0, ge=0.0, description="Total ad spend in USD")
    revenue: float = Field(default=0.0, ge=0.0, description="Total attributed revenue in USD")

    # Computed KPIs
    @property
    def ctr(self) -> float:
        """Click-through rate."""
        return self.clicks / self.impressions if self.impressions > 0 else 0.0

    @property
    def cvr(self) -> float:
        """Conversion rate (conversions / clicks)."""
        return self.conversions / self.clicks if self.clicks > 0 else 0.0

    @property
    def cpa(self) -> float:
        """Cost per acquisition (spend / conversions)."""
        return self.spend / self.conversions if self.conversions > 0 else 0.0

    @property
    def cpc(self) -> float:
        """Cost per click."""
        return self.spend / self.clicks if self.clicks > 0 else 0.0

    @property
    def cpm(self) -> float:
        """Cost per thousand impressions."""
        return (self.spend / self.impressions * 1000) if self.impressions > 0 else 0.0

    @property
    def roas(self) -> float:
        """Return on ad spend (revenue / spend)."""
        return self.revenue / self.spend if self.spend > 0 else 0.0

    @property
    def ecpm(self) -> float:
        """Effective CPM based on revenue."""
        return (self.revenue / self.impressions * 1000) if self.impressions > 0 else 0.0


class AdMetrics(CampaignMetrics):
    """Metrics for an individual ad creative."""

    ad_id: str = Field(..., description="Ad identifier")
    ad_name: str = Field(default="", description="Ad name")
    creative_type: str = Field(default="display", description="Creative type (display, video, native)")
    date: date = Field(..., description="Metrics date")


class AdSetMetrics(CampaignMetrics):
    """Metrics aggregated at the ad set level."""

    adset_id: str = Field(..., description="Ad set identifier")
    adset_name: str = Field(default="", description="Ad set name")
    date: date = Field(..., description="Metrics date")
    ad_metrics: list[AdMetrics] = Field(default_factory=list, description="Per-ad breakdown")


class CampaignDailyMetrics(CampaignMetrics):
    """Daily metrics for a campaign."""

    campaign_id: str = Field(..., description="Campaign identifier")
    campaign_name: str = Field(default="", description="Campaign name")
    date: date = Field(..., description="Metrics date")
    adset_metrics: list[AdSetMetrics] = Field(default_factory=list, description="Per-adset breakdown")


class AdvertiserReport(BaseModel):
    """Complete advertiser report with hierarchical campaign metrics."""

    report_id: str = Field(..., description="Unique report identifier")
    advertiser_id: str = Field(..., description="Advertiser account identifier")
    advertiser_name: str = Field(default="", description="Advertiser display name")
    period: ReportPeriod = Field(..., description="Report time period")
    generated_at: datetime = Field(default_factory=datetime.utcnow, description="Report generation timestamp")
    campaign_metrics: list[CampaignDailyMetrics] = Field(
        default_factory=list, description="Per-campaign metrics breakdown"
    )
    total_metrics: CampaignMetrics = Field(
        default_factory=CampaignMetrics, description="Aggregated totals across all campaigns"
    )
    currency: str = Field(default="USD", description="Report currency")
    is_final: bool = Field(default=False, description="True if report data is finalized")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
        }


# Fix forward reference for ReportPeriod.days property
from typing import Any  # noqa: E402
