"""Build advertiser reports: aggregate campaign hierarchy and compute KPIs."""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime

logger = logging.getLogger(__name__)


@dataclass
class RawCampaignRow:
    """A raw aggregated row from the events table."""

    advertiser_id: str
    campaign_id: str
    campaign_name: str
    adset_id: str
    adset_name: str
    ad_id: str
    ad_name: str
    report_date: date
    impressions: int = 0
    clicks: int = 0
    conversions: int = 0
    views: int = 0
    spend: float = 0.0
    revenue: float = 0.0


@dataclass
class ReportValidationResult:
    """Result of validating a generated report."""

    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class AdvertiserReportBuilder:
    """
    Builds structured advertiser reports from raw aggregated ad event data.

    Aggregation hierarchy: Advertiser -> Campaign -> AdSet -> Ad

    KPIs computed:
    - CTR (Click-Through Rate)
    - CVR (Conversion Rate)
    - CPA (Cost Per Acquisition)
    - CPC (Cost Per Click)
    - CPM (Cost Per Mille)
    - ROAS (Return On Ad Spend)
    - eCPM (Effective CPM based on revenue)
    """

    def __init__(
        self,
        currency: str = "USD",
        min_impressions_for_rate_reporting: int = 100,
        max_ctr_threshold: float = 0.50,
        max_cvr_threshold: float = 1.0,
    ) -> None:
        self.currency = currency
        self.min_impressions_for_rate_reporting = min_impressions_for_rate_reporting
        self.max_ctr_threshold = max_ctr_threshold
        self.max_cvr_threshold = max_cvr_threshold
        self._logger = logging.getLogger(self.__class__.__name__)

    def aggregate_to_campaign_level(
        self,
        rows: list[RawCampaignRow],
    ) -> dict[str, dict[str, int | float | str]]:
        """
        Aggregate raw rows up to campaign level.

        Args:
            rows: Raw event rows grouped by campaign/adset/ad/date

        Returns:
            Dict keyed by campaign_id with aggregated metrics
        """
        campaign_agg: dict[str, dict[str, int | float | str]] = {}

        for row in rows:
            cid = row.campaign_id
            if cid not in campaign_agg:
                campaign_agg[cid] = {
                    "campaign_id": cid,
                    "campaign_name": row.campaign_name,
                    "advertiser_id": row.advertiser_id,
                    "impressions": 0,
                    "clicks": 0,
                    "conversions": 0,
                    "views": 0,
                    "spend": 0.0,
                    "revenue": 0.0,
                }
            agg = campaign_agg[cid]
            agg["impressions"] = int(agg["impressions"]) + row.impressions
            agg["clicks"] = int(agg["clicks"]) + row.clicks
            agg["conversions"] = int(agg["conversions"]) + row.conversions
            agg["views"] = int(agg["views"]) + row.views
            agg["spend"] = float(agg["spend"]) + row.spend
            agg["revenue"] = float(agg["revenue"]) + row.revenue

        return campaign_agg

    def compute_kpis(
        self,
        impressions: int,
        clicks: int,
        conversions: int,
        spend: float,
        revenue: float,
    ) -> dict[str, float]:
        """
        Compute all KPIs from raw counts.

        Args:
            impressions: Total impressions
            clicks: Total clicks
            conversions: Total conversions
            spend: Total spend in USD
            revenue: Total attributed revenue in USD

        Returns:
            Dict of KPI name -> float value
        """
        ctr = clicks / impressions if impressions > 0 else 0.0
        cvr = conversions / clicks if clicks > 0 else 0.0
        cpa = spend / conversions if conversions > 0 else 0.0
        cpc = spend / clicks if clicks > 0 else 0.0
        cpm = (spend / impressions * 1000) if impressions > 0 else 0.0
        roas = revenue / spend if spend > 0 else 0.0
        ecpm = (revenue / impressions * 1000) if impressions > 0 else 0.0

        return {
            "ctr": ctr,
            "cvr": cvr,
            "cpa": cpa,
            "cpc": cpc,
            "cpm": cpm,
            "roas": roas,
            "ecpm": ecpm,
        }

    def validate_report(
        self,
        advertiser_id: str,
        total_impressions: int,
        total_clicks: int,
        total_conversions: int,
        total_spend: float,
        campaign_count: int,
    ) -> ReportValidationResult:
        """
        Validate a generated report for data quality issues.

        Checks:
        - CTR is within acceptable bounds
        - Conversions do not exceed clicks
        - Spend is non-negative
        - At least one campaign present
        """
        errors: list[str] = []
        warnings: list[str] = []

        if campaign_count == 0:
            errors.append(f"Report for advertiser {advertiser_id} has no campaigns")

        if total_spend < 0:
            errors.append(f"Negative spend detected: {total_spend}")

        if total_impressions > 0:
            ctr = total_clicks / total_impressions
            if ctr > self.max_ctr_threshold:
                warnings.append(f"Unusually high CTR: {ctr:.2%} - possible click fraud")

        if total_clicks > 0:
            cvr = total_conversions / total_clicks
            if cvr > self.max_cvr_threshold:
                errors.append(
                    f"CVR {cvr:.2%} exceeds maximum {self.max_cvr_threshold:.2%} - "
                    "conversions cannot exceed clicks"
                )

        if total_clicks > total_impressions:
            errors.append(
                f"Clicks ({total_clicks}) exceed impressions ({total_impressions}) - data integrity issue"
            )

        return ReportValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    def build_report_metadata(
        self,
        advertiser_id: str,
        report_date: date,
        campaign_count: int,
        total_impressions: int,
        total_spend: float,
    ) -> dict[str, str | int | float | bool]:
        """Build report metadata dict for storage."""
        return {
            "advertiser_id": advertiser_id,
            "report_date": report_date.isoformat(),
            "generated_at": datetime.utcnow().isoformat(),
            "campaign_count": campaign_count,
            "total_impressions": total_impressions,
            "total_spend": total_spend,
            "currency": self.currency,
            "is_final": True,
        }
