"""Unit tests for AdvertiserReportBuilder."""

from __future__ import annotations

from datetime import date

import pytest

from ad_data_pipeline.transformations.report_builder import (
    AdvertiserReportBuilder,
    RawCampaignRow,
    ReportValidationResult,
)


@pytest.fixture
def builder() -> AdvertiserReportBuilder:
    return AdvertiserReportBuilder(
        currency="USD",
        min_impressions_for_rate_reporting=100,
    )


@pytest.fixture
def sample_rows() -> list[RawCampaignRow]:
    report_date = date(2024, 1, 15)
    return [
        RawCampaignRow(
            advertiser_id="adv_001",
            campaign_id="camp_001",
            campaign_name="Campaign Alpha",
            adset_id="adset_001",
            adset_name="AdSet A",
            ad_id="ad_001",
            ad_name="Ad Creative 1",
            report_date=report_date,
            impressions=10000,
            clicks=150,
            conversions=5,
            views=0,
            spend=50.0,
            revenue=250.0,
        ),
        RawCampaignRow(
            advertiser_id="adv_001",
            campaign_id="camp_001",
            campaign_name="Campaign Alpha",
            adset_id="adset_002",
            adset_name="AdSet B",
            ad_id="ad_002",
            ad_name="Ad Creative 2",
            report_date=report_date,
            impressions=5000,
            clicks=75,
            conversions=3,
            views=0,
            spend=25.0,
            revenue=120.0,
        ),
    ]


class TestAdvertiserReportBuilder:
    def test_aggregate_to_campaign_level_single_campaign(
        self, builder: AdvertiserReportBuilder, sample_rows: list[RawCampaignRow]
    ) -> None:
        result = builder.aggregate_to_campaign_level(sample_rows)
        assert "camp_001" in result
        agg = result["camp_001"]
        assert agg["impressions"] == 15000
        assert agg["clicks"] == 225
        assert agg["conversions"] == 8

    def test_aggregate_to_campaign_level_multiple_campaigns(
        self, builder: AdvertiserReportBuilder
    ) -> None:
        rows = [
            RawCampaignRow(
                advertiser_id="adv_001",
                campaign_id="camp_001",
                campaign_name="Camp 1",
                adset_id="adset_001",
                adset_name="AdSet 1",
                ad_id="ad_001",
                ad_name="Ad 1",
                report_date=date(2024, 1, 15),
                impressions=1000,
                clicks=10,
                spend=5.0,
                revenue=20.0,
            ),
            RawCampaignRow(
                advertiser_id="adv_001",
                campaign_id="camp_002",
                campaign_name="Camp 2",
                adset_id="adset_002",
                adset_name="AdSet 2",
                ad_id="ad_002",
                ad_name="Ad 2",
                report_date=date(2024, 1, 15),
                impressions=2000,
                clicks=20,
                spend=10.0,
                revenue=40.0,
            ),
        ]
        result = builder.aggregate_to_campaign_level(rows)
        assert len(result) == 2
        assert "camp_001" in result
        assert "camp_002" in result

    def test_compute_kpis_correct_ctr(self, builder: AdvertiserReportBuilder) -> None:
        kpis = builder.compute_kpis(
            impressions=10000,
            clicks=200,
            conversions=10,
            spend=100.0,
            revenue=500.0,
        )
        assert kpis["ctr"] == pytest.approx(0.02)

    def test_compute_kpis_correct_cvr(self, builder: AdvertiserReportBuilder) -> None:
        kpis = builder.compute_kpis(
            impressions=10000,
            clicks=200,
            conversions=10,
            spend=100.0,
            revenue=500.0,
        )
        assert kpis["cvr"] == pytest.approx(0.05)

    def test_compute_kpis_correct_roas(self, builder: AdvertiserReportBuilder) -> None:
        kpis = builder.compute_kpis(
            impressions=10000,
            clicks=200,
            conversions=10,
            spend=100.0,
            revenue=500.0,
        )
        assert kpis["roas"] == pytest.approx(5.0)

    def test_compute_kpis_zero_impressions(self, builder: AdvertiserReportBuilder) -> None:
        kpis = builder.compute_kpis(
            impressions=0,
            clicks=0,
            conversions=0,
            spend=0.0,
            revenue=0.0,
        )
        assert kpis["ctr"] == 0.0
        assert kpis["cpm"] == 0.0
        assert kpis["roas"] == 0.0

    def test_validate_report_passes_for_valid_data(
        self, builder: AdvertiserReportBuilder
    ) -> None:
        result = builder.validate_report(
            advertiser_id="adv_001",
            total_impressions=100000,
            total_clicks=1000,
            total_conversions=50,
            total_spend=500.0,
            campaign_count=3,
        )
        assert result.is_valid is True
        assert result.errors == []

    def test_validate_report_fails_for_no_campaigns(
        self, builder: AdvertiserReportBuilder
    ) -> None:
        result = builder.validate_report(
            advertiser_id="adv_001",
            total_impressions=0,
            total_clicks=0,
            total_conversions=0,
            total_spend=0.0,
            campaign_count=0,
        )
        assert result.is_valid is False
        assert len(result.errors) >= 1

    def test_validate_report_fails_for_negative_spend(
        self, builder: AdvertiserReportBuilder
    ) -> None:
        result = builder.validate_report(
            advertiser_id="adv_001",
            total_impressions=1000,
            total_clicks=10,
            total_conversions=1,
            total_spend=-50.0,
            campaign_count=1,
        )
        assert result.is_valid is False

    def test_validate_report_fails_when_clicks_exceed_impressions(
        self, builder: AdvertiserReportBuilder
    ) -> None:
        result = builder.validate_report(
            advertiser_id="adv_001",
            total_impressions=100,
            total_clicks=200,  # More clicks than impressions
            total_conversions=5,
            total_spend=10.0,
            campaign_count=1,
        )
        assert result.is_valid is False

    def test_validate_report_warns_on_high_ctr(
        self, builder: AdvertiserReportBuilder
    ) -> None:
        result = builder.validate_report(
            advertiser_id="adv_001",
            total_impressions=1000,
            total_clicks=600,  # 60% CTR - suspicious
            total_conversions=10,
            total_spend=30.0,
            campaign_count=1,
        )
        assert len(result.warnings) >= 1

    def test_build_report_metadata(self, builder: AdvertiserReportBuilder) -> None:
        metadata = builder.build_report_metadata(
            advertiser_id="adv_001",
            report_date=date(2024, 1, 15),
            campaign_count=3,
            total_impressions=100000,
            total_spend=500.0,
        )
        assert metadata["advertiser_id"] == "adv_001"
        assert metadata["campaign_count"] == 3
        assert metadata["currency"] == "USD"
        assert metadata["is_final"] is True
