"""Unit tests for RankingFeatureComputer."""

from __future__ import annotations

import math

import pytest

from ad_data_pipeline.transformations.ranking_features import (
    AdRankingFeatures,
    RankingFeatureComputer,
)


@pytest.fixture
def computer() -> RankingFeatureComputer:
    return RankingFeatureComputer(
        decay_half_life_hours=6.0,
        laplace_smoothing_alpha=1.0,
        min_impressions_for_rate=10,
        prior_ctr=0.001,
        prior_cvr=0.0001,
    )


@pytest.fixture
def sample_features() -> AdRankingFeatures:
    return AdRankingFeatures(
        ad_id="ad_001",
        campaign_id="camp_001",
        adset_id="adset_001",
        advertiser_id="adv_001",
        impressions_1h=1000,
        impressions_6h=5000,
        impressions_24h=20000,
        clicks_1h=15,
        clicks_6h=80,
        clicks_24h=300,
        conversions_1h=2,
        conversions_6h=10,
        conversions_24h=40,
        revenue_1h=100.0,
        revenue_24h=400.0,
        ctr_1h=0.015,
        ctr_6h=0.016,
        ctr_24h=0.015,
        cvr_1h=0.133,
        cvr_6h=0.125,
        cvr_24h=0.133,
        ecpm_1h=100.0,
        ecpm_24h=20.0,
        ctr_decayed=0.015,
        cvr_decayed=0.13,
        ecpm_decayed=50.0,
        budget_utilization=0.65,
        pacing_factor=1.0,
    )


class TestRankingFeatureComputer:
    def test_compute_decayed_metric_zero_age(self, computer: RankingFeatureComputer) -> None:
        """A metric with age=0 should have full weight."""
        result = computer.compute_decayed_metric([(0.5, 0.0)])
        assert result == pytest.approx(0.5)

    def test_compute_decayed_metric_old_data_discounted(
        self, computer: RankingFeatureComputer
    ) -> None:
        """Older data should receive less weight than recent data."""
        # Both inputs are 1.0, but old_only has more decay -> still 1.0 (same value, different weight)
        # Test that a blend of old and new is between the two extremes
        blended = computer.compute_decayed_metric([(0.0, 0.0), (1.0, 24.0)])
        assert 0.0 < blended < 1.0

    def test_compute_decayed_metric_empty_returns_zero(
        self, computer: RankingFeatureComputer
    ) -> None:
        assert computer.compute_decayed_metric([]) == 0.0

    def test_compute_smoothed_ctr_above_min_impressions(
        self, computer: RankingFeatureComputer
    ) -> None:
        ctr = computer.compute_smoothed_ctr(clicks=20, impressions=1000)
        expected = (20 + 1.0 * 0.001) / (1000 + 1.0)
        assert ctr == pytest.approx(expected)

    def test_compute_smoothed_ctr_below_min_impressions(
        self, computer: RankingFeatureComputer
    ) -> None:
        # Below min_impressions_for_rate (10), should return prior
        ctr = computer.compute_smoothed_ctr(clicks=1, impressions=5)
        assert ctr == computer.prior_ctr

    def test_compute_smoothed_ctr_zero_impressions(
        self, computer: RankingFeatureComputer
    ) -> None:
        ctr = computer.compute_smoothed_ctr(clicks=0, impressions=0)
        assert ctr == computer.prior_ctr

    def test_compute_smoothed_cvr_above_min_clicks(
        self, computer: RankingFeatureComputer
    ) -> None:
        cvr = computer.compute_smoothed_cvr(conversions=5, clicks=100)
        expected = (5 + 1.0 * 0.0001) / (100 + 1.0)
        assert cvr == pytest.approx(expected)

    def test_compute_ecpm_normal(self, computer: RankingFeatureComputer) -> None:
        ecpm = computer.compute_ecpm(revenue=10.0, impressions=5000)
        assert ecpm == pytest.approx(2.0)

    def test_compute_ecpm_zero_impressions(self, computer: RankingFeatureComputer) -> None:
        assert computer.compute_ecpm(revenue=10.0, impressions=0) == 0.0

    def test_build_feature_vector_correct_length(
        self, computer: RankingFeatureComputer, sample_features: AdRankingFeatures
    ) -> None:
        vector = computer.build_feature_vector(sample_features)
        assert len(vector.feature_names) == len(RankingFeatureComputer.FEATURE_NAMES)
        assert len(vector.feature_values) == len(RankingFeatureComputer.FEATURE_NAMES)

    def test_build_feature_vector_log_transform(
        self, computer: RankingFeatureComputer, sample_features: AdRankingFeatures
    ) -> None:
        vector = computer.build_feature_vector(sample_features)
        fdict = vector.to_dict()
        expected_log = math.log1p(sample_features.impressions_24h)
        assert fdict["impressions_24h_log"] == pytest.approx(expected_log)

    def test_validate_features_clean(
        self, computer: RankingFeatureComputer, sample_features: AdRankingFeatures
    ) -> None:
        warnings = computer.validate_features(sample_features)
        assert len(warnings) == 0

    def test_validate_features_high_ctr(
        self, computer: RankingFeatureComputer, sample_features: AdRankingFeatures
    ) -> None:
        sample_features.ctr_24h = 0.75  # Suspiciously high
        warnings = computer.validate_features(sample_features)
        assert any("CTR" in w for w in warnings)

    def test_validate_features_impossible_cvr(
        self, computer: RankingFeatureComputer, sample_features: AdRankingFeatures
    ) -> None:
        sample_features.cvr_24h = 1.5  # Impossible
        warnings = computer.validate_features(sample_features)
        assert any("CVR" in w for w in warnings)

    def test_validate_features_clicks_without_impressions(
        self, computer: RankingFeatureComputer, sample_features: AdRankingFeatures
    ) -> None:
        sample_features.impressions_24h = 0
        sample_features.clicks_24h = 5
        warnings = computer.validate_features(sample_features)
        assert any("impressions" in w.lower() for w in warnings)

    def test_decay_lambda_computation(self, computer: RankingFeatureComputer) -> None:
        expected_lambda = math.log(2) / 6.0
        assert computer.decay_lambda == pytest.approx(expected_lambda)
