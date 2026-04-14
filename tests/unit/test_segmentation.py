"""Unit tests for UserSegmentationEngine."""

from __future__ import annotations

import pytest

from ad_data_pipeline.transformations.segmentation import (
    UserFeatures,
    UserSegmentationEngine,
)


@pytest.fixture
def engine() -> UserSegmentationEngine:
    return UserSegmentationEngine(
        lookback_days=90,
        min_events_threshold=5,
    )


@pytest.fixture
def active_user() -> UserFeatures:
    return UserFeatures(
        user_id="user_001",
        days_since_last_impression=2.0,
        days_since_last_click=5.0,
        days_since_last_conversion=10.0,
        impression_count_30d=50,
        click_count_30d=8,
        conversion_count_30d=2,
        impression_count_90d=150,
        click_count_90d=25,
        conversion_count_90d=5,
        total_spend_30d=99.0,
        total_spend_90d=250.0,
        avg_order_value=49.5,
        ctr_30d=0.16,
        cvr_30d=0.25,
    )


@pytest.fixture
def inactive_user() -> UserFeatures:
    return UserFeatures(
        user_id="user_002",
        days_since_last_impression=120.0,
        days_since_last_click=200.0,
        days_since_last_conversion=300.0,
        impression_count_30d=0,
        click_count_30d=0,
        conversion_count_30d=0,
        impression_count_90d=2,
        click_count_90d=0,
        conversion_count_90d=0,
        total_spend_30d=0.0,
        total_spend_90d=0.0,
    )


class TestUserSegmentationEngine:
    def test_compute_rfm_score_high_engagement(self, engine: UserSegmentationEngine) -> None:
        thresholds = [10.0, 30.0, 60.0, 90.0]  # recency (lower days = higher score)
        freq_thresholds = [5.0, 10.0, 20.0, 50.0]
        monetary_thresholds = [10.0, 50.0, 100.0, 200.0]

        r, f, m = engine.compute_rfm_score(
            days_since_last_event=2.0,
            event_count=100,
            total_spend=500.0,
            recency_thresholds=thresholds,
            frequency_thresholds=freq_thresholds,
            monetary_thresholds=monetary_thresholds,
        )
        # High recency (2 days ago), high frequency, high monetary -> all 5
        assert r == 5
        assert f == 5
        assert m == 5

    def test_compute_rfm_score_low_engagement(self, engine: UserSegmentationEngine) -> None:
        thresholds = [10.0, 30.0, 60.0, 90.0]
        freq_thresholds = [5.0, 10.0, 20.0, 50.0]
        monetary_thresholds = [10.0, 50.0, 100.0, 200.0]

        r, f, m = engine.compute_rfm_score(
            days_since_last_event=200.0,  # Very old
            event_count=1,
            total_spend=0.0,
            recency_thresholds=thresholds,
            frequency_thresholds=freq_thresholds,
            monetary_thresholds=monetary_thresholds,
        )
        assert r == 1
        assert f == 1
        assert m == 1

    def test_assign_rfm_tier_champions(self, engine: UserSegmentationEngine) -> None:
        tier = engine.assign_rfm_tier(recency=5, frequency=5, monetary=5)
        assert tier == "champions"

    def test_assign_rfm_tier_loyal(self, engine: UserSegmentationEngine) -> None:
        tier = engine.assign_rfm_tier(recency=3, frequency=5, monetary=5)
        assert tier == "loyal"

    def test_assign_rfm_tier_new_users(self, engine: UserSegmentationEngine) -> None:
        tier = engine.assign_rfm_tier(recency=5, frequency=1, monetary=1)
        assert tier == "new_users"

    def test_assign_rfm_tier_at_risk(self, engine: UserSegmentationEngine) -> None:
        tier = engine.assign_rfm_tier(recency=2, frequency=4, monetary=3)
        assert tier == "at_risk"

    def test_assign_rfm_tier_lost(self, engine: UserSegmentationEngine) -> None:
        tier = engine.assign_rfm_tier(recency=1, frequency=1, monetary=1)
        assert tier == "lost"

    def test_compute_engagement_score_high(self, engine: UserSegmentationEngine) -> None:
        score = engine.compute_engagement_score(
            ctr=0.10,           # Max CTR
            cvr=0.05,           # Max CVR
            impression_count=1000,   # Max volume
            days_active=0,      # Just started (max recency)
        )
        assert score == pytest.approx(1.0)

    def test_compute_engagement_score_low(self, engine: UserSegmentationEngine) -> None:
        score = engine.compute_engagement_score(
            ctr=0.0,
            cvr=0.0,
            impression_count=0,
            days_active=90,
        )
        assert score == pytest.approx(0.0)

    def test_compute_engagement_score_normalized(self, engine: UserSegmentationEngine) -> None:
        score = engine.compute_engagement_score(
            ctr=0.05,
            cvr=0.025,
            impression_count=500,
            days_active=45,
        )
        assert 0.0 <= score <= 1.0

    def test_filter_qualified_users_filters_below_threshold(
        self, engine: UserSegmentationEngine, active_user: UserFeatures, inactive_user: UserFeatures
    ) -> None:
        # inactive_user has impression_count_90d=2, below min_events_threshold=5
        qualified = engine.filter_qualified_users([active_user, inactive_user])
        assert len(qualified) == 1
        assert qualified[0].user_id == "user_001"

    def test_filter_qualified_users_all_pass(
        self, engine: UserSegmentationEngine, active_user: UserFeatures
    ) -> None:
        qualified = engine.filter_qualified_users([active_user])
        assert len(qualified) == 1

    def test_user_features_rfm_score_string(self, active_user: UserFeatures) -> None:
        active_user.recency_score = 4
        active_user.frequency_score = 3
        active_user.monetary_score = 5
        assert active_user.rfm_score == "435"
        assert active_user.rfm_total == 12
