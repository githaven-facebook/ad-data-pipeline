"""User segmentation logic: compute user features and assign segments."""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class UserFeatures:
    """Computed feature vector for a single user."""

    user_id: str
    # Recency features
    days_since_last_impression: float = 0.0
    days_since_last_click: float = 0.0
    days_since_last_conversion: float = 0.0
    # Frequency features (last 30 days)
    impression_count_30d: int = 0
    click_count_30d: int = 0
    conversion_count_30d: int = 0
    # Frequency features (last 90 days)
    impression_count_90d: int = 0
    click_count_90d: int = 0
    conversion_count_90d: int = 0
    # Monetary features
    total_spend_30d: float = 0.0
    total_spend_90d: float = 0.0
    avg_order_value: float = 0.0
    # Engagement features
    ctr_30d: float = 0.0
    cvr_30d: float = 0.0
    engagement_score: float = 0.0
    # RFM scores (1-5 scale)
    recency_score: int = 0
    frequency_score: int = 0
    monetary_score: int = 0
    computed_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def rfm_score(self) -> str:
        """Combined RFM score string (e.g., '555' for champion)."""
        return f"{self.recency_score}{self.frequency_score}{self.monetary_score}"

    @property
    def rfm_total(self) -> int:
        """Sum of RFM scores."""
        return self.recency_score + self.frequency_score + self.monetary_score


class UserSegmentationEngine:
    """
    Computes user features and assigns users to segments using rule-based and RFM approach.

    Pipeline:
    1. Aggregate raw ad events into per-user feature vectors
    2. Compute RFM (Recency, Frequency, Monetary) scores
    3. Assign users to segments based on RFM thresholds
    4. Apply additional rule-based filters for specialized segments
    """

    # RFM score thresholds for tier assignment
    RFM_TIER_THRESHOLDS: dict[str, tuple[int, int]] = {
        "champions": (13, 15),
        "loyal": (10, 12),
        "potential_loyal": (7, 9),
        "new_users": (0, 6),
    }

    def __init__(
        self,
        lookback_days: int = 90,
        min_events_threshold: int = 5,
        recency_quantiles: Optional[list[float]] = None,
        frequency_quantiles: Optional[list[float]] = None,
        monetary_quantiles: Optional[list[float]] = None,
    ) -> None:
        self.lookback_days = lookback_days
        self.min_events_threshold = min_events_threshold
        self.recency_quantiles = recency_quantiles or [0.2, 0.4, 0.6, 0.8]
        self.frequency_quantiles = frequency_quantiles or [0.2, 0.4, 0.6, 0.8]
        self.monetary_quantiles = monetary_quantiles or [0.2, 0.4, 0.6, 0.8]
        self._logger = logging.getLogger(self.__class__.__name__)

    def compute_rfm_score(
        self,
        days_since_last_event: float,
        event_count: int,
        total_spend: float,
        recency_thresholds: list[float],
        frequency_thresholds: list[float],
        monetary_thresholds: list[float],
    ) -> tuple[int, int, int]:
        """
        Compute RFM scores (1-5) for a user.

        Args:
            days_since_last_event: Days since last conversion event
            event_count: Total event count in lookback window
            total_spend: Total revenue attributed in lookback window
            recency_thresholds: Quantile thresholds for recency (ascending = better)
            frequency_thresholds: Quantile thresholds for frequency
            monetary_thresholds: Quantile thresholds for monetary value

        Returns:
            Tuple of (recency_score, frequency_score, monetary_score) each 1-5
        """
        # Recency: lower days_since = higher score
        recency_score = self._score_metric_inverse(days_since_last_event, recency_thresholds)
        frequency_score = self._score_metric(event_count, frequency_thresholds)
        monetary_score = self._score_metric(total_spend, monetary_thresholds)

        return recency_score, frequency_score, monetary_score

    def assign_rfm_tier(self, recency: int, frequency: int, monetary: int) -> str:
        """
        Assign RFM tier label based on combined score.

        Args:
            recency: Recency score (1-5)
            frequency: Frequency score (1-5)
            monetary: Monetary score (1-5)

        Returns:
            Tier label string
        """
        if recency >= 4 and frequency >= 4 and monetary >= 4:
            return "champions"
        elif frequency >= 4 and monetary >= 4:
            return "loyal"
        elif recency >= 3 and frequency >= 3:
            return "potential_loyal"
        elif recency >= 4 and frequency <= 2:
            return "new_users"
        elif recency <= 2 and frequency >= 3:
            return "at_risk"
        elif recency <= 2 and frequency >= 4 and monetary >= 4:
            return "cant_lose"
        elif recency <= 2 and frequency <= 2 and monetary <= 2:
            return "lost"
        else:
            return "hibernating"

    def compute_engagement_score(
        self,
        ctr: float,
        cvr: float,
        impression_count: int,
        days_active: int,
        max_impression_count: int = 1000,
        max_days_active: int = 90,
    ) -> float:
        """
        Compute a composite engagement score normalized to [0, 1].

        Weights: CTR (30%), CVR (30%), Impression volume (20%), Recency (20%)
        """
        # Normalize each component to [0, 1]
        ctr_norm = min(ctr / 0.10, 1.0)  # 10% CTR = max score
        cvr_norm = min(cvr / 0.05, 1.0)  # 5% CVR = max score
        volume_norm = min(impression_count / max_impression_count, 1.0)
        recency_norm = max(0.0, 1.0 - (days_active / max_days_active))

        return 0.3 * ctr_norm + 0.3 * cvr_norm + 0.2 * volume_norm + 0.2 * recency_norm

    def filter_qualified_users(
        self,
        user_features: list[UserFeatures],
    ) -> list[UserFeatures]:
        """Filter users that meet minimum event threshold for segment assignment."""
        qualified = [
            u for u in user_features
            if u.impression_count_90d >= self.min_events_threshold
        ]
        self._logger.info(
            "Qualified %d/%d users for segmentation (min_events=%d)",
            len(qualified),
            len(user_features),
            self.min_events_threshold,
        )
        return qualified

    @staticmethod
    def _score_metric(value: float, thresholds: list[float]) -> int:
        """Score a metric where higher value = higher score (1-5)."""
        for i, threshold in enumerate(thresholds):
            if value < threshold:
                return i + 1
        return 5

    @staticmethod
    def _score_metric_inverse(value: float, thresholds: list[float]) -> int:
        """Score a metric where lower value = higher score (1-5, inverted)."""
        for i, threshold in enumerate(reversed(thresholds)):
            if value >= threshold:
                return i + 1
        return 5
