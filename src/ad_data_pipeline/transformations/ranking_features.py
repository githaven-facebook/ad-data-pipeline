"""Compute ranking features: CTR, CVR, eCPM per ad/campaign with time-decayed metrics."""

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class AdRankingFeatures:
    """Feature vector for a single ad used in ranking models."""

    ad_id: str
    campaign_id: str
    adset_id: str
    advertiser_id: str
    # Raw counts
    impressions_1h: int = 0
    impressions_6h: int = 0
    impressions_24h: int = 0
    clicks_1h: int = 0
    clicks_6h: int = 0
    clicks_24h: int = 0
    conversions_1h: int = 0
    conversions_6h: int = 0
    conversions_24h: int = 0
    # Derived rates
    ctr_1h: float = 0.0
    ctr_6h: float = 0.0
    ctr_24h: float = 0.0
    cvr_1h: float = 0.0
    cvr_6h: float = 0.0
    cvr_24h: float = 0.0
    # Revenue metrics
    revenue_1h: float = 0.0
    revenue_24h: float = 0.0
    ecpm_1h: float = 0.0
    ecpm_24h: float = 0.0
    # Time-decayed metrics
    ctr_decayed: float = 0.0
    cvr_decayed: float = 0.0
    ecpm_decayed: float = 0.0
    # Budget signals
    budget_utilization: float = 0.0
    pacing_factor: float = 1.0
    # Computed timestamp
    computed_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class RankingFeatureVector:
    """Flattened feature vector for ML feature store."""

    ad_id: str
    feature_names: list[str] = field(default_factory=list)
    feature_values: list[float] = field(default_factory=list)
    computed_at: datetime = field(default_factory=datetime.utcnow)
    version: str = "v1"

    def to_dict(self) -> dict[str, float]:
        """Return feature dict keyed by feature name."""
        return dict(zip(self.feature_names, self.feature_values))


class RankingFeatureComputer:
    """
    Computes ranking features for ads used in real-time ad serving.

    Features include:
    - CTR/CVR at multiple time windows (1h, 6h, 24h)
    - Time-decayed CTR/CVR/eCPM using exponential decay
    - Budget utilization and pacing signals
    - Laplace smoothed rates for low-traffic ads
    """

    FEATURE_NAMES = [
        "ctr_1h", "ctr_6h", "ctr_24h",
        "cvr_1h", "cvr_6h", "cvr_24h",
        "ecpm_1h", "ecpm_24h",
        "ctr_decayed", "cvr_decayed", "ecpm_decayed",
        "budget_utilization", "pacing_factor",
        "impressions_24h_log", "clicks_24h_log",
        "conversions_24h_log",
    ]

    def __init__(
        self,
        decay_half_life_hours: float = 6.0,
        laplace_smoothing_alpha: float = 1.0,
        min_impressions_for_rate: int = 10,
        prior_ctr: float = 0.001,
        prior_cvr: float = 0.0001,
    ) -> None:
        self.decay_half_life_hours = decay_half_life_hours
        self.laplace_smoothing_alpha = laplace_smoothing_alpha
        self.min_impressions_for_rate = min_impressions_for_rate
        self.prior_ctr = prior_ctr
        self.prior_cvr = prior_cvr
        # Decay constant: lambda = ln(2) / half_life
        self.decay_lambda = math.log(2) / decay_half_life_hours
        self._logger = logging.getLogger(self.__class__.__name__)

    def compute_decayed_metric(
        self,
        values_with_ages: list[tuple[float, float]],
    ) -> float:
        """
        Compute time-decayed weighted average of a metric.

        Args:
            values_with_ages: List of (metric_value, age_hours) tuples

        Returns:
            Exponentially time-decayed weighted sum
        """
        if not values_with_ages:
            return 0.0

        total_weight = 0.0
        weighted_sum = 0.0

        for value, age_hours in values_with_ages:
            weight = math.exp(-self.decay_lambda * age_hours)
            weighted_sum += value * weight
            total_weight += weight

        return weighted_sum / total_weight if total_weight > 0 else 0.0

    def compute_smoothed_ctr(
        self,
        clicks: int,
        impressions: int,
    ) -> float:
        """
        Compute Laplace-smoothed CTR to handle low-traffic ads.

        Uses additive smoothing: (clicks + alpha * prior_ctr) / (impressions + alpha)
        """
        if impressions < self.min_impressions_for_rate:
            # Fall back to prior for very low traffic
            return self.prior_ctr

        alpha = self.laplace_smoothing_alpha
        return (clicks + alpha * self.prior_ctr) / (impressions + alpha)

    def compute_smoothed_cvr(
        self,
        conversions: int,
        clicks: int,
    ) -> float:
        """Compute Laplace-smoothed CVR."""
        if clicks < self.min_impressions_for_rate:
            return self.prior_cvr

        alpha = self.laplace_smoothing_alpha
        return (conversions + alpha * self.prior_cvr) / (clicks + alpha)

    def compute_ecpm(self, revenue: float, impressions: int) -> float:
        """Compute effective CPM (revenue per 1000 impressions)."""
        if impressions == 0:
            return 0.0
        return (revenue / impressions) * 1000

    def build_feature_vector(self, features: AdRankingFeatures) -> RankingFeatureVector:
        """
        Flatten AdRankingFeatures into a RankingFeatureVector for the feature store.

        Args:
            features: Computed ranking features for an ad

        Returns:
            RankingFeatureVector with named float features
        """
        values = [
            features.ctr_1h,
            features.ctr_6h,
            features.ctr_24h,
            features.cvr_1h,
            features.cvr_6h,
            features.cvr_24h,
            features.ecpm_1h,
            features.ecpm_24h,
            features.ctr_decayed,
            features.cvr_decayed,
            features.ecpm_decayed,
            features.budget_utilization,
            features.pacing_factor,
            math.log1p(features.impressions_24h),
            math.log1p(features.clicks_24h),
            math.log1p(features.conversions_24h),
        ]

        return RankingFeatureVector(
            ad_id=features.ad_id,
            feature_names=self.FEATURE_NAMES,
            feature_values=values,
            computed_at=features.computed_at,
        )

    def validate_features(self, features: AdRankingFeatures) -> list[str]:
        """
        Validate computed features for anomalies.

        Returns:
            List of warning messages (empty if all features look valid)
        """
        warnings: list[str] = []

        if features.ctr_24h > 0.5:
            warnings.append(f"Suspiciously high CTR: {features.ctr_24h:.3f} for ad {features.ad_id}")

        if features.cvr_24h > 1.0:
            warnings.append(f"CVR > 1.0 is impossible: {features.cvr_24h:.3f} for ad {features.ad_id}")

        if features.ecpm_24h > 100.0:
            warnings.append(f"Unusually high eCPM: ${features.ecpm_24h:.2f} for ad {features.ad_id}")

        if features.impressions_24h == 0 and features.clicks_24h > 0:
            warnings.append(f"Clicks without impressions for ad {features.ad_id}")

        return warnings
