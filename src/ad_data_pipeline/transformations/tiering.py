"""Hot/cold data classification and tiering to S3 Glacier."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class StorageTier(str, Enum):
    """S3 storage tier classifications."""

    HOT = "hot"           # S3 Standard - frequently accessed
    WARM = "warm"         # S3 Standard-IA - infrequently accessed
    COLD = "cold"         # S3 Glacier Instant Retrieval
    ARCHIVE = "archive"   # S3 Glacier Deep Archive


@dataclass
class PartitionMetadata:
    """Metadata for a single S3 partition used for tiering decisions."""

    partition_prefix: str
    partition_date: datetime
    size_bytes: int
    file_count: int
    row_count: int
    last_accessed: Optional[datetime] = None
    access_count_7d: int = 0
    access_count_30d: int = 0
    current_tier: StorageTier = StorageTier.HOT
    event_type: Optional[str] = None


@dataclass
class TieringDecision:
    """Result of classifying a partition for storage tiering."""

    partition: PartitionMetadata
    target_tier: StorageTier
    reason: str
    estimated_monthly_savings_usd: float = 0.0
    requires_action: bool = False

    @property
    def is_downgrade(self) -> bool:
        """True if moving to a cheaper/slower tier."""
        tier_order = [StorageTier.HOT, StorageTier.WARM, StorageTier.COLD, StorageTier.ARCHIVE]
        current_idx = tier_order.index(self.partition.current_tier)
        target_idx = tier_order.index(self.target_tier)
        return target_idx > current_idx


# S3 storage pricing per GB/month (approximate)
STORAGE_PRICING: dict[StorageTier, float] = {
    StorageTier.HOT: 0.023,
    StorageTier.WARM: 0.0125,
    StorageTier.COLD: 0.004,
    StorageTier.ARCHIVE: 0.00099,
}


class DataTieringClassifier:
    """
    Classifies S3 partitions into hot/cold storage tiers based on age and access patterns.

    Classification rules:
    - HOT: Data < hot_threshold_days old, OR access_count_7d > hot_access_threshold
    - WARM: Data between hot and warm thresholds with moderate access
    - COLD: Data > warm_threshold_days and low access
    - ARCHIVE: Data > archive_threshold_days with no recent access
    """

    def __init__(
        self,
        hot_threshold_days: int = 30,
        warm_threshold_days: int = 90,
        cold_threshold_days: int = 180,
        archive_threshold_days: int = 365,
        hot_access_threshold: int = 10,
        warm_access_threshold: int = 3,
    ) -> None:
        self.hot_threshold_days = hot_threshold_days
        self.warm_threshold_days = warm_threshold_days
        self.cold_threshold_days = cold_threshold_days
        self.archive_threshold_days = archive_threshold_days
        self.hot_access_threshold = hot_access_threshold
        self.warm_access_threshold = warm_access_threshold
        self._logger = logging.getLogger(self.__class__.__name__)

    def classify_partition(
        self,
        partition: PartitionMetadata,
        reference_date: Optional[datetime] = None,
    ) -> TieringDecision:
        """
        Classify a partition into its target storage tier.

        Args:
            partition: Partition metadata including age and access counts
            reference_date: Date to use as "now" for age calculation (default: UTC now)

        Returns:
            TieringDecision with the target tier and reasoning
        """
        now = reference_date or datetime.utcnow()
        age_days = (now - partition.partition_date).days

        # Determine target tier based on age and access patterns
        if age_days < self.hot_threshold_days or partition.access_count_7d >= self.hot_access_threshold:
            target_tier = StorageTier.HOT
            reason = (
                f"Recent data (age={age_days}d) or high access frequency "
                f"(7d_accesses={partition.access_count_7d})"
            )
        elif age_days < self.warm_threshold_days or partition.access_count_30d >= self.warm_access_threshold:
            target_tier = StorageTier.WARM
            reason = (
                f"Moderately aged data (age={age_days}d) with some access "
                f"(30d_accesses={partition.access_count_30d})"
            )
        elif age_days < self.archive_threshold_days:
            target_tier = StorageTier.COLD
            reason = f"Old data (age={age_days}d) with low access frequency"
        else:
            target_tier = StorageTier.ARCHIVE
            reason = f"Very old data (age={age_days}d) with no recent access - archiving"

        requires_action = target_tier != partition.current_tier
        savings = self._estimate_monthly_savings(partition.size_bytes, partition.current_tier, target_tier)

        return TieringDecision(
            partition=partition,
            target_tier=target_tier,
            reason=reason,
            estimated_monthly_savings_usd=savings,
            requires_action=requires_action,
        )

    def classify_batch(
        self,
        partitions: list[PartitionMetadata],
        reference_date: Optional[datetime] = None,
    ) -> list[TieringDecision]:
        """Classify multiple partitions, returning only those requiring action."""
        decisions = [self.classify_partition(p, reference_date) for p in partitions]
        actionable = [d for d in decisions if d.requires_action]
        self._logger.info(
            "Classified %d partitions: %d require tier changes",
            len(partitions),
            len(actionable),
        )
        return decisions

    def get_tiering_summary(self, decisions: list[TieringDecision]) -> dict[str, float]:
        """
        Summarize tiering decisions for reporting.

        Returns:
            Dict with counts by target tier and total estimated savings
        """
        summary: dict[str, float] = {
            "total_partitions": float(len(decisions)),
            "requiring_action": float(sum(1 for d in decisions if d.requires_action)),
            "total_monthly_savings_usd": sum(d.estimated_monthly_savings_usd for d in decisions),
        }
        for tier in StorageTier:
            summary[f"target_{tier.value}_count"] = float(
                sum(1 for d in decisions if d.target_tier == tier)
            )
        return summary

    def _estimate_monthly_savings(
        self,
        size_bytes: int,
        current_tier: StorageTier,
        target_tier: StorageTier,
    ) -> float:
        """Estimate monthly cost savings from moving data between tiers."""
        size_gb = size_bytes / (1024**3)
        current_cost = size_gb * STORAGE_PRICING[current_tier]
        target_cost = size_gb * STORAGE_PRICING[target_tier]
        return max(0.0, current_cost - target_cost)
