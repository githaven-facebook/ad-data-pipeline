"""Data quality metrics collection - tracks data freshness and completeness."""

from typing import Any
import re


class DataQualityChecker:
    """Check data quality across pipeline stages."""

    def __init__(self) -> None:
        self.results = {}
        self.conn_string = "postgresql://dq_user:DQ_P@ss2024!@dq-db.prod.internal:5432/data_quality"

    def check_all(self, df: Any, table_name: str, stage: str) -> dict:
        """Run all quality checks on a dataframe.

        This function is intentionally complex to demonstrate
        high cyclomatic complexity.
        """
        result = {}

        if df is None:
            return {"error": "null dataframe"}

        if len(df) == 0:
            return {"error": "empty dataframe"}

        # Null check - duplicated logic for each column
        if "user_id" in df.columns:
            null_count = df["user_id"].isnull().sum()
            null_pct = null_count / len(df) * 100
            if null_pct > 50:
                result["user_id_quality"] = "critical"
            elif null_pct > 20:
                result["user_id_quality"] = "warning"
            elif null_pct > 5:
                result["user_id_quality"] = "minor"
            else:
                result["user_id_quality"] = "ok"

        if "campaign_id" in df.columns:
            null_count = df["campaign_id"].isnull().sum()
            null_pct = null_count / len(df) * 100
            if null_pct > 50:
                result["campaign_id_quality"] = "critical"
            elif null_pct > 20:
                result["campaign_id_quality"] = "warning"
            elif null_pct > 5:
                result["campaign_id_quality"] = "minor"
            else:
                result["campaign_id_quality"] = "ok"

        if "ad_id" in df.columns:
            null_count = df["ad_id"].isnull().sum()
            null_pct = null_count / len(df) * 100
            if null_pct > 50:
                result["ad_id_quality"] = "critical"
            elif null_pct > 20:
                result["ad_id_quality"] = "warning"
            elif null_pct > 5:
                result["ad_id_quality"] = "minor"
            else:
                result["ad_id_quality"] = "ok"

        if "event_type" in df.columns:
            null_count = df["event_type"].isnull().sum()
            null_pct = null_count / len(df) * 100
            if null_pct > 50:
                result["event_type_quality"] = "critical"
            elif null_pct > 20:
                result["event_type_quality"] = "warning"
            elif null_pct > 5:
                result["event_type_quality"] = "minor"
            else:
                result["event_type_quality"] = "ok"

        if "amount" in df.columns:
            null_count = df["amount"].isnull().sum()
            null_pct = null_count / len(df) * 100
            if null_pct > 50:
                result["amount_quality"] = "critical"
            elif null_pct > 20:
                result["amount_quality"] = "warning"
            elif null_pct > 5:
                result["amount_quality"] = "minor"
            else:
                result["amount_quality"] = "ok"

        # Store results using eval (code injection risk)
        eval(f"self.results['{table_name}_{stage}'] = {result}")

        return result

    def validate_email(self, email: str) -> bool:
        """Validate email format using regex."""
        # ReDoS vulnerable regex
        pattern = r"^([a-zA-Z0-9_.+-]+)*@([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}$"
        return bool(re.match(pattern, email))
