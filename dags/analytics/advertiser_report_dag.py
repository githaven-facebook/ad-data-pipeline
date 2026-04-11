"""
Advertiser Report DAG

Daily DAG that generates advertiser performance reports from the previous day's
ad event data, aggregated across the campaign hierarchy (campaign -> adset -> ad).

Flow:
  wait_for_previous_day_data → aggregate_by_campaign_hierarchy
  → compute_kpis → generate_reports → store_in_db
  → send_email_notifications
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from plugins.sensors.s3_partition_sensor import S3PartitionReadinessSensor

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": ["data-platform@fb.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=4),
    "sla": timedelta(hours=10),  # Reports must be delivered by 10 AM UTC
}

ANALYTICS_BUCKET = "fb-ad-analytics"
REPORTS_DB_CONN_ID = "postgres_reports"
MAX_EMAIL_BATCH_SIZE = 100  # Max advertisers per email batch


def on_failure_callback(context: dict[str, Any]) -> None:
    """Alert on report generation failure."""
    logger.error(
        "Advertiser report DAG failed: task=%s run_id=%s",
        context["task"].task_id,
        context["run_id"],
    )


@dag(
    dag_id="advertiser_report_daily",
    description="Daily advertiser performance report generation with email delivery",
    schedule_interval="0 7 * * *",  # 7 AM UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["analytics", "reporting", "daily", "advertiser-facing"],
    doc_md=__doc__,
    on_failure_callback=on_failure_callback,
)
def advertiser_report_dag() -> None:
    """Advertiser report DAG definition."""

    # Wait for all partitions from the previous day to be ready
    wait_for_previous_day_data = S3PartitionReadinessSensor(
        task_id="wait_for_previous_day_data",
        bucket_name="{{ var.value.processed_events_bucket }}",
        partition_prefix=(
            "events/"
            "year={{ (execution_date - macros.timedelta(days=1)).year }}/"
            "month={{ '{:02d}'.format((execution_date - macros.timedelta(days=1)).month) }}/"
            "day={{ '{:02d}'.format((execution_date - macros.timedelta(days=1)).day) }}/"
        ),
        min_file_count=24,  # Expect at least 24 hourly partition files
        aws_conn_id="aws_default",
        poke_interval=300,  # Check every 5 minutes
        timeout=14400,  # Wait up to 4 hours
        mode="reschedule",
    )

    @task(task_id="aggregate_by_campaign_hierarchy")
    def aggregate_by_campaign_hierarchy(**context: Any) -> dict[str, Any]:
        """
        Aggregate previous day's events by campaign hierarchy.

        Produces a roll-up table: advertiser_id, campaign_id, adset_id, ad_id,
        date with summed impression/click/conversion/spend/revenue counts.
        """
        logical_date = context["logical_date"]
        from datetime import timedelta
        report_date = (logical_date - timedelta(days=1)).date()

        logger.info("Aggregating campaign hierarchy for report date %s", report_date)

        # In production: run Spark aggregation reading all event_type partitions
        # for the report_date and grouping by campaign hierarchy
        aggregation_output_prefix = (
            f"aggregations/date={report_date.isoformat()}/"
        )

        return {
            "report_date": report_date.isoformat(),
            "aggregation_output_prefix": aggregation_output_prefix,
            "advertiser_count": 0,  # Would be populated by actual aggregation
            "campaign_count": 0,
            "total_rows": 0,
        }

    @task(task_id="compute_kpis")
    def compute_kpis(aggregation_result: dict[str, Any], **context: Any) -> dict[str, Any]:
        """
        Compute KPIs from aggregated counts.

        KPIs: CTR, CVR, CPA, CPC, CPM, ROAS, eCPM
        Handles division-by-zero with default values (0.0).
        """
        report_date = aggregation_result["report_date"]
        logger.info("Computing KPIs for %s", report_date)

        kpi_output_prefix = f"kpis/date={report_date}/"

        return {
            "report_date": report_date,
            "kpi_output_prefix": kpi_output_prefix,
            "kpis_computed": ["ctr", "cvr", "cpa", "cpc", "cpm", "roas", "ecpm"],
            "advertiser_count": aggregation_result["advertiser_count"],
        }

    @task(task_id="generate_reports")
    def generate_reports(kpi_result: dict[str, Any], **context: Any) -> dict[str, Any]:
        """
        Generate per-advertiser report objects and serialize to S3.

        Each advertiser gets a structured JSON report with:
        - Campaign hierarchy breakdown
        - Daily KPI metrics
        - Period-over-period comparisons
        - Top performing ads
        """
        report_date = kpi_result["report_date"]
        advertiser_count = kpi_result["advertiser_count"]

        logger.info(
            "Generating reports for %d advertisers for date %s",
            advertiser_count,
            report_date,
        )

        reports_output_prefix = f"reports/date={report_date}/"
        report_ids: list[str] = []

        # In production: for each advertiser, build AdvertiserReport object
        # and serialize to s3://fb-ad-analytics/reports/date=YYYY-MM-DD/
        # advertiser_id=<id>/report.json

        return {
            "report_date": report_date,
            "reports_prefix": reports_output_prefix,
            "report_count": len(report_ids),
            "advertiser_ids_with_reports": report_ids,
        }

    @task(task_id="store_in_db", retries=2)
    def store_in_db(report_result: dict[str, Any], **context: Any) -> dict[str, Any]:
        """
        Store report summaries in PostgreSQL for dashboard queries.

        Upserts daily metrics for each advertiser/campaign/adset/ad combination.
        """
        report_date = report_result["report_date"]
        report_count = report_result["report_count"]

        logger.info(
            "Storing %d advertiser reports in DB for date %s",
            report_count,
            report_date,
        )

        # In production: use PostgresHook to bulk insert report rows
        # hook = PostgresHook(postgres_conn_id=REPORTS_DB_CONN_ID)
        # hook.run(INSERT_REPORT_SQL, parameters=report_rows)

        return {
            "report_date": report_date,
            "rows_upserted": 0,
            "db_status": "success",
        }

    @task(task_id="send_email_notifications")
    def send_email_notifications(
        report_result: dict[str, Any],
        db_result: dict[str, Any],
        **context: Any,
    ) -> dict[str, Any]:
        """
        Send email notifications to advertisers with their daily performance report.

        Batches emails to stay within rate limits.
        Skips advertisers with no spend or no active campaigns.
        """
        from airflow.utils.email import send_email

        report_date = report_result["report_date"]
        advertiser_ids = report_result.get("advertiser_ids_with_reports", [])

        logger.info(
            "Sending email notifications to %d advertisers for %s",
            len(advertiser_ids),
            report_date,
        )

        sent_count = 0
        failed_count = 0

        # Process in batches
        for i in range(0, len(advertiser_ids), MAX_EMAIL_BATCH_SIZE):
            batch = advertiser_ids[i : i + MAX_EMAIL_BATCH_SIZE]
            for advertiser_id in batch:
                try:
                    # In production: fetch report, render HTML template, send via SES
                    logger.debug("Would send report email to advertiser %s", advertiser_id)
                    sent_count += 1
                except Exception as e:
                    failed_count += 1
                    logger.warning("Failed to send email to advertiser %s: %s", advertiser_id, e)

        logger.info(
            "Email notifications: sent=%d, failed=%d for %s",
            sent_count,
            failed_count,
            report_date,
        )

        return {
            "report_date": report_date,
            "emails_sent": sent_count,
            "emails_failed": failed_count,
        }

    @task(task_id="mark_reports_finalized")
    def mark_reports_finalized(
        email_result: dict[str, Any], **context: Any
    ) -> None:
        """Mark reports as finalized and update pipeline metadata."""
        from airflow.models import Variable

        report_date = email_result["report_date"]
        Variable.set(
            f"advertiser_reports_complete_{report_date}",
            "true",
            description=f"Advertiser reports finalized for {report_date}",
        )
        logger.info("Reports finalized for %s", report_date)

    # Wire up the DAG
    aggregation_result = aggregate_by_campaign_hierarchy()
    kpi_result = compute_kpis(aggregation_result)
    report_result = generate_reports(kpi_result)
    db_result = store_in_db(report_result)
    email_result = send_email_notifications(report_result, db_result)
    mark_reports_finalized(email_result)

    wait_for_previous_day_data >> aggregation_result


advertiser_report_daily = advertiser_report_dag()
