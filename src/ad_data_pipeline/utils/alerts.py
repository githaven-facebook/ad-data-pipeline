"""Alert helpers for PagerDuty and Slack webhook notifications."""

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class AlertSeverity(str, Enum):
    """Alert severity levels."""

    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class Alert:
    """Represents an alert to be sent through notification channels."""

    title: str
    message: str
    severity: AlertSeverity
    dag_id: Optional[str] = None
    task_id: Optional[str] = None
    run_id: Optional[str] = None
    timestamp: datetime = None  # type: ignore[assignment]
    context: Optional[dict[str, str]] = None

    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


class AlertManager:
    """
    Manages alert delivery to PagerDuty and Slack.

    Supports:
    - PagerDuty Events API v2 for critical/error alerts
    - Slack webhook for all severity levels
    - Deduplication by dag_id + task_id
    """

    PAGERDUTY_EVENTS_URL = "https://events.pagerduty.com/v2/enqueue"
    PAGERDUTY_SEVERITY_MAP: dict[AlertSeverity, str] = {
        AlertSeverity.CRITICAL: "critical",
        AlertSeverity.ERROR: "error",
        AlertSeverity.WARNING: "warning",
        AlertSeverity.INFO: "info",
    }
    SLACK_COLOR_MAP: dict[AlertSeverity, str] = {
        AlertSeverity.CRITICAL: "#FF0000",
        AlertSeverity.ERROR: "#FF6600",
        AlertSeverity.WARNING: "#FFCC00",
        AlertSeverity.INFO: "#36A64F",
    }

    def __init__(
        self,
        pagerduty_integration_key: Optional[str] = None,
        slack_webhook_url: Optional[str] = None,
        environment: str = "production",
        service_name: str = "ad-data-pipeline",
        request_timeout_seconds: int = 10,
    ) -> None:
        self.pagerduty_integration_key = pagerduty_integration_key
        self.slack_webhook_url = slack_webhook_url
        self.environment = environment
        self.service_name = service_name
        self.request_timeout_seconds = request_timeout_seconds
        self._logger = logging.getLogger(self.__class__.__name__)

    def send_alert(self, alert: Alert) -> None:
        """
        Send alert to all configured channels based on severity.

        Critical/Error alerts go to PagerDuty + Slack.
        Warning/Info alerts go to Slack only.
        """
        if alert.severity in (AlertSeverity.CRITICAL, AlertSeverity.ERROR):
            if self.pagerduty_integration_key:
                self._send_pagerduty(alert)

        if self.slack_webhook_url:
            self._send_slack(alert)

        self._logger.info(
            "Alert sent: [%s] %s",
            alert.severity.value.upper(),
            alert.title,
        )

    def send_dag_failure_alert(
        self,
        dag_id: str,
        task_id: str,
        run_id: str,
        exception: Exception,
        context: Optional[dict[str, str]] = None,
    ) -> None:
        """Convenience method to send a DAG task failure alert."""
        alert = Alert(
            title=f"DAG Task Failed: {dag_id}.{task_id}",
            message=(
                f"Task `{task_id}` in DAG `{dag_id}` failed during run `{run_id}`.\n"
                f"Error: {type(exception).__name__}: {exception}"
            ),
            severity=AlertSeverity.ERROR,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            context=context or {},
        )
        self.send_alert(alert)

    def send_sla_miss_alert(
        self,
        dag_id: str,
        task_id: str,
        sla_minutes: int,
        actual_minutes: float,
    ) -> None:
        """Send alert when a task misses its SLA."""
        alert = Alert(
            title=f"SLA Miss: {dag_id}.{task_id}",
            message=(
                f"Task `{task_id}` in DAG `{dag_id}` missed SLA of {sla_minutes} minutes. "
                f"Actual duration: {actual_minutes:.1f} minutes."
            ),
            severity=AlertSeverity.WARNING,
            dag_id=dag_id,
            task_id=task_id,
        )
        self.send_alert(alert)

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
    )
    def _send_pagerduty(self, alert: Alert) -> None:
        """Send alert to PagerDuty Events API v2."""
        dedup_key = f"{self.service_name}-{alert.dag_id}-{alert.task_id}"

        payload = {
            "routing_key": self.pagerduty_integration_key,
            "event_action": "trigger",
            "dedup_key": dedup_key,
            "payload": {
                "summary": f"[{self.environment.upper()}] {alert.title}",
                "source": self.service_name,
                "severity": self.PAGERDUTY_SEVERITY_MAP[alert.severity],
                "timestamp": alert.timestamp.isoformat(),
                "component": alert.dag_id or self.service_name,
                "group": "data-platform",
                "custom_details": {
                    "message": alert.message,
                    "dag_id": alert.dag_id,
                    "task_id": alert.task_id,
                    "run_id": alert.run_id,
                    "environment": self.environment,
                    **(alert.context or {}),
                },
            },
        }

        response = requests.post(
            self.PAGERDUTY_EVENTS_URL,
            json=payload,
            timeout=self.request_timeout_seconds,
        )
        response.raise_for_status()
        self._logger.info("PagerDuty alert sent: %s", alert.title)

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
    )
    def _send_slack(self, alert: Alert) -> None:
        """Send alert to Slack via webhook."""
        color = self.SLACK_COLOR_MAP[alert.severity]
        fields = [
            {"title": "Environment", "value": self.environment, "short": True},
            {"title": "Severity", "value": alert.severity.value.upper(), "short": True},
        ]

        if alert.dag_id:
            fields.append({"title": "DAG", "value": alert.dag_id, "short": True})
        if alert.task_id:
            fields.append({"title": "Task", "value": alert.task_id, "short": True})
        if alert.run_id:
            fields.append({"title": "Run ID", "value": alert.run_id, "short": False})

        payload = {
            "attachments": [
                {
                    "fallback": alert.title,
                    "color": color,
                    "title": alert.title,
                    "text": alert.message,
                    "fields": fields,
                    "footer": self.service_name,
                    "ts": int(alert.timestamp.timestamp()),
                }
            ]
        }

        response = requests.post(
            self.slack_webhook_url,  # type: ignore[arg-type]
            json=payload,
            timeout=self.request_timeout_seconds,
        )
        response.raise_for_status()
        self._logger.info("Slack alert sent: %s", alert.title)
