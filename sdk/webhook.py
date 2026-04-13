# sdk/webhook.py

"""
Webhook Notifier for Canton Compliance SDK.

This module provides a utility for sending notifications about report generation
status to external systems like Slack, Microsoft Teams, or a generic webhook endpoint.

It is designed to be configured via environment variables for easy integration into
CI/CD pipelines and automated reporting workflows.

Configuration:
Set one of the following environment variables with your webhook URL:
- SLACK_WEBHOOK_URL: For sending notifications to a Slack channel.
- TEAMS_WEBHOOK_URL: For sending notifications to a Microsoft Teams channel.
- GENERIC_WEBHOOK_URL: For sending a simple JSON payload to any HTTP endpoint.

Usage:
    from sdk.webhook import get_notifier

    notifier = get_notifier()
    if notifier:
        try:
            # ... your report generation logic ...
            notifier.send_notification(
                report_name="MiFID II Post-Trade Report",
                status="SUCCESS",
                message="Report successfully generated and submitted to ESMA.",
                details={"submission_id": "abc-123", "records": 5043}
            )
        except Exception as e:
            notifier.send_notification(
                report_name="MiFID II Post-Trade Report",
                status="FAILURE",
                message=f"Report generation failed: {e}",
                details={"error_type": type(e).__name__}
            )

Dependencies:
- requests: Make sure to include 'requests' in your project's dependencies.
  (e.g., in requirements.txt or setup.py)
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Literal

import requests

# Configure logging for the module
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

WebhookType = Literal["slack", "teams", "generic"]

class WebhookNotifier:
    """
    Handles sending notifications to various webhook endpoints.
    """

    def __init__(self, webhook_type: WebhookType, url: str):
        """
        Initializes the WebhookNotifier.

        Args:
            webhook_type: The type of the webhook ('slack', 'teams', 'generic').
            url: The webhook URL endpoint.

        Raises:
            ValueError: If the URL is empty or invalid.
        """
        if not url or not url.startswith('http'):
            raise ValueError("A valid webhook URL is required.")
        self.webhook_type = webhook_type
        self.url = url
        self.headers = {'Content-Type': 'application/json'}

    def _format_slack_payload(
        self, report_name: str, status: str, message: str, details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Formats a payload for a Slack incoming webhook using Block Kit."""
        color = {"SUCCESS": "#2eb886", "FAILURE": "#d50000", "WARNING": "#ffc107"}.get(status, "#808080")
        status_icon = {"SUCCESS": ":white_check_mark:", "FAILURE": ":x:", "WARNING": ":warning:"}.get(status, ":grey_question:")
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Canton Compliance Report: {report_name}"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Status:*\n{status_icon} {status}"},
                    {"type": "mrkdwn", "text": f"*Timestamp:*\n{timestamp}"}
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message
                }
            }
        ]
        if details:
            details_text = "```\n" + json.dumps(details, indent=2, sort_keys=True) + "\n```"
            blocks.extend([
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Details:*\n{details_text}"
                    }
                }
            ])

        return {"attachments": [{"color": color, "blocks": blocks}]}

    def _format_teams_payload(
        self, report_name: str, status: str, message: str, details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Formats a payload for an MS Teams incoming webhook using MessageCard format."""
        theme_color = {"SUCCESS": "2eb886", "FAILURE": "d50000", "WARNING": "ffc107"}.get(status, "808080")
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')

        facts = [
            {"name": "Status:", "value": status},
            {"name": "Timestamp:", "value": timestamp}
        ]

        section = {
            "activityTitle": f"**Canton Compliance Report: {report_name}**",
            "activitySubtitle": message,
            "facts": facts,
            "markdown": True
        }

        if details:
            details_text = json.dumps(details, indent=2, sort_keys=True)
            section["text"] = f"**Details:**\n\n```\n{details_text}\n```"

        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": theme_color,
            "summary": f"Report: {report_name} - {status}",
            "sections": [section]
        }

    def _format_generic_payload(
        self, report_name: str, status: str, message: str, details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Formats a generic, flat JSON payload."""
        return {
            "source": "canton-compliance-sdk",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "reportName": report_name,
            "status": status,
            "message": message,
            "details": details or {}
        }

    def send_notification(
        self, report_name: str, status: str, message: str, details: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Constructs and sends a notification to the configured webhook.

        Args:
            report_name: The name of the report (e.g., "EMIR Collateral Report").
            status: The status of the operation ("SUCCESS", "FAILURE", "WARNING").
            message: A human-readable summary message.
            details: An optional dictionary with additional context.
        """
        formatters = {
            "slack": self._format_slack_payload,
            "teams": self._format_teams_payload,
            "generic": self._format_generic_payload,
        }
        formatter = formatters.get(self.webhook_type)
        if not formatter:
            logger.error(f"Invalid webhook type specified: {self.webhook_type}")
            return

        payload = formatter(report_name, status, message, details)
        
        try:
            response = requests.post(self.url, headers=self.headers, data=json.dumps(payload), timeout=10)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            logger.info(f"Successfully sent '{status}' notification for report '{report_name}' to {self.webhook_type}.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send webhook notification to {self.webhook_type}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during webhook notification: {e}")

def get_notifier() -> Optional[WebhookNotifier]:
    """
    Factory function to create a WebhookNotifier based on environment variables.

    It checks for environment variables in the following order:
    1. SLACK_WEBHOOK_URL
    2. TEAMS_WEBHOOK_URL
    3. GENERIC_WEBHOOK_URL

    Returns:
        An instance of WebhookNotifier if a corresponding environment variable is set,
        otherwise None.
    """
    hooks = {
        "slack": os.getenv("SLACK_WEBHOOK_URL"),
        "teams": os.getenv("TEAMS_WEBHOOK_URL"),
        "generic": os.getenv("GENERIC_WEBHOOK_URL"),
    }

    for hook_type, url in hooks.items():
        if url:
            try:
                logger.info(f"Configuring webhook notifier for: {hook_type.upper()}")
                return WebhookNotifier(webhook_type=hook_type, url=url)
            except ValueError as e:
                logger.error(f"Invalid webhook URL for {hook_type.upper()}: {e}")
                return None
    
    logger.debug("No webhook environment variables found. Notifier not created.")
    return None

if __name__ == "__main__":
    """
    Example usage and self-test.
    To run this, set one of the webhook environment variables.
    e.g., export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
    """
    print("--- Canton Compliance SDK Webhook Test ---")
    notifier_instance = get_notifier()

    if notifier_instance:
        print(f"Notifier created for type: {notifier_instance.webhook_type}")
        
        # Test SUCCESS notification
        print("\nSending test SUCCESS notification...")
        notifier_instance.send_notification(
            report_name="EMIR Daily Valuation (Test)",
            status="SUCCESS",
            message="Daily EMIR valuation report generated and archived successfully.",
            details={
                "environment": "development",
                "run_id": "bdf04a92-3b2d-4299-8d48-6d538e3a24ed",
                "record_count": 1250,
                "artifact_path": "s3://compliance-reports/emir/2023-10-27/report.xml"
            }
        )
        print("SUCCESS notification sent.")

        # Test FAILURE notification
        print("\nSending test FAILURE notification...")
        notifier_instance.send_notification(
            report_name="MiFID II RTS 22 (Test)",
            status="FAILURE",
            message="Failed to connect to the upstream trade data source.",
            details={
                "environment": "development",
                "error_code": "DATASOURCE_UNAVAILABLE",
                "traceback_snippet": "at sdk.sources.trade_api.connect(line 42)"
            }
        )
        print("FAILURE notification sent.")
    else:
        print("\nNo webhook environment variables set.")
        print("Please set one of SLACK_WEBHOOK_URL, TEAMS_WEBHOOK_URL, or GENERIC_WEBHOOK_URL to run tests.")

    print("\n--- Test Complete ---")