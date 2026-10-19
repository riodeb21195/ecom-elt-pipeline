"""
Einfaches SLA-Monitoring: Dagster Run-Failure-Sensor postet an Slack, wenn der
End-to-End-Pipeline-Job fehlschlaegt. "Kalibriert" heisst hier: es wird NICHT bei
jedem einzelnen Asset-Retry alarmiert, sondern nur beim finalen Fehlschlag des
gesamten Jobs, um Alert-Fatigue zu vermeiden.
"""

import os

import requests
from dagster import RunFailureSensorContext, run_failure_sensor

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")


@run_failure_sensor(monitored_jobs=None)  # None = alle Jobs, hier nur full_elt_pipeline relevant
def slack_on_pipeline_failure(context: RunFailureSensorContext):
    if not SLACK_WEBHOOK_URL:
        context.log.warning("SLACK_WEBHOOK_URL not set, skipping alert.")
        return

    message = (
        f":rotating_light: Pipeline-Run fehlgeschlagen: `{context.dagster_run.job_name}` "
        f"(run_id={context.dagster_run.run_id}). Siehe docs/runbook.md fuer Troubleshooting-Schritte."
    )
    requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=5)
