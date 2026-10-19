"""
Dagster-Orchestrierung: End-to-End mit expliziten Abhaengigkeiten.

extract_orders -> upload_to_staging -> load_to_warehouse -> dbt_run (staging/intermediate/marts)

Jedes Asset ist ein Daten-Artefakt, keine reine Task -- Dagster zeigt Lineage
und ermoeglicht selektive Neu-Materialisierung einzelner Stufen.
"""

import subprocess
import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    Definitions,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from dagster_dbt import DbtCliResource, dbt_assets

REPO_ROOT = Path(__file__).parents[2]
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"

dbt_resource = DbtCliResource(project_dir=str(DBT_PROJECT_DIR))


def _run_python_script(context: AssetExecutionContext, script_path: Path) -> None:
    result = subprocess.run(
    [sys.executable, str(script_path)],
    capture_output=True,
    text=True,
    check=False,
)
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"{script_path.name} failed with exit code {result.returncode}")


@asset(group_name="extract")
def raw_orders_extracted(context: AssetExecutionContext) -> None:
    """Zieht neue Orders aus der Mock-API und schreibt Parquet in die lokale Staging-Ablage."""
    _run_python_script(context, REPO_ROOT / "extract" / "extract_orders.py")
    context.add_output_metadata({"status": MetadataValue.text("extracted")})


@asset(group_name="staging", deps=[raw_orders_extracted])
def staged_orders_in_minio(context: AssetExecutionContext) -> None:
    """Laedt die extrahierten Parquet-Dateien nach MinIO hoch."""
    _run_python_script(context, REPO_ROOT / "staging_minio" / "upload_to_minio.py")


@asset(group_name="load", deps=[staged_orders_in_minio])
def orders_loaded_to_warehouse(context: AssetExecutionContext) -> None:
    """Bulk-Load aus Staging in raw.orders, atomar mit Row-Count-Validierung."""
    _run_python_script(context, REPO_ROOT / "warehouse" / "load_orders.py")


@dbt_assets(manifest=DBT_PROJECT_DIR / "target" / "manifest.json")
def dbt_transform_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Alle dbt-Modelle (staging -> intermediate -> marts) inkl. Tests."""
    yield from dbt.cli(["build"], context=context).stream()


full_pipeline_job = define_asset_job(
    name="full_elt_pipeline",
    selection=[
        raw_orders_extracted,
        staged_orders_in_minio,
        orders_loaded_to_warehouse,
        dbt_transform_assets,
    ],
)

# SLA: taeglicher Lauf um 04:00 UTC (siehe docs/sla_slo.md fuer das definierte SLA-Fenster)
daily_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 4 * * *",
)

defs = Definitions(
    assets=[
        raw_orders_extracted,
        staged_orders_in_minio,
        orders_loaded_to_warehouse,
        dbt_transform_assets,
    ],
    jobs=[full_pipeline_job],
    schedules=[daily_schedule],
    resources={"dbt": dbt_resource},
)
