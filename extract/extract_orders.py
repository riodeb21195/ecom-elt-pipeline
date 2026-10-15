import io
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

load_dotenv()

sys.path.append(os.path.dirname(__file__))
from checkpoint_store import RunMetadata, get_last_checkpoint, save_checkpoint
from schema_drift_check import SchemaDriftError, check_schema_drift

SOURCE = "orders"
SOURCE_API_BASE_URL = os.environ.get("SOURCE_API_BASE_URL", "http://localhost:8000")
SOURCE_API_KEY = os.environ.get("SOURCE_API_KEY", "")
PAGE_SIZE = 100
PAUSE_BETWEEN_REQUESTS_SECONDS = 0.2  # schont das (simulierte) Quellsystem


class RetryableAPIError(Exception):
    pass


@retry(
    retry=retry_if_exception_type(RetryableAPIError),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _fetch_page(offset: int, updated_after: str | None) -> dict:
    params = {"limit": PAGE_SIZE, "offset": offset}
    if updated_after:
        params["updated_after"] = updated_after

    headers = {"Authorization": f"Bearer {SOURCE_API_KEY}"} if SOURCE_API_KEY else {}
    response = requests.get(f"{SOURCE_API_BASE_URL}/orders", params=params, headers=headers, timeout=10)

    if response.status_code in (429, 503):
        # Retryable: Quellsystem ist ueberlastet oder rate-limitet uns bewusst.
        raise RetryableAPIError(f"Retryable status {response.status_code}: {response.text}")
    response.raise_for_status()
    return response.json()


def extract_all(updated_after: str | None) -> list[dict]:
    all_records: list[dict] = []
    offset = 0
    while True:
        page = _fetch_page(offset, updated_after)
        all_records.extend(page["data"])
        if not page["pagination"]["has_more"]:
            break
        offset += PAGE_SIZE
        time.sleep(PAUSE_BETWEEN_REQUESTS_SECONDS)
    return all_records


def write_to_staging(records: list[dict], run_id: str) -> str:
    """
    Schreibt Parquet lokal (bzw. in echten Setup: nach MinIO via s3fs/boto3).
    Hier lokal gehalten, damit das Projekt ohne laufendes MinIO getestet werden
    kann -- der MinIO-Upload-Pfad ist in staging_minio/upload_to_minio.py separat,
    damit Extract-Logik und Storage-Logik entkoppelt bleiben.
    """
    df = pd.DataFrame(records)
    output_dir = os.path.join(os.path.dirname(__file__), "..", "staging_minio", "_local_raw")
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"orders_{run_id}.parquet")
    df.to_parquet(path, index=False)
    return path


def _checkpoint_to_slug(checkpoint: str | None) -> str:
    """
    Leitet aus dem Checkpoint-Wert einen dateinamen-sicheren Slug ab. Das ist
    die Grundlage der Idempotenz: zwei Laeufe mit demselben Checkpoint (z.B.
    ein Retry nach einem fehlgeschlagenen Load-Schritt, bevor der Checkpoint
    vorgerueckt wurde) erzeugen denselben Dateinamen und ueberschreiben sich
    damit gegenseitig, statt Duplikate anzuhaeufen.
    """
    if not checkpoint:
        return "initial"
    return checkpoint.replace(":", "").replace("-", "").replace(".", "")


def main():
    run_started_at = datetime.now(timezone.utc)
    last_checkpoint = get_last_checkpoint(SOURCE)
    run_id = _checkpoint_to_slug(last_checkpoint)

    print(f"[extract] Starting run (checkpoint-slug={run_id}), since checkpoint={last_checkpoint}")

    try:
        records = extract_all(updated_after=last_checkpoint)

        drift_result = check_schema_drift(records, source=SOURCE)
        if drift_result["drift_detected"]:
            print(f"[extract] WARNING: non-breaking schema drift detected: {drift_result['details']}")

        if records:
            path = write_to_staging(records, run_id)
            print(f"[extract] Wrote {len(records)} records to {path}")
            new_checkpoint = max(r["updated_at"] for r in records)
        else:
            print("[extract] No new records since last checkpoint.")
            new_checkpoint = last_checkpoint or run_started_at.isoformat()

        metadata = RunMetadata(
            source=SOURCE,
            started_at=run_started_at.isoformat(),
            finished_at=datetime.now(timezone.utc).isoformat(),
            rows_extracted=len(records),
            status="success",
        )
        save_checkpoint(SOURCE, new_checkpoint, metadata)

    except SchemaDriftError as e:
        metadata = RunMetadata(
            source=SOURCE,
            started_at=run_started_at.isoformat(),
            finished_at=datetime.now(timezone.utc).isoformat(),
            rows_extracted=0,
            status="failed",
            error=str(e),
        )
        # Bei breaking drift: Checkpoint NICHT vorruecken, damit der naechste Run
        # dieselben Daten erneut versucht, sobald das Schema-Problem behoben ist.
        save_checkpoint(SOURCE, last_checkpoint or "", metadata)
        print(f"[extract] FAILED (schema drift): {e}")
        raise

    except Exception as e:
        metadata = RunMetadata(
            source=SOURCE,
            started_at=run_started_at.isoformat(),
            finished_at=datetime.now(timezone.utc).isoformat(),
            rows_extracted=0,
            status="failed",
            error=str(e),
        )
        save_checkpoint(SOURCE, last_checkpoint or "", metadata)
        print(f"[extract] FAILED: {e}")
        raise


if __name__ == "__main__":
    main()

