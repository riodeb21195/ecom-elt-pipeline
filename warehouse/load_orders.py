import hashlib
import os
import uuid
from datetime import UTC, datetime

import boto3
import duckdb
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH", "./warehouse/ecom.duckdb")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_RAW = os.environ.get("MINIO_BUCKET_RAW", "raw")

LEDGER_INSERT = """
    INSERT INTO pipeline_metadata.load_runs
        (load_run_id, source_file, table_name, rows_loaded, row_count_check,
         checksum, started_at, finished_at, status, error_message, file_sha256)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def get_minio_client():
    """Erstellt einen Client für den Zugriff auf MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def configure_minio(con: duckdb.DuckDBPyConnection) -> None:
    """Konfiguriert DuckDB für den direkten Zugriff auf MinIO."""
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute(f"SET s3_endpoint = '{MINIO_ENDPOINT}'")
    con.execute(f"SET s3_access_key_id = '{MINIO_ROOT_USER}'")
    con.execute(f"SET s3_secret_access_key = '{MINIO_ROOT_PASSWORD}'")
    con.execute("SET s3_region = 'us-east-1'")
    con.execute("SET s3_url_style = 'path'")
    con.execute("SET s3_use_ssl = false")


def list_minio_files(client) -> list[str]:
    """Findet alle Parquet-Dateien im orders-Ordner des Raw-Buckets."""
    paginator = client.get_paginator("list_objects_v2")
    files = []

    for page in paginator.paginate(Bucket=BUCKET_RAW, Prefix="orders/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                files.append(key)

    return sorted(files)


def _file_sha256(client, key: str) -> str:
    """Berechnet den SHA-256-Hash einer Datei direkt aus MinIO."""
    response = client.get_object(Bucket=BUCKET_RAW, Key=key)
    body = response["Body"]
    h = hashlib.sha256()

    try:
        for chunk in iter(lambda: body.read(1024 * 1024), b""):
            h.update(chunk)
    finally:
        body.close()

    return h.hexdigest()


def _already_loaded(
    con: duckdb.DuckDBPyConnection, file_sha: str
) -> bool:
    """Prüft, ob dieser Dateiinhalt bereits erfolgreich geladen wurde."""
    count = con.execute(
        """
        SELECT COUNT(*)
        FROM pipeline_metadata.load_runs
        WHERE file_sha256 = ?
          AND status = 'success'
        """,
        [file_sha],
    ).fetchone()[0]

    return count > 0


def _checksum_file(
    con: duckdb.DuckDBPyConnection, source: str
) -> str:
    """Berechnet einen logischen Checksum anhand der Order-IDs."""
    rows = con.execute(
        f"SELECT order_id FROM read_parquet({source})"
    ).fetchall()

    ids = sorted(str(row[0]) for row in rows)
    return hashlib.sha256(",".join(ids).encode()).hexdigest()


def load_file(
    con: duckdb.DuckDBPyConnection,
    client,
    key: str,
) -> None:
    load_run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)

    source_name = os.path.basename(key)
    file_sha = _file_sha256(client, key)

    # Bereits erfolgreich geladene Dateien überspringen.
    if _already_loaded(con, file_sha):
        print(
            f"[load] SKIP {source_name}: "
            "gleicher Inhalt bereits erfolgreich geladen"
        )
        return

    s3_uri = f"s3://{BUCKET_RAW}/{key}"
    escaped_uri = s3_uri.replace("'", "''")
    source = f"'{escaped_uri}'"

    source_row_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet({source})"
    ).fetchone()[0]

    checksum = _checksum_file(con, source)

    available_columns = {
        row[0]
        for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet({source})"
        ).fetchall()
    }

    def col_or_null(name: str) -> str:
        return name if name in available_columns else "NULL::VARCHAR"

    try:
        con.execute("BEGIN TRANSACTION")

        con.execute(
            f"""
            INSERT INTO raw.orders
            SELECT
                order_id,
                customer_state,
                product_category,
                {col_or_null('order_status')} AS order_status,
                {col_or_null('order_status_v2')} AS order_status_v2,
                order_purchase_timestamp::TIMESTAMP,
                order_delivered_timestamp::TIMESTAMP,
                order_estimated_delivery_days,
                review_score,
                updated_at::TIMESTAMP,
                current_timestamp,
                '{load_run_id}'
            FROM read_parquet({source})
            """
        )

        loaded_count = con.execute(
            """
            SELECT COUNT(*)
            FROM raw.orders
            WHERE _load_run_id = ?
            """,
            [load_run_id],
        ).fetchone()[0]

        row_count_check = loaded_count == source_row_count

        if not row_count_check:
            raise ValueError(
                f"Row count mismatch: source={source_row_count}, "
                f"loaded={loaded_count}"
            )

        # Erfolgsprotokoll innerhalb derselben Transaktion schreiben.
        con.execute(
            LEDGER_INSERT,
            [
                load_run_id,
                source_name,
                "raw.orders",
                loaded_count,
                row_count_check,
                checksum,
                started_at,
                datetime.now(UTC),
                "success",
                None,
                file_sha,
            ],
        )

        # Rohdaten und Erfolgsprotokoll gemeinsam bestätigen.
        con.execute("COMMIT")

        print(
            f"[load] {source_name}: loaded {loaded_count} rows "
            f"(run_id={load_run_id})"
        )

    except Exception as e:
        con.execute("ROLLBACK")

        # Fehlgeschlagenen Versuch nach dem Rollback protokollieren.
        con.execute(
            LEDGER_INSERT,
            [
                load_run_id,
                source_name,
                "raw.orders",
                0,
                False,
                checksum,
                started_at,
                datetime.now(UTC),
                "failed",
                str(e),
                file_sha,
            ],
        )

        print(f"[load] FAILED {source_name}: {e}")
        raise


def main() -> None:
    client = get_minio_client()
    con = duckdb.connect(WAREHOUSE_PATH)

    try:
        configure_minio(con)

        init_sql_path = os.path.join(
            os.path.dirname(__file__), "init.sql"
        )

        with open(init_sql_path, encoding="utf-8") as f:
            con.execute(f.read())

        files = list_minio_files(client)

        if not files:
            print("[load] No Parquet files found in MinIO.")
            return

        for key in files:
            load_file(con, client, key)

    finally:
        con.close()


if __name__ == "__main__":
    main()