import glob
import hashlib
import os
import uuid
from datetime import datetime, timezone

import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH", "./warehouse/ecom.duckdb")
LOCAL_RAW_DIR = os.path.join(
    os.path.dirname(__file__), "..", "staging_minio", "_local_raw"
)

LEDGER_INSERT = """
    INSERT INTO pipeline_metadata.load_runs
        (load_run_id, source_file, table_name, rows_loaded, row_count_check,
         checksum, started_at, finished_at, status, error_message, file_sha256)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _file_sha256(filepath: str) -> str:
    """Berechnet den SHA-256-Hash der gesamten Datei."""
    h = hashlib.sha256()

    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)

    return h.hexdigest()


def _already_loaded(
    con: duckdb.DuckDBPyConnection, file_sha: str
) -> bool:
    """Prüft, ob genau dieser Dateiinhalt bereits erfolgreich geladen wurde."""
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


def _checksum_file(filepath: str) -> str:
    """Berechnet den bisherigen logischen Checksum anhand der Order-IDs."""
    df = pd.read_parquet(filepath)
    ids = sorted(df["order_id"].tolist())

    return hashlib.sha256(",".join(ids).encode()).hexdigest()


def load_file(con: duckdb.DuckDBPyConnection, filepath: str) -> None:
    load_run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    source_row_count = len(pd.read_parquet(filepath))
    checksum = _checksum_file(filepath)

    source_name = os.path.basename(filepath)
    file_sha = _file_sha256(filepath)

    # Bereits erfolgreich geladene Datei überspringen.
    if _already_loaded(con, file_sha):
        print(
            f"[load] SKIP {source_name}: "
            "gleicher Inhalt bereits erfolgreich geladen"
        )
        return

    available_columns = set(pd.read_parquet(filepath).columns)

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
                {col_or_null('order_status')}    AS order_status,
                {col_or_null('order_status_v2')} AS order_status_v2,
                order_purchase_timestamp::TIMESTAMP,
                order_delivered_timestamp::TIMESTAMP,
                order_estimated_delivery_days,
                review_score,
                updated_at::TIMESTAMP,
                current_timestamp,
                '{load_run_id}'
            FROM read_parquet('{filepath}')
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
                datetime.now(timezone.utc),
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
                datetime.now(timezone.utc),
                "failed",
                str(e),
                file_sha,
            ],
        )

        print(f"[load] FAILED {source_name}: {e}")
        raise


def main() -> None:
    con = duckdb.connect(WAREHOUSE_PATH)

    try:
        init_sql_path = os.path.join(
            os.path.dirname(__file__), "init.sql"
        )

        with open(init_sql_path, encoding="utf-8") as f:
            con.execute(f.read())

        files = sorted(
            glob.glob(os.path.join(LOCAL_RAW_DIR, "orders_*.parquet"))
        )

        if not files:
            print("[load] No staged files found.")
            return

        for filepath in files:
            load_file(con, filepath)

    finally:
        con.close()


if __name__ == "__main__":
    main()