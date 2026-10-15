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
LOCAL_RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "staging_minio", "_local_raw")

def _checksum_file(filepath: str) -> str:
    df = pd.read_parquet(filepath)
    ids = sorted(df["order_id"].tolist())
    return hashlib.sha256(",".join(ids).encode()).hexdigest()


def load_file(con: duckdb.DuckDBPyConnection, filepath: str) -> str:
    load_run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    source_row_count = len(pd.read_parquet(filepath))
    checksum = _checksum_file(filepath)

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
            f"SELECT COUNT(*) FROM raw.orders WHERE _load_run_id = '{load_run_id}'"
        ).fetchone()[0]

        row_count_check = loaded_count == source_row_count
        if not row_count_check:
            raise ValueError(
                f"Row count mismatch: source={source_row_count}, loaded={loaded_count}"
            )

        con.execute(
            """
            INSERT INTO pipeline_metadata.load_runs
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                load_run_id, filepath, "raw.orders", loaded_count, row_count_check,
                checksum, started_at, datetime.now(timezone.utc), "success", None,
            ],
        )

        con.execute("COMMIT")
        print(f"[load] {filepath}: loaded {loaded_count} rows (run_id={load_run_id})")

    except Exception as e:
        con.execute("ROLLBACK")
        con.execute(
            """
            INSERT INTO pipeline_metadata.load_runs
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                load_run_id, filepath, "raw.orders", 0, False,
                checksum, started_at, datetime.now(timezone.utc), "failed", str(e),
            ],
        )
        print(f"[load] FAILED {filepath}: {e}")
        raise


def main():
    con = duckdb.connect(WAREHOUSE_PATH)
    con.execute(open(os.path.join(os.path.dirname(__file__), "init.sql")).read())

    files = sorted(glob.glob(os.path.join(LOCAL_RAW_DIR, "orders_*.parquet")))
    if not files:
        print("[load] No staged files found.")
        return

    for filepath in files:
        load_file(con, filepath)

    con.close()


if __name__ == "__main__":
    main()
