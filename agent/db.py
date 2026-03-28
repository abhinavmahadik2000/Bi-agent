from __future__ import annotations

from pathlib import Path
from time import perf_counter

import duckdb
import pandas as pd


REQUIRED_CSVS = {
    "orders": "orders.csv",
    "order_products_prior": "order_products__prior.csv",
    "order_products_train": "order_products__train.csv",
    "products": "products.csv",
    "aisles": "aisles.csv",
    "departments": "departments.csv",
}


class InstacartDB:
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).expanduser().resolve())
        self.conn = duckdb.connect(self.db_path)
        self.conn.execute("PRAGMA threads=4")

    def close(self) -> None:
        self.conn.close()

    def _table_exists(self, name: str) -> bool:
        query = """
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        LIMIT 1
        """
        return self.conn.execute(query, [name]).fetchone() is not None

    def _view_exists(self, name: str) -> bool:
        query = """
        SELECT 1
        FROM information_schema.views
        WHERE lower(table_name) = lower(?)
        LIMIT 1
        """
        return self.conn.execute(query, [name]).fetchone() is not None

    @staticmethod
    def _csv_path(dataset_dir: str, filename: str) -> str:
        return str((Path(dataset_dir) / filename).resolve())

    def _validate_dataset(self, dataset_dir: str) -> None:
        missing = []
        for filename in REQUIRED_CSVS.values():
            p = Path(dataset_dir) / filename
            if not p.exists():
                missing.append(str(p))
        if missing:
            raise FileNotFoundError(f"Missing required CSV files: {missing}")

    def ensure_ingested(self, dataset_dir: str, force: bool = False) -> dict:
        self._validate_dataset(dataset_dir)

        timings: dict[str, float] = {}
        create_needed = force or any(not self._table_exists(table) for table in REQUIRED_CSVS)

        if create_needed:
            for table, filename in REQUIRED_CSVS.items():
                start = perf_counter()
                csv_path = self._csv_path(dataset_dir, filename)
                self.conn.execute(
                    f"""
                    CREATE OR REPLACE TABLE {table} AS
                    SELECT *
                    FROM read_csv_auto('{csv_path}', HEADER=TRUE)
                    """
                )
                timings[table] = perf_counter() - start

        self.conn.execute(
            """
            CREATE OR REPLACE VIEW order_products_all AS
            SELECT
                order_id,
                product_id,
                add_to_cart_order,
                reordered,
                'prior'::VARCHAR AS source_eval_set
            FROM order_products_prior
            UNION ALL
            SELECT
                order_id,
                product_id,
                add_to_cart_order,
                reordered,
                'train'::VARCHAR AS source_eval_set
            FROM order_products_train
            """
        )

        self.conn.execute(
            """
            CREATE OR REPLACE VIEW fact_order_lines AS
            SELECT
                op.order_id,
                o.user_id,
                o.eval_set,
                o.order_number,
                o.order_dow,
                o.order_hour_of_day,
                o.days_since_prior_order,
                op.product_id,
                op.add_to_cart_order,
                op.reordered,
                op.source_eval_set,
                p.product_name,
                p.aisle_id,
                p.department_id
            FROM order_products_all op
            JOIN orders o ON op.order_id = o.order_id
            JOIN products p ON op.product_id = p.product_id
            """
        )

        return timings

    def execute_df(self, sql: str) -> pd.DataFrame:
        return self.conn.execute(sql).df()

    def health_summary(self) -> dict:
        summary = {}
        for table in REQUIRED_CSVS:
            summary[table] = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        summary["order_products_all"] = self.conn.execute("SELECT COUNT(*) FROM order_products_all").fetchone()[0]
        return summary
