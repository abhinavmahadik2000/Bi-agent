#!/usr/bin/env python3
"""
One-time build-time script: generates data_plan.md at the project root.

Usage:
    python scripts/generate_data_plan.py [--db-path PATH] [--dataset-dir DIR] [--api-key KEY]

The script:
  1. Connects to DuckDB and ensures all tables/views are ingested.
  2. Samples top 10 rows from every table and view.
  3. Sends the schema + samples to Claude.
  4. Writes the generated data_plan.md to the project root.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow running from project root or scripts/ directory
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import duckdb
from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")


TABLES_AND_VIEWS = [
    "orders",
    "order_products_prior",
    "order_products_train",
    "products",
    "aisles",
    "departments",
    "order_products_all",
    "fact_order_lines",
]

SCHEMA_CONTEXT = """
## Database: Instacart Market Basket (DuckDB)

### Base Tables

**orders** — one row per order placed by a user
- order_id BIGINT (primary key)
- user_id BIGINT
- eval_set VARCHAR — which dataset split this order belongs to ('prior', 'train', 'test')
- order_number BIGINT — sequence number of the order for this user (1 = first order)
- order_dow BIGINT — day of week (0=Saturday, 1=Sunday, ..., 6=Friday)
- order_hour_of_day BIGINT — hour of day (0–23)
- days_since_prior_order DOUBLE — days since this user's previous order (NULL for first order)

**order_products_prior** — products in all 'prior' set orders
- order_id BIGINT (FK → orders.order_id)
- product_id BIGINT (FK → products.product_id)
- add_to_cart_order BIGINT — position in which product was added to cart
- reordered BIGINT — 1 if this product was ordered before by this user, 0 otherwise

**order_products_train** — products in 'train' set orders (same schema as order_products_prior)
- order_id, product_id, add_to_cart_order, reordered

**products** — product master
- product_id BIGINT (primary key)
- product_name VARCHAR
- aisle_id BIGINT (FK → aisles.aisle_id)
- department_id BIGINT (FK → departments.department_id)

**aisles** — aisle dimension
- aisle_id BIGINT (primary key)
- aisle VARCHAR

**departments** — department dimension
- department_id BIGINT (primary key)
- department VARCHAR

### Views

**order_products_all** — UNION of prior + train sets
- order_id, product_id, add_to_cart_order, reordered
- source_eval_set VARCHAR — 'prior' or 'train'

**fact_order_lines** — pre-joined fact table (order_products_all + orders + products)
- order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day,
  days_since_prior_order, product_id, add_to_cart_order, reordered,
  source_eval_set, product_name, aisle_id, department_id

### Key Join Paths
- order_products_all.order_id = orders.order_id
- order_products_all.product_id = products.product_id
- products.aisle_id = aisles.aisle_id
- products.department_id = departments.department_id

### Standard Metric Definitions
- reorder_rate = AVG(CAST(reordered AS DOUBLE))
- avg_basket_position = AVG(add_to_cart_order)
- order_frequency_proxy = AVG(days_since_prior_order) — always filter days_since_prior_order IS NOT NULL for this metric
""".strip()


GENERATION_PROMPT = """You are a data documentation expert. I am building an AI SQL agent for Instacart analytics.

Below is the database schema and sample rows (top 10) from each table and view in DuckDB.

Your task: Write a comprehensive `data_plan.md` file that the SQL agent will load as its primary context when writing queries.

The file must include:
1. **Overview** — what this database contains, at what granularity, and how tables relate
2. **Table-by-table documentation** — for each table/view:
   - Purpose in plain English
   - Column descriptions with actual example values from the samples
   - Nullability notes (e.g., days_since_prior_order is NULL for a user's first order)
   - Cardinality estimates where obvious from context
3. **Recommended join paths** — written as both prose and SQL snippet examples
4. **Metric dictionary** — precise DuckDB SQL expressions for each standard metric
5. **Known gotchas** — common mistakes to avoid (e.g., wrong eval_set filter, double-counting from using both prior and train, etc.)
6. **DuckDB dialect notes** — any syntax specifics relevant to this dataset

Write in clean Markdown. Be precise, specific, and use real example values from the samples throughout. This file will be read by an LLM to write SQL queries, so maximize clarity and completeness.

---

## Schema

{schema}

---

## Sample Data (top 10 rows per table/view)

{samples}

---

Now write the complete data_plan.md content:"""


def _df_to_markdown(df) -> str:
    if df.empty:
        return "(empty)"
    return df.to_markdown(index=False)


def _collect_samples(conn: duckdb.DuckDBPyConnection) -> str:
    parts = []
    for name in TABLES_AND_VIEWS:
        try:
            df = conn.execute(f"SELECT * FROM {name} LIMIT 10").df()
            parts.append(f"### {name}\n\n{_df_to_markdown(df)}")
        except Exception as exc:
            parts.append(f"### {name}\n\n(error: {exc})")
    return "\n\n".join(parts)


def generate(db_path: str, dataset_dir: str, api_key: str, output_path: Path) -> None:
    from agent.db import InstacartDB

    print(f"Connecting to DuckDB at {db_path} ...")
    db = InstacartDB(db_path)

    print(f"Ensuring dataset ingested from {dataset_dir} ...")
    timings = db.ensure_ingested(dataset_dir)
    if timings:
        for table, secs in timings.items():
            print(f"  Ingested {table} in {secs:.1f}s")

    print("Sampling top 10 rows from each table/view ...")
    samples_md = _collect_samples(db.conn)

    prompt = GENERATION_PROMPT.format(schema=SCHEMA_CONTEXT, samples=samples_md)

    print("Calling Claude to generate data_plan.md ...")
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )

    content = ""
    for block in response.content:
        if getattr(block, "type", "") == "text":
            content += block.text

    output_path.write_text(content.strip(), encoding="utf-8")
    print(f"\ndata_plan.md written to {output_path}")
    print(f"Characters: {len(content):,}")
    db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate data_plan.md for nLSql-v2")
    parser.add_argument("--db-path", default=str(PROJECT_ROOT / "instacart.duckdb"))
    parser.add_argument("--dataset-dir", default=str(PROJECT_ROOT / "dataset"))
    parser.add_argument("--api-key", default=os.getenv("ANTHROPIC_API_KEY", ""))
    parser.add_argument("--output", default=str(PROJECT_ROOT / "data_plan.md"))
    args = parser.parse_args()

    if not args.api_key:
        print("Error: ANTHROPIC_API_KEY not set. Use --api-key or set the env var.")
        sys.exit(1)

    generate(
        db_path=args.db_path,
        dataset_dir=args.dataset_dir,
        api_key=args.api_key,
        output_path=Path(args.output),
    )


if __name__ == "__main__":
    main()
