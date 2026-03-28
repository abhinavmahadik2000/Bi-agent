from __future__ import annotations

from pathlib import Path


_FALLBACK_SCHEMA = """
Database objects:

orders(
  order_id BIGINT,
  user_id BIGINT,
  eval_set VARCHAR,
  order_number BIGINT,
  order_dow BIGINT,
  order_hour_of_day BIGINT,
  days_since_prior_order DOUBLE
)

order_products_prior(
  order_id BIGINT,
  product_id BIGINT,
  add_to_cart_order BIGINT,
  reordered BIGINT
)

order_products_train(
  order_id BIGINT,
  product_id BIGINT,
  add_to_cart_order BIGINT,
  reordered BIGINT
)

products(
  product_id BIGINT,
  product_name VARCHAR,
  aisle_id BIGINT,
  department_id BIGINT
)

aisles(aisle_id BIGINT, aisle VARCHAR)
departments(department_id BIGINT, department VARCHAR)

order_products_all view columns:
  order_id, product_id, add_to_cart_order, reordered, source_eval_set

fact_order_lines view columns:
  order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day,
  days_since_prior_order, product_id, add_to_cart_order, reordered,
  source_eval_set, product_name, aisle_id, department_id

Join paths:
- order_products_all.order_id = orders.order_id
- order_products_all.product_id = products.product_id
- products.aisle_id = aisles.aisle_id
- products.department_id = departments.department_id

Metric dictionary:
- reorder_rate = AVG(CAST(reordered AS DOUBLE))
- avg_basket_position = AVG(add_to_cart_order)
- order_frequency_proxy = AVG(days_since_prior_order) with days_since_prior_order IS NOT NULL
""".strip()

_SQL_RULES = """
Rules:
1) Return exactly one SQL statement only.
2) Read-only analytics query only (SELECT / CTE SELECT).
3) Never use INSERT/UPDATE/DELETE/DROP/ALTER/COPY/ATTACH/DETACH.
4) Do NOT globally filter days_since_prior_order IS NOT NULL unless metric requires it.
5) For large-table queries, prefer aggregation and LIMIT results where reasonable.
6) Use explicit table aliases.
7) If question is impossible with available columns (e.g., calendar month/year), return a best-effort SQL using available dimensions.
8) Output only SQL, no markdown and no explanations.
""".strip()


def _load_data_plan() -> str | None:
    # Look for data_plan.md relative to the project root (one level up from agent/)
    candidates = [
        Path(__file__).parent.parent / "data_plan.md",
        Path("./data_plan.md").resolve(),
    ]
    for path in candidates:
        if path.exists():
            return path.read_text(encoding="utf-8").strip()
    return None


def system_prompt() -> str:
    return (
        "You are a senior DuckDB analytics engineer for Instacart BI. "
        "Generate SQL that is correct, efficient, and safe for read-only analytics."
    )


def planner_system_prompt() -> str:
    return (
        "You are a DuckDB query planner for Instacart BI analytics. "
        "Given a user question and the database schema, produce a concise structured query plan. "
        "Do NOT write SQL. Output a plain-text plan only."
    )


def schema_and_rules_prompt() -> str:
    data_plan = _load_data_plan()
    if data_plan:
        schema_section = data_plan
    else:
        schema_section = _FALLBACK_SCHEMA
    return f"{schema_section}\n\n{_SQL_RULES}"


def planner_user_prompt(question: str, memory_context: str) -> str:
    data_plan = _load_data_plan()
    schema_section = data_plan if data_plan else _FALLBACK_SCHEMA
    parts = [
        "Schema and data context:",
        schema_section,
        "",
        "Conversation context:",
        memory_context or "(none)",
        "",
        f"User question: {question}",
        "",
        "Produce a structured plan with these fields:",
        "Tables: <which tables or views to use>",
        "Joins: <join path needed>",
        "Metric: <metric definition to apply, or 'none'>",
        "Filter: <key WHERE conditions, or 'none'>",
        "Pitfalls: <known gotchas to avoid, or 'none'>",
    ]
    return "\n".join(parts)


def sql_user_prompt(
    question: str,
    plan: str,
    memory_context: str,
    correction_context: str | None = None,
) -> str:
    parts = [
        "Conversation context:",
        memory_context or "(none)",
        "",
        "Query plan:",
        plan or "(none — generate SQL directly from the question)",
        "",
        f"User question: {question}",
    ]
    if correction_context:
        parts.extend(["", "Correction context:", correction_context])
    return "\n".join(parts)
