from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .chart_selector import choose_chart
from .state import SQLAgentState

if TYPE_CHECKING:
    from .db import InstacartDB
    from .llm import SQLGenerator
    from .planner import SQLPlanner
    from .sql_validator import SQLValidator


def make_plan_node(planner: "SQLPlanner"):
    """Returns a LangGraph node that generates a query plan."""

    def plan_node(state: SQLAgentState) -> dict[str, Any]:
        try:
            plan_text = planner.plan(
                question=state["question"],
                memory_context=state["memory_context"],
            )
        except Exception:
            plan_text = ""
        return {"plan": plan_text}

    return plan_node


def make_generate_node(generator: "SQLGenerator"):
    """Returns a LangGraph node that generates SQL from the plan."""

    def generate_node(state: SQLAgentState) -> dict[str, Any]:
        try:
            sql = generator.generate_sql(
                question=state["question"],
                plan=state.get("plan", ""),
                memory_context=state.get("memory_context", ""),
                correction_context=state.get("correction_context") or None,
            )
        except Exception as exc:
            sql = ""
        return {"sql": sql}

    return generate_node


def make_validate_node(validator: "SQLValidator"):
    """Returns a LangGraph node that validates the SQL and auto-fixes it where possible."""

    def validate_node(state: SQLAgentState) -> dict[str, Any]:
        result = validator.validate(state.get("sql", ""))
        if result.valid:
            return {
                "sql": result.sql,
                "validation_passed": True,
                "warnings": result.warnings,
            }
        correction = (
            f"Previous SQL was rejected by the validator. Reason: {result.reason}. "
            f"Previous SQL:\n{result.sql}\n\nPlease fix the SQL."
        )
        return {
            "sql": result.sql,
            "validation_passed": False,
            "correction_context": correction,
            "retry_count": state.get("retry_count", 0) + 1,
            "warnings": [],
        }

    return validate_node


def make_execute_node(db: "InstacartDB"):
    """Returns a LangGraph node that executes the SQL against DuckDB."""

    def execute_node(state: SQLAgentState) -> dict[str, Any]:
        try:
            df = db.execute_df(state["sql"])
            return {"df": df, "error": None}
        except Exception as exc:
            correction = (
                f"DuckDB execution error: {exc}. "
                f"Previous SQL:\n{state['sql']}\n\nPlease fix the SQL."
            )
            return {
                "df": None,
                "error": str(exc),
                "correction_context": correction,
                "retry_count": state.get("retry_count", 0) + 1,
            }

    return execute_node


def chart_node(state: SQLAgentState) -> dict[str, Any]:
    """Selects a chart type for the result DataFrame."""
    df = state.get("df")
    if df is None or df.empty:
        return {"chart_spec": None}
    try:
        spec = choose_chart(df, state["question"])
        return {"chart_spec": spec}
    except Exception:
        return {"chart_spec": None}
