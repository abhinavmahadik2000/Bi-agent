from __future__ import annotations

from typing import Any, Optional
from typing_extensions import TypedDict


class SQLAgentState(TypedDict):
    question: str               # Original user question
    memory_context: str         # Formatted last-N turns from ConversationMemory
    plan: str                   # Planner output: tables, joins, metric, pitfalls
    sql: str                    # Current SQL (may be updated by validator auto-fix)
    correction_context: str     # Error feedback fed back to generate_node on retry
    retry_count: int            # How many retries have been used so far
    max_retries: int            # Max retries from config
    validation_passed: bool     # Set by validate_node
    df: Optional[Any]           # pandas DataFrame on successful execution
    chart_spec: Optional[Any]   # ChartSpec on success
    error: Optional[str]        # Final error message if all retries exhausted
    warnings: list[str]         # Non-fatal warnings from validator or execution
