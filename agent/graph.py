from __future__ import annotations

from langgraph.graph import END, StateGraph

from .config import AppConfig
from .db import InstacartDB
from .llm import SQLGenerator
from .memory import ConversationMemory
from .nodes import (
    chart_node,
    make_execute_node,
    make_generate_node,
    make_plan_node,
    make_validate_node,
)
from .planner import SQLPlanner
from .sql_validator import SQLValidator
from .state import SQLAgentState


def _route_after_validate(state: SQLAgentState) -> str:
    if state.get("validation_passed"):
        return "execute"
    if state.get("retry_count", 0) <= state.get("max_retries", 2):
        return "generate"
    return END


def _route_after_execute(state: SQLAgentState) -> str:
    if state.get("df") is not None:
        return "chart"
    if state.get("retry_count", 0) <= state.get("max_retries", 2):
        return "generate"
    return END


def build_graph(config: AppConfig, db: InstacartDB):
    """Compile and return the LangGraph SQL agent graph."""
    planner = SQLPlanner(model=config.llm_model, api_key=config.anthropic_api_key)
    generator = SQLGenerator(model=config.llm_model, api_key=config.anthropic_api_key)
    validator = SQLValidator(max_rows=config.max_query_rows)

    plan_node = make_plan_node(planner)
    generate_node = make_generate_node(generator)
    validate_node = make_validate_node(validator)
    execute_node = make_execute_node(db)

    graph = StateGraph(SQLAgentState)

    graph.add_node("plan", plan_node)
    graph.add_node("generate", generate_node)
    graph.add_node("validate", validate_node)
    graph.add_node("execute", execute_node)
    graph.add_node("chart", chart_node)

    graph.set_entry_point("plan")
    graph.add_edge("plan", "generate")
    graph.add_edge("generate", "validate")

    graph.add_conditional_edges(
        "validate",
        _route_after_validate,
        {"execute": "execute", "generate": "generate", END: END},
    )

    graph.add_conditional_edges(
        "execute",
        _route_after_execute,
        {"chart": "chart", "generate": "generate", END: END},
    )

    graph.add_edge("chart", END)

    return graph.compile()


def make_initial_state(
    question: str,
    memory: ConversationMemory,
    max_retries: int,
) -> SQLAgentState:
    return SQLAgentState(
        question=question,
        memory_context=memory.to_prompt_context(),
        plan="",
        sql="",
        correction_context="",
        retry_count=0,
        max_retries=max_retries,
        validation_passed=False,
        df=None,
        chart_spec=None,
        error=None,
        warnings=[],
    )
