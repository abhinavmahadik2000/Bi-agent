from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from agent.chart_selector import ChartSpec, render_chart
from agent.config import AppConfig
from agent.db import InstacartDB
from agent.graph import build_graph, make_initial_state
from agent.memory import ConversationMemory

load_dotenv()

st.set_page_config(page_title="Instacart BI Agent v2", layout="wide")
st.title("Instacart BI Agent v2")
st.caption("Powered by LangGraph · Plan → Generate → Validate → Execute")


# ── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Configuration")

    dataset_dir = st.text_input("Dataset directory", value="./dataset")
    db_path = st.text_input("DuckDB path", value="./instacart.duckdb")

    st.caption("LLM provider: Anthropic")
    llm_model = st.text_input("LLM model", value="claude-sonnet-4-6")
    api_key = st.text_input(
        "ANTHROPIC_API_KEY",
        type="password",
        value=os.getenv("ANTHROPIC_API_KEY", ""),
    )

    max_display_rows = st.number_input("Max rows for display", min_value=10, max_value=2000, value=300)
    max_query_rows = st.number_input("Max query row limit", min_value=100, max_value=50000, value=5000)
    max_retries = st.number_input("SQL retry count", min_value=0, max_value=5, value=2)
    force_reingest = st.checkbox("Force re-ingest CSVs", value=False)

    data_plan_status = "data_plan.md not found — using fallback schema"
    from pathlib import Path
    if Path("./data_plan.md").exists():
        size_kb = Path("./data_plan.md").stat().st_size // 1024
        data_plan_status = f"data_plan.md loaded ({size_kb} KB)"
    st.info(data_plan_status)

    if st.button("Initialize / Reload", use_container_width=True):
        for key in ("_db", "_graph", "_memory", "_config"):
            st.session_state.pop(key, None)
        st.session_state.chat_history = []


# ── Initialize DB + Graph ─────────────────────────────────────────────────────

@st.cache_resource
def _init_db(db_path: str, dataset_dir: str, force: bool) -> tuple[InstacartDB | None, str | None]:
    try:
        config = AppConfig._resolve_dataset_dir(dataset_dir)
        db = InstacartDB(db_path)
        db.ensure_ingested(config, force=force)
        return db, None
    except Exception as exc:
        return None, str(exc)


config = AppConfig(
    dataset_dir=AppConfig._resolve_dataset_dir(dataset_dir),
    db_path=db_path,
    llm_model=llm_model,
    anthropic_api_key=api_key,
    max_display_rows=int(max_display_rows),
    max_query_rows=int(max_query_rows),
    max_retries=int(max_retries),
)

db, init_error = _init_db(
    db_path=db_path,
    dataset_dir=dataset_dir,
    force=force_reingest,
)

if init_error:
    st.error(f"Database initialization failed: {init_error}")
    st.stop()

if db:
    with st.expander("Database health", expanded=False):
        try:
            st.json(db.health_summary())
        except Exception as exc:
            st.warning(f"Health check failed: {exc}")

# Build the LangGraph compiled graph (cached per config values)
@st.cache_resource
def _build_graph(db_path: str, llm_model: str, api_key: str, max_query_rows: int, max_retries: int):
    cfg = AppConfig(
        dataset_dir="./dataset",
        db_path=db_path,
        llm_model=llm_model,
        anthropic_api_key=api_key,
        max_query_rows=max_query_rows,
        max_retries=max_retries,
    )
    # db is already initialized; re-open a read connection for the graph
    _db = InstacartDB(db_path)
    return build_graph(cfg, _db)


if not api_key:
    st.warning("Set your ANTHROPIC_API_KEY in the sidebar or .env file to start querying.")
    st.stop()

compiled_graph = _build_graph(
    db_path=db_path,
    llm_model=llm_model,
    api_key=api_key,
    max_query_rows=int(max_query_rows),
    max_retries=int(max_retries),
)

# Per-session conversation memory (not cached — unique per browser session)
if "memory" not in st.session_state:
    st.session_state.memory = ConversationMemory()

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []


# ── Render chat history ───────────────────────────────────────────────────────

for _hist_idx, item in enumerate(st.session_state.chat_history):
    if item["role"] == "user":
        st.chat_message("user").write(item["content"])
        continue

    with st.chat_message("assistant"):
        if item.get("error"):
            st.error(item["error"])
            if item.get("sql"):
                with st.expander("Last attempted SQL"):
                    st.code(item["sql"], language="sql")
            continue

        st.write(item.get("summary", "Done"))

        records = item.get("records", [])
        columns = item.get("columns", [])
        if records and columns:
            df = pd.DataFrame(records, columns=columns)
            df_display = df.head(int(max_display_rows))
            st.dataframe(df_display, use_container_width=True)

            chart_spec_raw = item.get("chart_spec")
            if chart_spec_raw:
                spec = ChartSpec(**chart_spec_raw)
                fig = render_chart(df_display, spec)
                if fig is not None:
                    st.plotly_chart(fig, use_container_width=True, key=f"chart_hist_{_hist_idx}")

        if item.get("warnings"):
            st.warning("\n".join(item["warnings"]))

        if item.get("plan"):
            with st.expander("Query plan", expanded=False):
                st.text(item["plan"])

        with st.expander("SQL", expanded=False):
            st.code(item.get("sql", ""), language="sql")


# ── Chat input ────────────────────────────────────────────────────────────────

if prompt := st.chat_input("Ask a question about Instacart data"):
    st.session_state.chat_history.append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            initial_state = make_initial_state(
                question=prompt,
                memory=st.session_state.memory,
                max_retries=int(max_retries),
            )
            try:
                final_state = compiled_graph.invoke(initial_state)
            except Exception as exc:
                final_state = {**initial_state, "error": f"Graph execution failed: {exc}"}

        payload: dict = {
            "role": "assistant",
            "sql": final_state.get("sql", ""),
            "plan": final_state.get("plan", ""),
            "warnings": final_state.get("warnings") or [],
        }

        df = final_state.get("df")
        error = final_state.get("error")

        if error and df is None:
            payload["error"] = error
            st.error(error)
            if payload["sql"]:
                with st.expander("Last attempted SQL"):
                    st.code(payload["sql"], language="sql")
        else:
            total_rows = int(df.shape[0]) if df is not None else 0
            df_display = df.head(int(max_display_rows)) if df is not None else pd.DataFrame()

            summary = f"Returned {total_rows} rows"
            if total_rows > int(max_display_rows):
                summary += f" (showing first {int(max_display_rows)})."
            else:
                summary += "."
            payload["summary"] = summary
            payload["columns"] = [str(c) for c in df_display.columns]
            payload["records"] = df_display.to_dict(orient="records")

            chart_spec = final_state.get("chart_spec")
            payload["chart_spec"] = chart_spec.to_dict() if chart_spec else None

            st.write(summary)
            if not df_display.empty:
                st.dataframe(df_display, use_container_width=True)

            if chart_spec:
                fig = render_chart(df_display, chart_spec)
                if fig is not None:
                    st.plotly_chart(fig, use_container_width=True, key=f"chart_live_{len(st.session_state.chat_history)}")

            if payload["warnings"]:
                st.warning("\n".join(payload["warnings"]))

            # Update conversation memory on success
            if df is not None:
                st.session_state.memory.add(prompt, payload["sql"], df)

        if payload.get("plan"):
            with st.expander("Query plan", expanded=False):
                st.text(payload["plan"])

        with st.expander("SQL", expanded=False):
            st.code(payload["sql"], language="sql")

    st.session_state.chat_history.append(payload)
