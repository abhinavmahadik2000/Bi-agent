from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class AppConfig:
    dataset_dir: str
    db_path: str
    llm_provider: str = "anthropic"
    llm_model: str = "claude-sonnet-4-6"
    anthropic_api_key: str = ""
    max_display_rows: int = 300
    max_query_rows: int = 5000
    max_retries: int = 2
    data_plan_path: str = "./data_plan.md"

    @staticmethod
    def _resolve_dataset_dir(raw_path: str | None) -> str:
        candidates = [
            raw_path,
            os.getenv("DATASET_DIR"),
            "/dataset",
            "./dataset",
        ]
        for candidate in candidates:
            if not candidate:
                continue
            p = Path(candidate).expanduser().resolve()
            if p.exists() and p.is_dir():
                return str(p)
        return str(Path("./dataset").resolve())

    @classmethod
    def from_env(cls, dataset_dir: str | None = None, db_path: str | None = None) -> "AppConfig":
        return cls(
            dataset_dir=cls._resolve_dataset_dir(dataset_dir),
            db_path=db_path or os.getenv("DUCKDB_PATH", "./instacart.duckdb"),
            llm_provider=os.getenv("LLM_PROVIDER", "anthropic"),
            llm_model=os.getenv("LLM_MODEL", "claude-sonnet-4-6"),
            anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", ""),
            max_display_rows=int(os.getenv("MAX_DISPLAY_ROWS", "300")),
            max_query_rows=int(os.getenv("MAX_QUERY_ROWS", "5000")),
            max_retries=int(os.getenv("MAX_SQL_RETRIES", "2")),
            data_plan_path=os.getenv("DATA_PLAN_PATH", "./data_plan.md"),
        )
