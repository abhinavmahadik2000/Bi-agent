from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

import pandas as pd


@dataclass
class TurnRecord:
    question: str
    sql: str
    row_count: int
    columns: list[str]
    semantic_tags: list[str] = field(default_factory=list)


class ConversationMemory:
    def __init__(self, max_turns: int = 8) -> None:
        self.turns: deque[TurnRecord] = deque(maxlen=max_turns)

    @staticmethod
    def _infer_tags(question: str, columns: list[str]) -> list[str]:
        q = question.lower()
        tags: list[str] = []
        for token in ("department", "aisle", "product", "reorder", "basket", "correlation", "top", "trend"):
            if token in q:
                tags.append(token)
        for col in columns:
            if col.lower() in {"department", "aisle", "product_name", "reordered", "add_to_cart_order"}:
                tags.append(col.lower())
        return sorted(set(tags))

    def add(self, question: str, sql: str, df: pd.DataFrame) -> None:
        self.turns.append(
            TurnRecord(
                question=question,
                sql=sql,
                row_count=int(df.shape[0]),
                columns=[str(c) for c in df.columns],
                semantic_tags=self._infer_tags(question, [str(c) for c in df.columns]),
            )
        )

    def to_prompt_context(self, max_turns: int = 3) -> str:
        if not self.turns:
            return ""
        selected = list(self.turns)[-max_turns:]
        lines: list[str] = []
        for idx, turn in enumerate(selected, start=1):
            lines.append(f"Turn {idx} question: {turn.question}")
            lines.append(f"Turn {idx} sql: {turn.sql}")
            lines.append(f"Turn {idx} result: {turn.row_count} rows, columns={turn.columns}")
            lines.append(f"Turn {idx} tags: {turn.semantic_tags}")
        return "\n".join(lines)
