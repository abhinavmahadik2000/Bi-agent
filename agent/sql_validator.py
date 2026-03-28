from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class SQLValidationResult:
    valid: bool
    sql: str
    reason: str = ""
    warnings: list[str] = field(default_factory=list)


class SQLValidator:
    BLOCKED_KEYWORDS = (
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "ALTER",
        "COPY",
        "ATTACH",
        "DETACH",
        "TRUNCATE",
        "EXPORT",
        "IMPORT",
    )

    def __init__(self, max_rows: int = 5000) -> None:
        self.max_rows = max_rows

    @staticmethod
    def _normalize(sql: str) -> str:
        sql = sql.strip()
        if sql.startswith("```"):
            sql = re.sub(r"^```[a-zA-Z]*\n", "", sql)
            sql = sql.replace("```", "").strip()
        return sql

    @staticmethod
    def _is_single_statement(sql: str) -> bool:
        parts = [part.strip() for part in sql.split(";") if part.strip()]
        return len(parts) == 1

    @staticmethod
    def _is_read_query(sql: str) -> bool:
        head = sql.lstrip().upper()
        return head.startswith("SELECT") or head.startswith("WITH")

    @staticmethod
    def _has_aggregation(sql: str) -> bool:
        return bool(
            re.search(
                r"\bgroup\s+by\b|\b(count|sum|avg|min|max)\s*\(|\bdistinct\b",
                sql,
                flags=re.IGNORECASE,
            )
        )

    @staticmethod
    def _extract_limit(sql: str) -> int | None:
        m = re.search(r"\bLIMIT\s+(\d+)\b", sql, flags=re.IGNORECASE)
        if not m:
            return None
        return int(m.group(1))

    def validate(self, raw_sql: str) -> SQLValidationResult:
        sql = self._normalize(raw_sql)
        if not sql:
            return SQLValidationResult(valid=False, sql="", reason="Empty SQL generated")

        if not self._is_single_statement(sql):
            return SQLValidationResult(valid=False, sql=sql, reason="Multiple SQL statements are not allowed")

        for keyword in self.BLOCKED_KEYWORDS:
            if re.search(rf"\b{keyword}\b", sql, flags=re.IGNORECASE):
                return SQLValidationResult(valid=False, sql=sql, reason=f"Blocked keyword detected: {keyword}")

        if not self._is_read_query(sql):
            return SQLValidationResult(valid=False, sql=sql, reason="Only SELECT queries are allowed")

        warnings: list[str] = []
        limit = self._extract_limit(sql)
        if limit is not None and limit > self.max_rows:
            sql = re.sub(r"\bLIMIT\s+\d+\b", f"LIMIT {self.max_rows}", sql, flags=re.IGNORECASE)
            warnings.append(f"LIMIT reduced to {self.max_rows} for safety")

        if limit is None and not self._has_aggregation(sql):
            if sql.rstrip().endswith(";"):
                sql = sql.rstrip()[:-1] + f" LIMIT {self.max_rows};"
            else:
                sql = sql.rstrip() + f" LIMIT {self.max_rows}"
            warnings.append(f"LIMIT {self.max_rows} auto-appended to non-aggregated query")

        if not sql.rstrip().endswith(";"):
            sql = sql.rstrip() + ";"

        return SQLValidationResult(valid=True, sql=sql, warnings=warnings)
