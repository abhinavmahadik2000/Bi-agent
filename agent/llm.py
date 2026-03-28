from __future__ import annotations

import json
import re

from .prompts import schema_and_rules_prompt, sql_user_prompt, system_prompt


class SQLGenerator:
    def __init__(self, model: str, api_key: str) -> None:
        self.model = model
        self.api_key = api_key

    @staticmethod
    def _extract_sql(text: str) -> str:
        value = text.strip()
        if not value:
            return value

        if value.startswith("```"):
            value = re.sub(r"^```[a-zA-Z]*\n", "", value)
            value = value.replace("```", "").strip()

        if value.startswith("{") and value.endswith("}"):
            try:
                payload = json.loads(value)
                if isinstance(payload, dict) and "sql" in payload:
                    return str(payload["sql"]).strip()
            except json.JSONDecodeError:
                pass

        return value

    def generate_sql(
        self,
        question: str,
        plan: str = "",
        memory_context: str = "",
        correction_context: str | None = None,
    ) -> str:
        if not self.api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is missing")

        import anthropic

        client = anthropic.Anthropic(api_key=self.api_key)
        response = client.messages.create(
            model=self.model,
            max_tokens=1000,
            temperature=0,
            system=f"{system_prompt()}\n\n{schema_and_rules_prompt()}",
            messages=[
                {
                    "role": "user",
                    "content": sql_user_prompt(
                        question=question,
                        plan=plan,
                        memory_context=memory_context,
                        correction_context=correction_context,
                    ),
                }
            ],
        )

        content = ""
        if response.content:
            for block in response.content:
                if getattr(block, "type", "") == "text":
                    content += block.text
        return self._extract_sql(content)
