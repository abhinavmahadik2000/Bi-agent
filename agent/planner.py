from __future__ import annotations

from .prompts import planner_system_prompt, planner_user_prompt


class SQLPlanner:
    def __init__(self, model: str, api_key: str) -> None:
        self.model = model
        self.api_key = api_key

    def plan(self, question: str, memory_context: str = "") -> str:
        if not self.api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is missing")

        import anthropic

        client = anthropic.Anthropic(api_key=self.api_key)
        response = client.messages.create(
            model=self.model,
            max_tokens=400,
            temperature=0,
            system=planner_system_prompt(),
            messages=[
                {
                    "role": "user",
                    "content": planner_user_prompt(question=question, memory_context=memory_context),
                }
            ],
        )

        content = ""
        if response.content:
            for block in response.content:
                if getattr(block, "type", "") == "text":
                    content += block.text
        return content.strip()
