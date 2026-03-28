from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd
import plotly.express as px


@dataclass
class ChartSpec:
    chart_type: str
    x: str | None = None
    y: str | None = None
    title: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def _numeric_columns(df: pd.DataFrame) -> list[str]:
    # Exclude ID columns — they're numeric but not meaningful as chart axes
    return [
        c for c in df.columns
        if pd.api.types.is_numeric_dtype(df[c]) and not c.lower().endswith("_id") and c.lower() != "id"
    ]


def _categorical_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]


def choose_chart(df: pd.DataFrame, question: str) -> ChartSpec:
    q = question.lower()
    if df.empty:
        return ChartSpec(chart_type="table", title="No rows returned")

    if df.shape[0] > 5000:
        return ChartSpec(chart_type="table", title="Result too large for chart")

    numeric = _numeric_columns(df)
    categorical = _categorical_columns(df)

    if any(word in q for word in ["correl", "relationship", "vs", "against"]) and len(numeric) >= 2:
        return ChartSpec(chart_type="scatter", x=numeric[0], y=numeric[1], title="Correlation View")

    if any(word in q for word in ["distribution", "histogram", "spread"]) and len(numeric) >= 1:
        return ChartSpec(chart_type="histogram", x=numeric[0], title="Distribution")

    if any(word in q for word in ["share", "composition", "percent", "%"]) and categorical and numeric and df.shape[0] <= 8:
        return ChartSpec(chart_type="pie", x=categorical[0], y=numeric[0], title="Composition")

    if any(word in q for word in ["trend", "over time", "sequence", "over order", "by order"]) and len(numeric) >= 1:
        if "order_number" in df.columns:
            return ChartSpec(chart_type="line", x="order_number", y=numeric[0], title="Trend")

    if categorical and numeric:
        return ChartSpec(chart_type="bar", x=categorical[0], y=numeric[0], title="Category Comparison")

    if len(numeric) == 1:
        return ChartSpec(chart_type="histogram", x=numeric[0], title="Distribution")

    return ChartSpec(chart_type="table", title="Table view")


def render_chart(df: pd.DataFrame, spec: ChartSpec):
    if spec.chart_type == "table":
        return None

    if spec.chart_type == "bar" and spec.x and spec.y and spec.x in df.columns and spec.y in df.columns:
        return px.bar(df, x=spec.x, y=spec.y, title=spec.title)

    if spec.chart_type == "line" and spec.x and spec.y and spec.x in df.columns and spec.y in df.columns:
        return px.line(df, x=spec.x, y=spec.y, markers=True, title=spec.title)

    if spec.chart_type == "pie" and spec.x and spec.y and spec.x in df.columns and spec.y in df.columns:
        return px.pie(df, names=spec.x, values=spec.y, title=spec.title)

    if spec.chart_type == "scatter" and spec.x and spec.y and spec.x in df.columns and spec.y in df.columns:
        return px.scatter(df, x=spec.x, y=spec.y, title=spec.title)

    if spec.chart_type == "histogram" and spec.x and spec.x in df.columns:
        return px.histogram(df, x=spec.x, title=spec.title)

    return None
