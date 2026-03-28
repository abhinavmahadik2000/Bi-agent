# Instacart BI Agent

A conversational BI agent for querying Instacart data. Ask questions in plain English — the agent plans, generates SQL, validates, executes, and visualizes results.

Powered by LangGraph · Anthropic Claude · DuckDB · Streamlit

## Requirements

- Python 3.10+
- An [Anthropic API key](https://console.anthropic.com)
- Instacart CSV dataset files (see below)

## Setup

**1. Install dependencies**

```bash
pip install -r requirements.txt
```

**2. Configure environment**

```bash
cp .env.example .env
```

Edit `.env` and set your `ANTHROPIC_API_KEY`.

**3. Add dataset files**

Place the following CSV files in `./dataset/`:

```
dataset/
├── orders.csv
├── order_products__prior.csv
├── order_products__train.csv
├── products.csv
├── aisles.csv
└── departments.csv
```

These are available from the [Instacart Market Basket Analysis](https://www.kaggle.com/c/instacart-market-basket-analysis/data) dataset on Kaggle.

## Run

```bash
streamlit run app.py
```

The app opens at `http://localhost:8501`.

On first run, CSVs are ingested into `instacart.duckdb`. Subsequent runs skip ingestion unless you check **Force re-ingest CSVs** in the sidebar.
