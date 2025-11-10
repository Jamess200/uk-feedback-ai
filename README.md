# AI-Powered UK Feedback & Trends Platform

Analyse messy UK complaint/feedback data joined with Met Office weather and ONS socio-economic context to **predict, forecast, and explain** complaint trends. Ships with an interactive Streamlit dashboard and AI summaries.

## Why this project
- **Real-world messiness:** multiple public UK datasets with inconsistent formats and geography.
- **End-to-end:** ingestion → cleaning → feature engineering → ML/NLP/forecasting → dashboard.
- **Employer value:** actionable insights with clear communication and automation.

## Quickstart
```bash
python -m venv .venv
# macOS/Linux
source .venv/bin/activate
# Windows (PowerShell)
# .\.venv\Scripts\activate

pip install -r requirements.txt
streamlit run app/dashboard.py
```

## Project structure
```
uk-feedback-ai/
├─ data/
│  ├─ raw/          # source files (gitignored)
│  └─ processed/    # clean & feature tables (gitignored)
├─ notebooks/       # EDA, modeling, experiments
├─ scripts/         # download, cleaning, feature engineering
├─ app/             # Streamlit dashboard
├─ artifacts/       # figures, reports (gitignored)
├─ models/          # trained models (gitignored)
├─ logs/            # logs (gitignored)
└─ .streamlit/      # local app config (gitignored)
```

## Planned datasets (UK)
- **Complaints/feedback:** Financial Ombudsman Service data.
- **Weather:** Met Office (temperatures, rainfall, severe events).
- **Socio-economic:** ONS population (for per-capita rates) and Indices of Multiple Deprivation.
- **(Optional)** Inflation/price indices for financial stress context.

## Features (MVP)
- Data ingestion + schema checks.
- Cleaning + joins on region/date; per-capita complaint rates.
- ML classification (e.g., upheld vs. not upheld) and basic forecasting by region.
- NLP topic discovery on complaint text (if available).
- Streamlit dashboard with filters, charts, and AI-generated summaries.

## Roadmap
1. Ingest & EDA (complaints)  
2. Join ONS population + IMD; derive features  
3. Model: classification + forecasting  
4. Dashboard (KPIs, trends, topics, forecasts)  
5. AI summaries + documentation polish

## How to run the dashboard
```bash
streamlit run app/dashboard.py
```

## License
MIT

