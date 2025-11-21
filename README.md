# UK Road Safety Data Pipeline & Dashboard (WIP)

End-to-end data pipeline for the UK Department for Transport (DfT) **Road Safety data (last 5 years)**.

The goal is to:

- Ingest **Collisions**, **Vehicles**, and **Casualties** tables from the official DfT release  
- Run **automated profiling and data quality checks** on the raw CSVs  
- Produce **clean, analysis-ready Parquet tables** for notebooks and dashboards  
- Eventually ship an interactive dashboard (e.g. Streamlit) for exploring road safety patterns

---

## Why this project exists

This repo is designed to look like something you’d use in a real job:

- **Real government data** – large CSVs, multiple related tables, coded fields  
- **Reproducible pipeline** – scripts you can run from a fresh clone to get from raw → processed  
- **Data quality first** – pre-merge inspection and contract checks before any modelling or dashboarding  
- **Employer-friendly** – clear structure, clear scripts, and room for notebooks + an app

---

## Data used

You’ll need the official DfT road safety dataset for the last 5 years, with at least:

- `Collisions.csv` – one row per collision  
- `Vehicles.csv` – one row per vehicle  
- `Casualties.csv` – one row per casualty  

Place them under:

```
data/raw/dft_road_safety_last_5_years/
    Collisions.csv
    Vehicles.csv
    Casualties.csv
```

> **Note:** The data itself is not committed to the repo (gitignored).

---

## Project structure

```
uk-road-safety-data-pipeline-dash/
├─ data/
│  ├─ raw/
│  │  └─ dft_road_safety_last_5_years/
│  │       Collisions.csv
│  │       Vehicles.csv
│  │       Casualties.csv
│  └─ processed/
│     ├─ _profile/
│     │    premerge_profile.md
│     │    premerge_profile.json
│     │    columns_profile.csv
│     ├─ collisions_clean.parquet
│     ├─ vehicles_enriched.parquet
│     └─ casualties_enriched.parquet
├─ scripts/
│  ├─ premerge_inspect.py   # raw-data profiling & integrity report
│  └─ load_merge.py         # light clean + merge → processed Parquet tables
├─ notebooks/               # (planned) EDA & modelling
├─ app/                     # (planned) dashboard
├─ artifacts/               # (planned) exported figures/reports
└─ README.md
```

---

## Setup

Create and activate a virtual environment:

```bash
python -m venv .venv

# Windows
.\.venv\Scripts\Activate.ps1

pip install --upgrade pip
pip install -r requirements.txt   # once added
```

Minimum packages for now:

```bash
pip install pandas numpy pyarrow
```

---

## Pipeline scripts

### 1. Pre-merge inspection

**Script:** `scripts/premerge_inspect.py`

Checks:

- PK uniqueness on `collision_index`  
- FK coverage (vehicles/casualties → collisions)  
- Duplicate key combinations  
- Range validation (lat/long, ages, engine capacity, year)  
- Allowed speed limits  
- Categorical distribution previews  
- Years present  

Outputs:

- `data/processed/_profile/premerge_profile.md`  
- `data/processed/_profile/premerge_profile.json`  
- `data/processed/_profile/columns_profile.csv`

Run:

```bash
python scripts/premerge_inspect.py
# Or faster:
python scripts/premerge_inspect.py --nrows 300000 --emit-samples
```

---

### 2. Load, light-clean & merge to Parquet

**Script:** `scripts/load_merge.py`

Does:

- Loads all three tables  
- Converts sentinel codes → proper `NA`  
- Normalises speed limits  
- Light geo sanity check  
- Enriches vehicles & casualties with collision context  
- Ensures clean key relationships  
- Saves Parquet tables:

```
data/processed/
    collisions_clean.parquet
    vehicles_enriched.parquet
    casualties_enriched.parquet
```

Run:

```bash
python scripts/load_merge.py
```

---

## Typical workflow

```bash
# 1. Inspect raw data
python scripts/premerge_inspect.py

# 2. Build cleaned/enriched Parquet tables
python scripts/load_merge.py

# 3. (Planned) Validation script
# python scripts/validate_processed.py

# 4. (Planned) Notebooks or dashboard
# jupyter lab
# streamlit run app/streamlit_app.py
```

---

## Roadmap

1. Contract-based validation on processed tables  
2. Human-readable dimension tables  
3. EDA + modelling notebooks  
4. Streamlit dashboard with filters, charts, hotspots, time trends  

---

## License

MIT
