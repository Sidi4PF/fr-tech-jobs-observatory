# FR Tech Jobs Observatory

> Analyse du marché tech français via l'API France Travail — pipeline Python · DuckDB · Power BI

![Python](https://img.shields.io/badge/Python-3.11-blue) ![DuckDB](https://img.shields.io/badge/DuckDB-latest-yellow) ![Power BI](https://img.shields.io/badge/Power%20BI-2.152-orange)

---

## What this project does

This project collects, cleans, and analyses French tech job postings (Python & SQL roles) from the France Travail public API. It produces a structured star schema in DuckDB and a two-page Power BI dashboard with market insights.

**Key findings from the latest dataset (1 115 offers · 2025–2026) :**

- SQL appears in **81%** of postings — it is the baseline requirement, not a differentiator
- Git (52%) and Python (41%) follow — Python remains the signal for senior data roles
- Remote roles pay a **+11% median premium** (40k€ vs 36k€ on-site)
- **W12 (mid-March)** was the hiring peak at 294 offers — post-Q1 budget release
- IDF concentrates 61% of offers, but Lyon (97), Nantes (54) and Marseille (51) show active regional markets

> Salary data is available for ~28% of offers only. Medians should be interpreted with caution, especially for roles with fewer than 10 salary-bearing offers.

---

## Dashboard

Two-page Power BI report — see [`docs/screenshots/dashboard.pdf`](docs/screenshots/dashboard.pdf)

**Page 1 — Overview**

- 5 KPI cards (total offers, median salary, remote %, unique companies, unique cities)
- Top skills in demand · Median salary by role · Offers map (Azure Maps) · Contract type split · Weekly hiring trend

**Page 2 — Market Insights**

- Skills in demand as % of postings
- Remote vs on-site salary comparison (+11% premium)
- Top 3 regional markets outside IDF
- Weekly hiring trend with W12 peak annotation
- Data quality caveat

---

## Stack

| Layer           | Tool                                 |
| --------------- | ------------------------------------ |
| Data collection | Python · France Travail API (OAuth2) |
| Storage (raw)   | Parquet                              |
| Transformation  | DuckDB · SQL                         |
| Exports         | CSV (UTF-8-sig)                      |
| Visualisation   | Power BI Desktop 2.152 · Azure Maps  |

---

## Project structure

```
fr-tech-jobs-observatory/
├── src/
│   ├── collect.py          # API collector — OAuth2, pagination, raw parquet
│   ├── clean.py            # Cleaning — salary parsing, skill extraction, GPS coords
│   ├── transform.py        # DuckDB star schema + CSV exports
│   └── run_all.py          # One-command pipeline runner
├── data/
│   ├── raw/                # offers_raw.parquet (gitignored)
│   ├── processed/          # offers_clean.parquet (gitignored)
│   └── exports/            # CSV files for Power BI
│       ├── fact_offers.csv
│       ├── fact_offer_skills.csv
│       ├── kpi_top_skills.csv
│       ├── kpi_salary_overview.csv
│       ├── kpi_geo.csv
│       ├── kpi_weekly_trend.csv
│       ├── kpi_contract_type.csv
│       └── kpi_top_cities.csv
├── docs/
│   └── screenshots/
│       └── dashboard.pdf
├── .gitignore
└── README.md
```

---

## Data model

```
fact_offers (1) ──────────── (many) fact_offer_skills
    id                              id
    title                           title
    company_name                    company_name
    contract_type                   city
    city                            city_geocode
    city_geocode                    department
    latitude                        skill
    longitude
    postal_code
    department
    date_creation
    year · month · week
    is_remote
    salary_min · salary_max · salary_median
    skills_count
```

Relation : one-to-many · cross-filter : Both directions

---

## Pipeline

### 1. Collect — `src/collect.py`

Connects to the France Travail API using OAuth2 (`scope: api_offresdemploiv2 o2dsoffre`). Paginates results by offsets of 150 with a 400ms sleep between requests. Saves raw offers to `data/raw/offers_raw.parquet`.

### 2. Clean — `src/clean.py`

- Extracts structured fields from nested API JSON
- Parses salary from free-text `libelle` field using regex
- Detects remote work mentions in job descriptions
- Extracts 30+ skills from description text
- Extracts GPS coordinates (`latitude`, `longitude`) directly from `lieuTravail`
- Outputs `data/processed/offers_clean.parquet`

**Salary parsing logic :**

```python
def parse_salary(salaire):
    # Detects monthly vs annual
    # Converts monthly → annual once (×12)
    # Safety net: amounts < 15k assumed monthly
    # Filters outliers: median < 15k or > 300k → None
```

A critical bug was fixed in April 2026 : alternance salaries (e.g. 810–1801€/month) were being converted twice, producing inflated values up to 259k€/year. The fix applies the monthly→annual conversion exactly once using `if/elif`.

### 3. Transform — `src/transform.py`

Loads the clean parquet into DuckDB and builds two fact tables :

- `fact_offers` — one row per offer
- `fact_offer_skills` — one row per offer × skill (via `UNNEST`)

Exports 7 pre-aggregated KPI tables as CSV for Power BI consumption.

### 4. Run all — `src/run_all.py`

```bash
python src/run_all.py
```

Runs the full pipeline in sequence : collect → clean → transform.

---

## Setup

### Prerequisites

- Python 3.11+
- France Travail API credentials ([register here](https://francetravail.io/data/api))

### Install

```bash
git clone https://github.com/Sidi4PF/fr-tech-jobs-observatory.git
cd fr-tech-jobs-observatory
pip install -r requirements.txt
```

### Configure

Create a `.env` file at the root :

```
CLIENT_ID=your_client_id
CLIENT_SECRET=your_client_secret
```

### Run

```bash
python src/run_all.py
```

Then open Power BI Desktop and refresh the data sources pointing to `data/exports/`.

---

## Technical challenges

**API pagination** — France Travail limits results to 150 per request. The collector loops over offsets with a sleep to avoid rate limiting.

**OAuth2 scope** — the correct scope is `api_offresdemploiv2 o2dsoffre`. Using only `api_offresdemploiv2` returns a 403.

**Salary parsing** — salaries are stored as free text (e.g. `"Annuel de 45000.0 Euros à 65000.0 Euros sur 12.0 mois"`). A regex parser handles annual, monthly, and edge cases. A double-conversion bug for alternance contracts was identified and fixed.

**GPS coordinates** — the API already provides `latitude` and `longitude` in `lieuTravail`. These are extracted directly, avoiding the need for a geocoding API for 88% of offers.

**Star schema in DuckDB** — `fact_offer_skills` is built via `UNNEST(skills)` directly in SQL, producing a clean one-row-per-skill table without Python loops.

---

## Author

Sidi · Data Analyst · [GitHub](https://github.com/Sidi4PF)
