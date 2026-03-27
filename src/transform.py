from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_FILE = PROJECT_ROOT / "data" / "processed" / "offers_clean.parquet"
DB_FILE = PROJECT_ROOT / "data" / "jobs.duckdb"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports"


QUERIES = {
    "fact_offers": """
        SELECT
            id,
            title,
            company_name,
            contract_type,
            city,
            postal_code,
            department,
            date_creation,
            year,
            month,
            week,
            is_remote,
            salary_min,
            salary_max,
            salary_median,
            skills_count
        FROM fact_offers
        ORDER BY date_creation DESC NULLS LAST
    """,
    "fact_offer_skills": """
        SELECT
            id,
            title,
            company_name,
            city,
            department,
            skill
        FROM fact_offer_skills
        ORDER BY skill, id
    """,
    "kpi_top_skills": """
        SELECT
            skill,
            COUNT(*) AS nb_offres,
            ROUND(
                COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT id) FROM fact_offers),
                1
            ) AS pct_offres
        FROM fact_offer_skills
        GROUP BY skill
        ORDER BY nb_offres DESC, skill
        LIMIT 20
    """,
    "kpi_salary_overview": """
        SELECT
            COUNT(*) AS total_offers,
            COUNT(salary_median) AS offers_with_salary,
            ROUND(MEDIAN(salary_median), 0) AS median_salary,
            ROUND(AVG(salary_median), 0) AS avg_salary,
            ROUND(
                SUM(CASE WHEN is_remote THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
                1
            ) AS pct_remote
        FROM fact_offers
    """,
    "kpi_geo": """
        SELECT
            department,
            COUNT(*) AS nb_offres,
            ROUND(MEDIAN(salary_median), 0) AS median_salary,
            ROUND(
                SUM(CASE WHEN is_remote THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
                1
            ) AS pct_remote
        FROM fact_offers
        WHERE department IS NOT NULL
          AND department <> ''
        GROUP BY department
        ORDER BY nb_offres DESC, department
    """,
    "kpi_weekly_trend": """
        SELECT
            year,
            week,
            COUNT(*) AS nb_offres,
            ROUND(MEDIAN(salary_median), 0) AS median_salary
        FROM fact_offers
        GROUP BY year, week
        ORDER BY year, week
    """,
    "kpi_contract_type": """
        SELECT
            contract_type,
            COUNT(*) AS nb_offres,
            ROUND(
                COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_offers),
                1
            ) AS pct_offres
        FROM fact_offers
        WHERE contract_type IS NOT NULL
          AND contract_type <> ''
        GROUP BY contract_type
        ORDER BY nb_offres DESC
    """,
    "kpi_top_cities": """
        SELECT
            city,
            COUNT(*) AS nb_offres,
            ROUND(MEDIAN(salary_median), 0) AS median_salary
        FROM fact_offers
        WHERE city IS NOT NULL
          AND city <> ''
        GROUP BY city
        ORDER BY nb_offres DESC, city
        LIMIT 20
    """,
}


def build_database(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE offers_clean AS
        SELECT *
        FROM read_parquet('{PROCESSED_FILE.as_posix()}')
    """)

    con.execute("""
        CREATE OR REPLACE TABLE fact_offers AS
        SELECT
            id,
            title,
            company_name,
            contract_type,
            city,
            postal_code,
            department,
            date_creation,
            year,
            month,
            week,
            is_remote,
            salary_min,
            salary_max,
            salary_median,
            skills,
            skills_count
        FROM offers_clean
    """)

    con.execute("""
        CREATE OR REPLACE TABLE fact_offer_skills AS
        SELECT
            id,
            title,
            company_name,
            city,
            department,
            unnest(skills) AS skill
        FROM offers_clean
        WHERE skills IS NOT NULL
          AND array_length(skills) > 0
    """)


def export_queries(con: duckdb.DuckDBPyConnection) -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    for name, query in QUERIES.items():
        df = con.execute(query).df()
        output_file = EXPORT_DIR / f"{name}.csv"
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"[saved] {output_file}")


def main() -> None:
    if not PROCESSED_FILE.exists():
        raise FileNotFoundError(f"Processed file not found: {PROCESSED_FILE}")

    con = duckdb.connect(DB_FILE.as_posix())

    build_database(con)
    export_queries(con)

    fact_offers_count = con.execute("SELECT COUNT(*) FROM fact_offers").fetchone()[0]
    fact_skills_count = con.execute("SELECT COUNT(*) FROM fact_offer_skills").fetchone()[0]

    print(f"[saved] DuckDB database: {DB_FILE}")
    print(f"[summary] fact_offers rows: {fact_offers_count}")
    print(f"[summary] fact_offer_skills rows: {fact_skills_count}")

    con.close()


if __name__ == "__main__":
    main()