from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_FILE = PROJECT_ROOT / "data" / "raw" / "offers_raw.parquet"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_FILE = PROCESSED_DIR / "offers_clean.parquet"


SKILLS = [
    "python",
    "sql",
    "power bi",
    "tableau",
    "spark",
    "dbt",
    "airflow",
    "azure",
    "aws",
    "gcp",
    "docker",
    "kubernetes",
    "terraform",
    "git",
    "pandas",
    "numpy",
    "scikit-learn",
    "tensorflow",
    "pytorch",
    "machine learning",
    "deep learning",
    "nlp",
    "llm",
    "genai",
    "databricks",
    "snowflake",
    "looker",
    "kafka",
    "postgresql",
    "mysql",
]


def safe_get_nested(data: Any, *keys: str, default=None):
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def extract_skills(description: Any) -> list[str]:
    if not isinstance(description, str):
        return []

    text = description.lower()
    found = [skill for skill in SKILLS if skill in text]
    return sorted(set(found))


def extract_salary_bounds(salaire: Any) -> tuple[float | None, float | None]:
    if not isinstance(salaire, dict):
        return None, None

    libelle = salaire.get("libelle", "")
    if not isinstance(libelle, str) or not libelle.strip():
        return None, None

    text = libelle.replace("\xa0", " ").replace("€", "").replace(",", ".")
    numbers = re.findall(r"\d[\d\s]*", text)

    parsed = []
    for n in numbers:
        n = n.replace(" ", "")
        if n.isdigit():
            parsed.append(float(n))

    if len(parsed) >= 2:
        return parsed[0], parsed[1]
    if len(parsed) == 1:
        return parsed[0], parsed[0]

    return None, None


def detect_remote(description: Any) -> bool:
    if not isinstance(description, str):
        return False

    text = description.lower()
    patterns = [
        "télétravail",
        "teletravail",
        "remote",
        "full remote",
        "hybride",
        "travail à distance",
    ]
    return any(pattern in text for pattern in patterns)


def extract_department(lieu_travail: Any) -> str:
    if not isinstance(lieu_travail, dict):
        return ""

    code_postal = str(lieu_travail.get("codePostal", "")).strip()
    commune = str(lieu_travail.get("commune", "")).strip()

    if len(code_postal) >= 2:
        return code_postal[:2]

    if len(commune) >= 2 and commune[:2].isdigit():
        return commune[:2]

    return ""


def normalize_contract_type(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().upper()


def main() -> None:
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Raw file not found: {RAW_FILE}")

    df = pd.read_parquet(RAW_FILE)

    if df.empty:
        raise ValueError("Raw dataset is empty.")

    print(f"[load] Raw rows: {len(df)}")
    print(f"[load] Raw columns: {len(df.columns)}")

    df_clean = pd.DataFrame()

    df_clean["id"] = df.get("id")
    df_clean["title"] = df.get("intitule")
    df_clean["description"] = df.get("description")
    df_clean["date_creation"] = pd.to_datetime(df.get("dateCreation"), errors="coerce")

    df_clean["company_name"] = df.get("entreprise").apply(
        lambda x: safe_get_nested(x, "nom", default="") if isinstance(x, dict) else ""
    )

    df_clean["company_description"] = df.get("entreprise").apply(
        lambda x: safe_get_nested(x, "description", default="") if isinstance(x, dict) else ""
    )

    df_clean["contract_type"] = df.get("typeContrat").apply(normalize_contract_type)
    df_clean["experience_required"] = df.get("experienceExige")

    df_clean["city"] = df.get("lieuTravail").apply(
        lambda x: safe_get_nested(x, "libelle", default="") if isinstance(x, dict) else ""
    )

    df_clean["postal_code"] = df.get("lieuTravail").apply(
        lambda x: safe_get_nested(x, "codePostal", default="") if isinstance(x, dict) else ""
    )

    df_clean["department"] = df.get("lieuTravail").apply(extract_department)

    df_clean["salary_label"] = df.get("salaire").apply(
        lambda x: safe_get_nested(x, "libelle", default="") if isinstance(x, dict) else ""
    )

    df_clean["salary_min"], df_clean["salary_max"] = zip(
        *df.get("salaire").apply(extract_salary_bounds)
    )

    df_clean["salary_median"] = df_clean[["salary_min", "salary_max"]].mean(
        axis=1, skipna=True
    )

    df_clean["skills"] = df_clean["description"].apply(extract_skills)
    df_clean["skills_count"] = df_clean["skills"].apply(len)

    df_clean["is_remote"] = df_clean["description"].apply(detect_remote)

    iso = df_clean["date_creation"].dt.isocalendar()
    df_clean["year"] = iso.year.astype("Int64")
    df_clean["week"] = iso.week.astype("Int64")
    df_clean["month"] = df_clean["date_creation"].dt.month.astype("Int64")

    df_clean = df_clean.drop_duplicates(subset="id").reset_index(drop=True)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df_clean.to_parquet(PROCESSED_FILE, index=False)

    print(f"[saved] Clean offers saved to: {PROCESSED_FILE}")
    print(f"[summary] Clean rows: {len(df_clean)}")
    print(f"[summary] Offers with salary: {int(df_clean['salary_median'].notna().sum())}")
    print(f"[summary] Offers with >=1 skill: {int((df_clean['skills_count'] > 0).sum())}")
    print(f"[summary] Remote offers: {int(df_clean['is_remote'].sum())}")


if __name__ == "__main__":
    main()