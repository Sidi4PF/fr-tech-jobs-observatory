from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from duckdb import df
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_FILE = PROJECT_ROOT / "data" / "raw" / "offers_raw.parquet"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_FILE = PROCESSED_DIR / "offers_clean.parquet"



SKILLS = [
    "python", "sql", "power bi", "tableau", "spark", "dbt", "airflow",
    "azure", "aws", "gcp", "docker", "kubernetes", "terraform", "git",
    "pandas", "numpy", "scikit-learn", "tensorflow", "pytorch",
    "machine learning", "deep learning", "nlp", "llm", "genai",
    "databricks", "snowflake", "looker", "kafka", "postgresql", "mysql",
]



def safe_get_nested(data: Any, *keys: str, default=None):
    """Safely extract nested dict values."""
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def extract_skills(description: Any) -> list[str]:
    """Extract known skills from job description."""
    if not isinstance(description, str):
        return []
    text = description.lower()
    found = [skill for skill in SKILLS if skill in text]
    return sorted(set(found))


def detect_remote(description: Any) -> bool:
    """Detect remote work mentions."""
    if not isinstance(description, str):
        return False
    text = description.lower()
    patterns = [
        "télétravail", "teletravail", "remote", "full remote",
        "hybride", "travail à distance"
    ]
    return any(p in text for p in patterns)


def clean_city(value):
    parts = value.split(" - ", 1)
    if len(parts) == 2:
        dept, city = parts
    else:
        return None, None, None

    city = re.sub(r"CEDEX\s*\d*", "", city, flags=re.IGNORECASE)
    city = re.sub(r"\b\d+e?\s+Arrondissement\b", "", city, flags=re.IGNORECASE)
    city = city.strip().title()

    # Sans parenthèses, plus standard pour un fallback éventuel
    city_geocode = f"{city}, {dept}, France"

    return dept, city, city_geocode



def extract_department(lieu_travail: Any) -> str:
    """Extract department from postal code or commune."""
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
    """Normalize contract type."""
    if not isinstance(value, str):
        return ""
    return value.strip().upper()



def parse_salary(salaire: Any) -> tuple[float | None, float | None, float | None]:
    """Parse salary from France Travail 'libelle' field."""
    if not isinstance(salaire, dict):
        return None, None, None
    libelle = salaire.get("libelle")
    if not isinstance(libelle, str) or not libelle.strip():
        return None, None, None
    
    text = libelle.replace("\xa0", " ").replace("€", "").replace(",", ".").lower()
    
    # Detect monthly salary
    is_monthly = "mensuel" in text or "mois" in text
    
    # Extract numbers
    numbers = re.findall(r"\d+(?:\.\d+)?", text)
    amounts = [float(n) for n in numbers if float(n) > 100]
    
    if not amounts:
        return None, None, None
    
    sal_min = amounts[0]
    sal_max = amounts[1] if len(amounts) >= 2 else amounts[0]
    
    # Convert monthly → annual (UNE SEULE FOIS)
    if is_monthly:
        sal_min *= 12
        sal_max *= 12
    # Si pas de mention "mensuel" mais montants < 15k, assume mensuel
    elif sal_min < 15000:
        sal_min *= 12
        sal_max *= 12
    
    sal_median = (sal_min + sal_max) / 2
    
    # Filter outliers
    if sal_median < 15000 or sal_median > 300000:
        return None, None, None
    
    return sal_min, sal_max, sal_median



def main() -> None:
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Raw file not found: {RAW_FILE}")

    df = pd.read_parquet(RAW_FILE)

    if df.empty:
        raise ValueError("Raw dataset is empty.")

    print(f"[load] Raw rows: {len(df)}")
    print(f"[load] Raw columns: {len(df.columns)}")

    df_clean = pd.DataFrame()

    # ============================
    # BASIC FIELDS
    # ============================
    df_clean["id"] = df.get("id")
    df_clean["title"] = df.get("intitule")
    df_clean["description"] = df.get("description")
    df_clean["date_creation"] = pd.to_datetime(df.get("dateCreation"), errors="coerce")

    # ============================
    # COMPANY INFO
    # ============================
    df_clean["company_name"] = df.get("entreprise").apply(
        lambda x: safe_get_nested(x, "nom", default="") if isinstance(x, dict) else ""
    )
    df_clean["company_description"] = df.get("entreprise").apply(
        lambda x: safe_get_nested(x, "description", default="") if isinstance(x, dict) else ""
    )

    # ============================
    # CONTRACT & EXPERIENCE
    # ============================
    df_clean["contract_type"] = df.get("typeContrat").apply(normalize_contract_type)
    df_clean["experience_required"] = df.get("experienceExige")

    # ============================
    # LOCATION (RAW)
    # ============================
    df_clean["city_raw"] = df.get("lieuTravail").apply(
        lambda x: safe_get_nested(x, "libelle", default="") if isinstance(x, dict) else ""
    )
    df_clean["postal_code"] = df.get("lieuTravail").apply(
        lambda x: safe_get_nested(x, "codePostal", default="") if isinstance(x, dict) else ""
    )
    df_clean["department_api"] = df.get("lieuTravail").apply(extract_department)
    df_clean["latitude"] = df.get("lieuTravail").apply(
    lambda x: safe_get_nested(x, "latitude", default=None) if isinstance(x, dict) else None)
    df_clean["longitude"] = df.get("lieuTravail").apply(
        lambda x: safe_get_nested(x, "longitude", default=None) if isinstance(x, dict) else None)

    # ============================
    # CLEAN CITY USING clean_city()
    # ============================
    city_df = df_clean["city_raw"].apply(
        lambda v: pd.Series(clean_city(v), index=["department_city", "city", "city_geocode"])
    )

    df_clean = pd.concat([df_clean, city_df], axis=1)

    # Final department: priorité au parsing de clean_city
    df_clean["department"] = df_clean["department_city"].fillna(df_clean["department_api"])

    # ============================
    # SALARY
    # ============================
    df_clean["salary_label"] = df.get("salaire").apply(
        lambda x: safe_get_nested(x, "libelle", default="") if isinstance(x, dict) else ""
    )

    df_clean["salary_min"], df_clean["salary_max"], df_clean["salary_median"] = zip(
        *df.get("salaire").apply(parse_salary)
    )

    # ============================
    # SKILLS
    # ============================
    df_clean["skills"] = df_clean["description"].apply(extract_skills)
    df_clean["skills_count"] = df_clean["skills"].apply(len)

    # ============================
    # REMOTE DETECTION
    # ============================
    df_clean["is_remote"] = df_clean["description"].apply(detect_remote)

    # ============================
    # TIME FEATURES
    # ============================
    iso = df_clean["date_creation"].dt.isocalendar()
    df_clean["year"] = iso.year.astype("Int64")
    df_clean["week"] = iso.week.astype("Int64")
    df_clean["month"] = df_clean["date_creation"].dt.month.astype("Int64")

    # ============================
    # DEDUP
    # ============================
    df_clean = df_clean.drop_duplicates(subset="id").reset_index(drop=True)

    # ============================
    # SAVE
    # ============================
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df_clean.to_parquet(PROCESSED_FILE, index=False)

    print(f"[saved] Clean offers saved to: {PROCESSED_FILE}")
    print(f"[summary] Clean rows: {len(df_clean)}")
    print(f"[summary] Offers with salary: {int(df_clean['salary_median'].notna().sum())}")
    print(f"[summary] Offers with >=1 skill: {int((df_clean['skills_count'] > 0).sum())}")
    print(f"[summary] Remote offers: {int(df_clean['is_remote'].sum())}")


if __name__ == "__main__":
    main()

