# FR Tech Jobs Observatory — Analyse du Marché Tech Français

[![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10-yellow?style=flat-square)](https://duckdb.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-orange?style=flat-square&logo=powerbi)](https://powerbi.microsoft.com/)
[![API](https://img.shields.io/badge/France%20Travail-API-green?style=flat-square)](https://francetravail.io/)
[![License](https://img.shields.io/badge/License-MIT-lightgrey?style=flat-square)](LICENSE)

## Vue d'ensemble

Pipeline data analyst end-to-end analysant **1 075+ offres d'emploi tech réelles** issues de l'API France Travail (ex-Pôle Emploi).

**Stack technique** : Python | DuckDB | SQL | Power BI

---

## Objectifs du Projet

- Collecter des données réelles via une API REST avec authentification OAuth2
- Nettoyer et structurer des données brutes (salaires en texte libre, champs imbriqués)
- Construire un schéma en étoile et calculer des KPIs business actionnables
- Visualiser les insights dans un dashboard Power BI interactif

---

## Architecture

```
┌──────────────────┐
│  France Travail  │
│       API        │  OAuth2 + pagination
└────────┬─────────┘
         │ collect.py
         ▼
┌──────────────────┐
│   RAW PARQUET    │  offers_raw.parquet
│   ~1 075 offres  │
└────────┬─────────┘
         │ clean.py
         ▼
┌──────────────────┐
│ CLEAN PARQUET    │  Salaires parsés, skills extraits
│                  │  is_remote détecté
└────────┬─────────┘
         │ transform.py (DuckDB)
         ▼
┌──────────────────┐
│   STAR SCHEMA    │  fact_offers
│                  │  fact_offer_skills
│   + CSV EXPORTS  │  → Power BI
└──────────────────┘
```

---

## Résultats Clés

> ⚠️ Les insights chiffrés seront ajoutés à la fin de la construction du dashboard Power BI.

<!--
### Métriques Business

- **X** offres analysées
- **X%** des offres mentionnent SQL
- **X%** des offres en remote ou hybride
- **Salaire médian** : ~Xk€/an

### Insights

- 🏆 Skill la plus demandée : ...
- 📊 Data Engineer vs Data Analyst : écart salarial de Xk€
- 🌍 Île-de-France concentre X% des offres
-->

---

## Installation & Usage

### Prérequis

```
Python 3.11+
Compte développeur France Travail (gratuit)
Power BI Desktop
```

### Setup

**1. Cloner le repo**

```bash
git clone https://github.com/Sidi4PF/fr-tech-jobs-observatory.git
cd fr-tech-jobs-observatory
```

**2. Environnement Python**

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

**3. Credentials API**

Créer un fichier `.env` à la racine (jamais commité) :

```
FT_CLIENT_ID=your_client_id
FT_CLIENT_SECRET=your_client_secret
```

Obtenir les credentials sur [francetravail.io](https://francetravail.io) → créer une application → activer le scope `api_offresdemploiv2`.

**4. Lancer le pipeline**

```bash
# Collecte des offres via l'API (pagination automatique)
python src/collect.py

# Nettoyage, parsing des salaires, extraction des skills
python src/clean.py

# Schéma en étoile DuckDB + export CSV pour Power BI
python src/transform.py
```

Les fichiers CSV sont générés dans `data/exports/` — à charger directement dans Power BI.

---

## Structure du Projet

```
fr-tech-jobs-observatory/
├── src/
│   ├── collect.py              # Collecteur API (OAuth2, pagination)
│   ├── clean.py                # Nettoyage, parsing salaires, extraction skills
│   └── transform.py            # DuckDB star schema + exports KPI
├── sql/
│   └── kpi_queries.sql         # Toutes les requêtes KPI
├── data/
│   ├── raw/                    # offers_raw.parquet (non versionné)
│   ├── processed/              # offers_clean.parquet (non versionné)
│   └── exports/                # CSVs → Power BI (versionnés)
│       ├── fact_offers.csv
│       ├── fact_offer_skills.csv
│       ├── kpi_top_skills.csv
│       ├── kpi_salary_overview.csv
│       ├── kpi_geo.csv
│       ├── kpi_weekly_trend.csv
│       └── kpi_contract_type.csv
├── assets/screenshots/         # Screenshots dashboard (à venir)
├── theme_observatory.json      # Thème Power BI dark
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Défis Techniques

### 1. Pagination API — 150 résultats max par requête

L'API France Travail plafonne les réponses à 150 résultats par appel. Résolu en implémentant un collecteur paginé avec des offsets `range` itératifs, avec un sleep de 400ms entre chaque requête pour respecter le rate limit.

### 2. Erreurs d'authentification OAuth2 (403)

Les premières requêtes retournaient `403 Forbidden`. Cause racine : mauvais scope OAuth. Corrigé en passant explicitement `api_offresdemploiv2 o2dsoffre` dans la requête de token. Documenté dans `src/collect.py`.

### 3. Stratégie de mots-clés — pivot décisif

Démarrage avec des keywords stricts sur les métiers Data (`data analyst`, `data scientist`...) → seulement ~300 offres, volume insuffisant. Pivot vers une requête large `Python` + `SQL` → 1 075 offres uniques après déduplication sur l'`id`. Ce choix reflète mieux le marché réel des profils techniques.

### 4. Salaires stockés en texte libre

L'API retourne les salaires comme chaînes de caractères libres (`"35 000 à 45 000 EUR"`). Résolu avec du parsing regex dans `clean.py` pour extraire `salary_min`, `salary_max` et calculer `salary_median`. ~38% des offres avaient des données salariales exploitables.

### 5. Schéma en étoile avec DuckDB

Choix de DuckDB plutôt qu'une base de données complète pour la simplicité et la rapidité en local. Utilisation de `UNNEST()` pour exploser les listes de skills en table `fact_offer_skills` séparée, permettant une analyse many-to-many sans duplication.

### 6. Power BI — types de données et relations ambiguës

Lors du chargement des CSVs : salaires importés en texte au lieu de décimal, et colonnes `id` avec types incohérents entre les deux tables causaient des relations silencieusement cassées. Corrigé en forçant les types dans Power Query avant de créer les relations.

---

## Modèle de Données

```
fact_offers (1) ──────────────────────── (many) fact_offer_skills
    id (PK)                                          id (FK)
    title                                            skill
    company_name
    contract_type
    city / department
    date_creation / year / month / week
    is_remote
    salary_min / salary_max / salary_median
    skills_count
```

Relation : `fact_offers[id]` → `fact_offer_skills[id]` · One-to-many · Cross-filter : Both

---

## Dashboard Power BI

> ⚠️ Dashboard en cours de finalisation — screenshots à venir.

**Page 1 — Overview**

- 5 KPI cards : total offres, salaire médian, % remote, entreprises uniques, villes uniques
- Bar chart horizontal : Top 10 skills les plus demandées
- Bar chart horizontal : Salaire médian par métier
- Filled Map : densité d'offres par département
- Donut chart : répartition CDI / CDD
- Column chart : évolution hebdomadaire des offres

**Page 2 — Skills & Salaires**

- Matrix heatmap : skills × type de contrat
- Scatter plot : fréquence skill vs salaire médian associé
- Bar chart : % remote par métier

---

## Stack Technique

| Technologie        | Usage                                           |
| ------------------ | ----------------------------------------------- |
| **Python**         | Collecte API, nettoyage, orchestration pipeline |
| **DuckDB**         | Transformations SQL locales, star schema        |
| **pandas / NumPy** | Manipulation et nettoyage des données           |
| **Power BI**       | Dashboard interactif, DAX measures              |
| **GitHub Actions** | Refresh automatique hebdomadaire                |

---

## Décisions de Design

- **Scope large (Python & SQL)** plutôt que strict Data roles → dataset plus réaliste et représentatif du marché tech
- **DuckDB** au lieu d'une vraie BDD → zéro infrastructure, SQL complet, performant sur fichiers locaux
- **Star schema** `fact_offers` + `fact_offer_skills` → analyse des skills sans duplication, compatible Power BI
- **Parquet** pour le stockage intermédiaire → plus léger que CSV, typage natif

---

## Améliorations Futures

- [ ] Enrichissement avec données INSEE (revenus médians par département)
- [ ] Ajout d'autres sources (Welcome to the Jungle, LinkedIn)
- [ ] NLP plus avancé pour l'extraction de skills (spaCy)
- [ ] Dashboard Streamlit pour une version web publique
- [ ] Incremental loading — ne collecter que les nouvelles offres
- [ ] CI/CD avec GitHub Actions pour refresh automatique

---

## Contact

**Sidi Amadou BOCOUM**

- 💼 LinkedIn : https://www.linkedin.com/in/sidi-amadou-bocoum-046b691b6/
- 📧 Email : sidi.bocoum02@gmail.com
- 💻 GitHub : https://github.com/Sidi4PF

---

## Licence

Ce projet est sous licence MIT.

Les données proviennent de l'[API France Travail](https://francetravail.io) — usage soumis aux [CGU France Travail](https://francetravail.io/data/api).
