-- Top 20 skills
SELECT skill, COUNT(*) AS nb_offres, ROUND(
        COUNT(*) * 100.0 / (
            SELECT COUNT(DISTINCT id)
            FROM fact_offers
        ), 1
    ) AS pct_offres
FROM fact_offer_skills
GROUP BY
    skill
ORDER BY nb_offres DESC, skill
LIMIT 20;

-- Salary overview
SELECT
    COUNT(*) AS total_offers,
    COUNT(salary_median) AS offers_with_salary,
    ROUND(MEDIAN (salary_median), 0) AS median_salary,
    ROUND(AVG(salary_median), 0) AS avg_salary,
    ROUND(
        SUM(
            CASE
                WHEN is_remote THEN 1
                ELSE 0
            END
        ) * 100.0 / COUNT(*),
        1
    ) AS pct_remote
FROM fact_offers;

-- Offers by department
SELECT
    department,
    COUNT(*) AS nb_offres,
    ROUND(MEDIAN (salary_median), 0) AS median_salary,
    ROUND(
        SUM(
            CASE
                WHEN is_remote THEN 1
                ELSE 0
            END
        ) * 100.0 / COUNT(*),
        1
    ) AS pct_remote
FROM fact_offers
WHERE
    department IS NOT NULL
    AND department <> ''
GROUP BY
    department
ORDER BY nb_offres DESC, department;

-- Weekly trend
SELECT
    year,
    week,
    COUNT(*) AS nb_offres,
    ROUND(MEDIAN (salary_median), 0) AS median_salary
FROM fact_offers
GROUP BY
    year,
    week
ORDER BY year, week;

-- Contract types
SELECT
    contract_type,
    COUNT(*) AS nb_offres,
    ROUND(
        COUNT(*) * 100.0 / (
            SELECT COUNT(*)
            FROM fact_offers
        ),
        1
    ) AS pct_offres
FROM fact_offers
WHERE
    contract_type IS NOT NULL
    AND contract_type <> ''
GROUP BY
    contract_type
ORDER BY nb_offres DESC;

-- Top cities
SELECT
    city,
    COUNT(*) AS nb_offres,
    ROUND(MEDIAN (salary_median), 0) AS median_salary
FROM fact_offers
WHERE
    city IS NOT NULL
    AND city <> ''
GROUP BY
    city
ORDER BY nb_offres DESC, city
LIMIT 20;