{{ config(
    materialized='view',
    schema='gold_mart',
    tags=['mart_jobs']
) }}

SELECT
    location,
    industry_sector,
    COUNT(*) AS job_count,
    SUM(vacancies_count) AS total_vacancies,
    ROUND(AVG(salary_in_lpa), 2) AS avg_salary,
    ARRAY_AGG(DISTINCT job_role) AS common_roles
FROM {{ ref('core_job_postings_final') }}
GROUP BY location, industry_sector
ORDER BY total_vacancies DESC