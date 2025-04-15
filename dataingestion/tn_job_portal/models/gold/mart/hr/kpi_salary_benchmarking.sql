--\models\gold\mart\hr\kpi_salary_benchmarking.sql
{{ config(
    materialized='view',
    schema='gold_mart',
    tags=['mart_jobs']
) }}

SELECT
    industry_sector,
    job_role,
    experience_level,
    ROUND(AVG(salary_in_lpa), 2) AS avg_salary,
    MIN(salary_in_lpa) AS min_salary,
    MAX(salary_in_lpa) AS max_salary,
    COUNT(*) AS job_count
FROM {{ ref('core_job_postings_final') }}
GROUP BY industry_sector, job_role, experience_level
ORDER BY avg_salary DESC