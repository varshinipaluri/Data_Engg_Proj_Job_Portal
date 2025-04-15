{{ config(
    materialized='view',
    schema='gold_mart',
    tags=['mart_jobs']
) }}

SELECT
    industry_sector,
    COUNT(*) AS job_postings,
    SUM(vacancies_count) AS total_vacancies,
    ROUND(AVG(salary_in_lpa), 2) AS avg_salary,
    ROUND(SUM(vacancies_count) * 100.0 / (SELECT SUM(vacancies_count) FROM {{ ref('core_job_postings_final') }}), 2) AS vacancy_percentage
FROM {{ ref('core_job_postings_final') }}
GROUP BY industry_sector
ORDER BY total_vacancies DESC