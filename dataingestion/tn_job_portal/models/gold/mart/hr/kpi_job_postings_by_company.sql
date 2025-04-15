--models\gold\mart\hr\kpi_job_postings_by_company.sql
{{ config(
    materialized='view',
    schema='gold_mart',
    tags=['mart_jobs']
) }}

SELECT
    company,
    COUNT(*) AS job_postings,
    SUM(vacancies_count) AS total_vacancies,
    ROUND(AVG(salary_in_lpa), 2) AS avg_salary
FROM {{ ref('core_job_postings_final') }}
GROUP BY company
ORDER BY job_postings DESC


