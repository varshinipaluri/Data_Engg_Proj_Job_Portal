{{ config(
    materialized='view',
    schema='gold_mart',
    tags=['mart_jobs']
) }}

SELECT
    location,
    job_role,
    COUNT(*) AS job_count,
    ROUND(AVG(salary_in_lpa), 2) AS avg_salary,
    STRING_AGG(DISTINCT company, ', ') AS hiring_companies
FROM {{ ref('core_job_postings_final') }}
GROUP BY location, job_role
ORDER BY job_count DESC