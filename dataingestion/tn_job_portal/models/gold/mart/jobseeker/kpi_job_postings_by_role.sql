{{ config(
    materialized='view',
    schema='gold_mart',
    tags=['mart_jobs']
    ) 
}}

SELECT
    job_role,
    COUNT(*) AS job_count,
    ROUND(AVG(salary_in_lpa), 2) AS avg_salary_lpa
FROM {{ ref('core_job_postings_final') }}
GROUP BY job_role
ORDER BY job_count DESC


