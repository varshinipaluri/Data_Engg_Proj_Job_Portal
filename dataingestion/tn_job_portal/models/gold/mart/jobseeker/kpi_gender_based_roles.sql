{{ config(
    materialized='view',
    schema='gold_mart',
    tags=['mart_jobs']
    ) 
}}
 
SELECT
    job_role,
    gender_requirement,
    COUNT(*) AS job_count
FROM {{ ref('core_job_postings_final') }}
GROUP BY job_role, gender_requirement
ORDER BY job_count DESC

