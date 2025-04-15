{{ config(
    materialized='view',
    schema='gold_mart',
    tags=['mart_jobs']
) }}

WITH skill_data AS (
    SELECT
        job_url,
        TRIM(value) AS skill
    FROM {{ ref('core_job_postings_final') }},
    LATERAL FLATTEN(input => SPLIT(skills, ','))
)

SELECT
    skill,
    COUNT(*) AS demand_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{ ref('core_job_postings_final') }}), 2) AS percentage_of_jobs
FROM skill_data
GROUP BY skill
ORDER BY demand_count DESC