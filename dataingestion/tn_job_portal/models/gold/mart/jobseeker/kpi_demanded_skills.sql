{{ config(
    materialized='view',
    schema='gold_mart',
    tags=['mart_jobs']
    ) 
}}

WITH exploded_skills AS (
    SELECT
        job_title,
        skill.value AS skill
    FROM {{ ref('core_job_postings_final') }},
        LATERAL FLATTEN(input => SPLIT(skills, '|')) AS skill
)
SELECT
    skill,
    COUNT(*) AS skill_demand
FROM exploded_skills
GROUP BY skill
ORDER BY skill_demand DESC
