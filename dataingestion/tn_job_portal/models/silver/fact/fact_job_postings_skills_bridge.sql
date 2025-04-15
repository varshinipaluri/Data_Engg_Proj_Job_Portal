-- fact_job_postings_skills_bridge.sql
{{ config(
    materialized='table',
    schema='facts',
    tags=['bridge']
) }}

WITH bridge_data AS (
    SELECT
        job_posting_id,
        unnest(string_to_array({{ ref('fact_job_postings') }}.skills, '|')) AS skills_id
    FROM {{ ref('fact_job_postings') }}
)

SELECT
    job_posting_id,
    skills_id
FROM bridge_data;
