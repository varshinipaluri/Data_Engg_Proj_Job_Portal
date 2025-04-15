{{ config(
    materialized='view',
    schema='gold_mart',
    tags=['mart_jobs']
    ) 
}}

SELECT
    job_title,
    company,
    location,
    open_until AS application_deadline
FROM {{ ref('core_job_postings_final') }}
ORDER BY application_deadline ASC;
