-- models/raw/raw_job_postings.sql
{{
  config(
    materialized='table',
    schema='bronze_raw',
  )
}}

SELECT * FROM {{ source("job_postings","tn_jobs_details") }}
