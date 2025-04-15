-- models/gold/mart/jobseeker/kpi_job_offers_by_type.sql
{{
  config(
    materialized='view',
    schema='gold_mart',
    tags=['jobseeker_kpis']
  )
}}

SELECT
  job_type,
  COUNT(*) AS job_count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{ ref('fact_job_postings_final') }}), 2) AS percentage_of_total_jobs
FROM {{ ref('fact_job_postings_final') }}
GROUP BY job_type
ORDER BY job_count DESC
