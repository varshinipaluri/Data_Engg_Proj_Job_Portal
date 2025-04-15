-- models/gold/mart/jobseeker/kpi_job_vacancy_trends.sql
{{
  config(
    materialized='view',
    schema='gold_mart',
    tags=['jobseeker_kpis']
  )
}}

SELECT
  DATE_TRUNC('month', TO_DATE(application_deadline)) AS month,
  COUNT(*) AS job_count
FROM {{ ref('fact_job_postings_final') }}
WHERE TRY_TO_DATE(application_deadline) IS NOT NULL  -- Filter invalid dates
GROUP BY month
ORDER BY month
