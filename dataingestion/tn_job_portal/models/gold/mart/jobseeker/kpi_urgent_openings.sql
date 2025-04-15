-- models/gold/mart/job_seeker/kpi_urgent_openings.sql
{{
  config(
    materialized='view',
    schema='gold_mart',
    tags=['jobseeker_kpis']
  )
}}
SELECT
  r.job_role,
  l.location,
  f.salary_in_lpa,
  f.vacancies_count,
  DATEDIFF('day', CURRENT_DATE, f.application_deadline) AS days_remaining
FROM {{ ref('fact_job_postings_final') }} f
JOIN {{ ref('dim_job_role') }} r ON f.job_role_id = r.dim_job_role_id
JOIN {{ ref('dim_location') }} l ON f.location_id = l.dim_location_id
WHERE f.application_deadline <= CURRENT_DATE + 7
  AND f.vacancies_count >= 2
ORDER BY days_remaining, salary_in_lpa DESC