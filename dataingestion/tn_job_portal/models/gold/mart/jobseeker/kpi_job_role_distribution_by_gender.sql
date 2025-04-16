-- models/gold/mart/jobseeker/kpi_job_role_distribution_by_gender.sql
{{
  config(
    materialized='table',
    schema='gold_mart',
    tags=['jobseeker_kpis']
  )
}}

SELECT
  role.job_role,
  gender.gender_requirement,
  COUNT(*) AS job_count
FROM {{ ref('fact_job_postings_final') }} fact
JOIN {{ ref('dim_job_role') }} role
  ON fact.job_role_id = role.dim_job_role_id
JOIN {{ ref('dim_gender_requirement') }} gender
  ON fact.gender_requirement_id = gender.dim_gender_requirement_id
GROUP BY role.job_role, gender.gender_requirement
ORDER BY job_count DESC

