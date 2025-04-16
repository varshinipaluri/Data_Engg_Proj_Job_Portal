--models\gold\mart\hr\kpi_vacancy_distribution_by_experience.sql
{{
  config(
    materialized='table',
    schema='gold_mart',
    tags=['jobseeker_kpis']
  )
}}

SELECT
  jt.job_type,
  COUNT(*) AS job_count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{ ref('fact_job_postings_final') }}), 2) AS percentage_of_total_jobs,
  ROUND(AVG(f.salary_in_lpa), 2) AS avg_salary_in_lpa
FROM {{ ref('fact_job_postings_final') }} f
JOIN {{ ref('dim_job_type') }} jt
  ON f.job_type_id = jt.dim_job_type_id
GROUP BY jt.job_type
ORDER BY job_count DESC

