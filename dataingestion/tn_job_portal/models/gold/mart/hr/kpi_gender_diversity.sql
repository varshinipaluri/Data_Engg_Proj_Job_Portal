--models\gold\mart\hr\kpi_gender_diversity.sql
{{
  config(
    materialized='table',
    schema='gold_mart', 
    tags=['hr_kpis']
  )
}}
WITH total_jobs AS (
  SELECT COUNT(*) AS total_job_count
  FROM {{ ref('fact_job_postings_final') }}
)
SELECT 
  g.gender_requirement,
  COUNT(*) AS job_count,
  SUM(f.vacancies_count) AS total_vacancies
FROM {{ ref('fact_job_postings_final') }} f
JOIN {{ ref('dim_gender_requirement') }} g
  ON f.gender_requirement_id = g.dim_gender_requirement_id
GROUP BY g.gender_requirement
