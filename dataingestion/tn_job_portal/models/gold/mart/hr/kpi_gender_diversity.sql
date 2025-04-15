-- models/gold/mart/hr/kpi_gender_diversity.sql
{{
  config(
    materialized='view',
    schema='gold_mart', 
    tags=['hr_kpis']
  )
}}
SELECT 
  g.gender_requirement,
  COUNT(*) AS job_count,
  SUM(f.vacancies_count) AS potential_hires,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS requirement_distribution
FROM {{ ref('fact_job_postings_final') }} f
JOIN {{ ref('dim_gender_requirement') }} g
  ON f.gender_requirement_id = g.dim_gender_requirement_id
GROUP BY g.gender_requirement