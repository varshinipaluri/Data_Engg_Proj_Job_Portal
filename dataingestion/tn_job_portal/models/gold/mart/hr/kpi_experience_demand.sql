-- models/gold/mart/hr/kpi_experience_demand.sql
{{
  config(
    materialized='view',
    schema='gold_mart', 
    tags=['hr_kpis']
  )
}}
SELECT 
  s.industry_sector,
  ROUND(AVG(f.experience_years), 1) AS avg_experience_required,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.experience_years) AS median_experience,
  COUNT(*) AS job_count
FROM {{ ref('fact_job_postings_final') }} f
JOIN {{ ref('dim_industry_sector') }} s
  ON f.industry_sector_id = s.dim_industry_sector_id
GROUP BY s.industry_sector