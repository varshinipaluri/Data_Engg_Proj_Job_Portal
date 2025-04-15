-- models/gold/mart/hr/kpi_location_demand.sql
{{
  config(
    materialized='view',
    schema='gold_mart', 
    tags=['hr_kpis']
  )
}}

SELECT 
  l.location,
  SUM(f.vacancies_count) AS total_vacancies,
  ROUND(AVG(f.salary_in_lpa), 2) AS avg_salary,
  LISTAGG(s.industry_sector, ', ') WITHIN GROUP (ORDER BY s.industry_sector) AS hiring_sectors
FROM {{ ref('fact_job_postings_final') }} f
JOIN {{ ref('dim_location') }} l ON f.location_id = l.dim_location_id
JOIN {{ ref('dim_industry_sector') }} s ON f.industry_sector_id = s.dim_industry_sector_id
GROUP BY l.location
ORDER BY total_vacancies DESC