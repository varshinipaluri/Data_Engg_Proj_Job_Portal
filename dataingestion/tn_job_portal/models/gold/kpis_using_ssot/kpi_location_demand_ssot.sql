-- models/gold/mart/hr/kpi_location_demand.sql
{{
  config(
    materialized='table',
    schema='gold_kpis',
    tags=['kpis']
  )
}}

SELECT 
  location,
  SUM(vacancies_count) AS total_vacancies,
  ROUND(AVG(salary_in_lpa), 2) AS avg_salary,
  LISTAGG(DISTINCT industry_sector, ', ') WITHIN GROUP (ORDER BY industry_sector) AS hiring_sectors
FROM {{ ref('core_job_postings_final') }}
WHERE location IS NOT NULL
GROUP BY location
ORDER BY total_vacancies DESC
