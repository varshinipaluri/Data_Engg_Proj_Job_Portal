-- models/gold/mart/hr/kpi_vacancy_to_salary_with_sector.sql
{{
  config(
    materialized='table',
    schema='gold_kpis',
    tags=['kpis']
  )
}}

SELECT
  job_type, 
  industry_sector,
  SUM(vacancies_count) AS total_vacancies,
  ROUND(AVG(salary_in_lpa), 2) AS avg_salary
FROM {{ ref('core_job_postings_final') }}
WHERE job_type IS NOT NULL AND industry_sector IS NOT NULL
GROUP BY job_type, industry_sector  
ORDER BY total_vacancies DESC
