-- models/gold/mart/hr/kpi_job_postings_by_company.sql
{{
  config(
    materialized='table',
    schema='gold_kpis',
    tags=['kpis']
  )
}}
SELECT 
  company,
  COUNT(*) AS total_job_postings,
  SUM(vacancies_count) AS total_vacancies
FROM {{ ref('core_job_postings_final') }}
WHERE company IS NOT NULL
GROUP BY company
ORDER BY total_vacancies DESC
