-- models/gold/mart/hr/kpi_gender_diversity.sql
{{
  config(
    materialized='table',
    schema='gold_kpis',
    tags=['kpis']
  )
}}

SELECT 
  gender_requirement,
  COUNT(*) AS total_job_postings,
  SUM(vacancies_count) AS total_vacancies,
  COUNT(DISTINCT company) AS companies_hiring,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS job_posting_percentage,
  ROUND(100.0 * SUM(vacancies_count) / SUM(SUM(vacancies_count)) OVER (), 2) AS vacancy_percentage
FROM {{ ref('core_job_postings_final') }}
WHERE gender_requirement IS NOT NULL
GROUP BY gender_requirement
ORDER BY total_vacancies DESC
