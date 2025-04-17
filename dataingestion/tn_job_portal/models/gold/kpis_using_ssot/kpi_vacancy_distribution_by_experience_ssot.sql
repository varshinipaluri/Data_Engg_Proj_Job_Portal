-- models/gold/mart/hr/kpi_vacancy_distribution_by_experience_with_sector_and_company.sql
{{
  config(
    materialized='table',
    schema='gold_kpis',
    tags=['kpis']
  )
}}

SELECT
  experience_level,
  company,
  industry_sector,
  SUM(vacancies_count) AS total_vacancies
FROM {{ ref('core_job_postings_final') }}
WHERE experience_level IS NOT NULL
GROUP BY experience_level, company, industry_sector
ORDER BY total_vacancies DESC
