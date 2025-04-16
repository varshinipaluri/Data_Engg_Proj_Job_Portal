--models\gold\mart\hr\kpi_job_postings_by_company.sql
{{
  config(
    materialized='table',
    schema='gold_mart', 
    tags=['hr_kpis']
  )
}}
SELECT 
    comp.company,
    COUNT(*) AS total_job_postings,
    SUM(fact.vacancies_count) AS total_vacancies
FROM {{ ref('fact_job_postings_final') }} AS fact
JOIN {{ ref('dim_company') }} AS comp 
    ON fact.company_id = comp.dim_company_id
GROUP BY comp.company

