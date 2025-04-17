-- models/gold/mart/jobseeker/kpi_job_vacancy_trends.sql
{{
  config(
    materialized='table',
    schema='gold_kpis',
    tags=['kpis']
  )
}}

SELECT
  DATE_TRUNC('month', TRY_TO_DATE(application_deadline)) AS month,  
  COUNT(*) AS vacancies  
FROM {{ ref('core_job_postings_final') }}  
WHERE TRY_TO_DATE(application_deadline) IS NOT NULL  
GROUP BY DATE_TRUNC('month', TRY_TO_DATE(application_deadline))
ORDER BY month  
