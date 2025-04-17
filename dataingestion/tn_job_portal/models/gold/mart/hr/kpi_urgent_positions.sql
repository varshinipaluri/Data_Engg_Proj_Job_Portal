--models\gold\mart\hr\kpi_urgent_positions.sql
{{
  config(
    materialized='table',
    schema='gold_mart', 
    tags=['hr_kpis']
  )
}}

SELECT
  f.job_role_id, 
  f.company_id,  
  f.application_deadline, 
  f.vacancies_count,  
  DATEDIFF('day', CURRENT_DATE, f.application_deadline) AS days_until_closure  
FROM {{ ref('fact_job_postings_final') }} AS f
WHERE f.application_deadline BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '15 DAY'  
ORDER BY f.application_deadline ASC



