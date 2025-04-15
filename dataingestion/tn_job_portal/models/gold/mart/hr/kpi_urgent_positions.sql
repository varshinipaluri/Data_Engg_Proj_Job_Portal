-- models/gold/mart/hr/kpi_urgent_positions.sql
{{
  config(
    materialized='view',
    schema='gold_mart', 
    tags=['hr_kpis']
  )
}}
SELECT
  COUNT(*) AS jobs_closing_soon,
  SUM(vacancies_count) AS urgent_vacancies,
  ROUND(AVG(DATEDIFF('day', CURRENT_DATE, application_deadline)), 1) AS avg_days_remaining
FROM {{ ref('fact_job_postings_final') }}
WHERE application_deadline BETWEEN CURRENT_DATE AND CURRENT_DATE + 15