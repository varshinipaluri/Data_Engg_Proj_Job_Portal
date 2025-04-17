-- models/gold/mart/hr/kpi_urgent_positions.sql
{{
  config(
    materialized='table',
    schema='gold_kpis',
    tags=['kpis']
  )
}}

SELECT
  job_role,
  company,
  application_deadline,
  vacancies_count,
  DATEDIFF('day', CURRENT_DATE, application_deadline) AS days_until_closure
FROM {{ ref('core_job_postings_final') }}
WHERE application_deadline BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '15 DAY'
ORDER BY application_deadline ASC
