-- dataingestion\tn_job_portal\models\gold\mart\hr\kpi_vacancy_to_salary_ratio.sql
{{
  config(
    materialized='table',
    schema='gold_mart', 
    tags=['hr_kpis']
  )
}}

SELECT
  jt.job_type, 
  SUM(f.vacancies_count) AS total_vacancies,
  ROUND(AVG(f.salary_in_lpa), 2) AS avg_salary,
  SUM(f.vacancies_count) / ROUND(AVG(f.salary_in_lpa), 2) AS vacancy_to_salary_ratio
FROM {{ ref('fact_job_postings_final') }} f
JOIN {{ ref('dim_job_type') }} jt
  ON f.job_type_id = jt.dim_job_type_id  
GROUP BY jt.job_type  
ORDER BY vacancy_to_salary_ratio DESC
