--models\gold\mart\hr\kpi_vacancy_distribution_by_experience.sql
{{
  config(
    materialized='table',
    schema='gold_mart', 
    tags=['hr_kpis']
  )
}}

SELECT
  e.experience_level,  
  SUM(f.vacancies_count) AS total_vacancies
FROM {{ ref('fact_job_postings_final') }} f
JOIN {{ ref('dim_experience_level') }} e
  ON f.experience_level_id = e.dim_experience_level_id  
GROUP BY e.experience_level  
ORDER BY total_vacancies DESC
