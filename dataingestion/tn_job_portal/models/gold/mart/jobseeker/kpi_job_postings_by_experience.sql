--dataingestion\tn_job_portal\models\gold\mart\hr\kpi_vacancy_distribution_by_experience.sql
{{
  config(
    materialized='table',
    schema='gold_mart',
    tags=['jobseeker_kpis']
  )
}}

SELECT
  exp.experience_level,
  COUNT(*) AS total_job_postings
FROM {{ ref('fact_job_postings_final') }} fact
JOIN {{ ref('dim_experience_level') }} exp
  ON fact.experience_level_id = exp.dim_experience_level_id
GROUP BY exp.experience_level
ORDER BY total_job_postings DESC
