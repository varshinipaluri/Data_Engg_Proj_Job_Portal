--models\gold\mart\jobseeker\kpi_location_opportunities.sql
{{
  config(
    materialized='view',
    schema='gold_mart',
    tags=['jobseeker_kpis']
  )
}}
SELECT
    loc.location AS location,
    role.job_role AS job_role,
    COUNT(*) AS job_count,
    ROUND(AVG(fact.salary_in_lpa), 2) AS avg_salary,
    LISTAGG(DISTINCT comp.company, ', ') WITHIN GROUP (ORDER BY comp.company) AS hiring_companies
FROM {{ ref('fact_job_postings_final') }} fact
JOIN {{ ref('dim_location') }} loc 
    ON fact.location_id = loc.dim_location_id
JOIN {{ ref('dim_job_role') }} role 
    ON fact.job_role_id = role.dim_job_role_id
JOIN {{ ref('dim_company') }} comp 
    ON fact.company_id = comp.dim_company_id
GROUP BY loc.location, role.job_role
ORDER BY job_count DESC