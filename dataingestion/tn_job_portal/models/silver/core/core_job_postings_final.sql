{{
  config(
    materialized='table',
    schema='silver_core',  
    tags=['core_jobs'],
    description='Final staging table with transformed job postings ready for analytics'
  )
}}

SELECT  
  url AS job_url,
  job_title,
  job_role,
  industry_sector,
  company,  
  location,  
  gender_requirement,
  min_age,
  max_age,
  experience_years,
  job_type,  
  open_until AS application_deadline, 
  specialization,
  skills,  
  additional_skills,  
  job_description,
  vacancies_count,
  experience_level,
  salary_in_lpa
FROM {{ ref('core_job_postings_transform') }}  