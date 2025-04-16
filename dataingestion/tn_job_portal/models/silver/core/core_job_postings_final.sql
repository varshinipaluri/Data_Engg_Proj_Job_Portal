{{
  config(
    materialized='table',
    schema='silver_core',  
    tags=['core_jobs'],
    description='Final staging table with transformed job postings ready for analytics'
  )
}}

SELECT  
  job_url, -- Removed redundant aliasing
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
  application_deadline, 
  specialization,
  skills,  
  additional_skills,  
  job_description,
  vacancies_count,
  experience_level,
  annual_salary,
  salary_in_lpa
FROM {{ ref('core_job_postings_transform') }}


