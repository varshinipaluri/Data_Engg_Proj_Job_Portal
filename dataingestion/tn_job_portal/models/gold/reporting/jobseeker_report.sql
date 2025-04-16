{{ config(
    materialized='table',
    schema='gold_reporting',
    tags=['job_seeker_report']
) }}

WITH job_seeker_report_data AS (
    SELECT 
        f.job_posting_id,
        f.job_url,
        f.application_deadline,
        f.vacancies_count,
        f.salary_in_lpa,
        f.min_age,
        f.max_age,
        f.experience_years,
        
        -- Join with dimension tables for Job Seeker Attributes
        comp.company,
        loc.location,
        role.job_role,
        spec.specialization,
        ind.industry_sector,
        gen.gender_requirement,
        exp.experience_level,
        jt.job_type,
        
        
        -- Join with bridge tables for multi-valued attributes (skills, additional_skills)
        skill_bridge.skills_id AS skill_id,
        addskill_bridge.additional_skills_id AS addskill_id
        
    FROM {{ ref('fact_job_postings_final') }} f
    
    -- Join with dimension tables for company, location, job role, specialization, etc.
    LEFT JOIN {{ ref('dim_company') }} comp ON f.company_id = comp.dim_company_id
    LEFT JOIN {{ ref('dim_location') }} loc ON f.location_id = loc.dim_location_id
    LEFT JOIN {{ ref('dim_job_role') }} role ON f.job_role_id = role.dim_job_role_id
    LEFT JOIN {{ ref('dim_specialization') }} spec ON f.specialization_id = spec.dim_specialization_id
    LEFT JOIN {{ ref('dim_industry_sector') }} ind ON f.industry_sector_id = ind.dim_industry_sector_id
    LEFT JOIN {{ ref('dim_gender_requirement') }} gen ON f.gender_requirement_id = gen.dim_gender_requirement_id
    LEFT JOIN {{ ref('dim_experience_level') }} exp ON f.experience_level_id = exp.dim_experience_level_id
    LEFT JOIN {{ ref('dim_job_type') }} jt ON f.job_type_id = jt.dim_job_type_id
    
    -- Join with bridge tables for skills and additional skills
    LEFT JOIN {{ ref('fact_job_postings_skills_bridge') }} skill_bridge ON f.job_posting_id = skill_bridge.job_posting_id
    LEFT JOIN {{ ref('fact_job_postings_additional_skills_bridge') }} addskill_bridge ON f.job_posting_id = addskill_bridge.job_posting_id
)

SELECT 
    job_posting_id,
    job_url,
    company,
    location,
    job_role,
    specialization,
    industry_sector,
    gender_requirement,
    experience_level,
    job_type,
    application_deadline,
    vacancies_count,
    salary_in_lpa,
    min_age,
    max_age,
    experience_years,
  
    
    -- Skills and additional skills associated with the job posting
    skill_id,
    addskill_id
FROM job_seeker_report_data
ORDER BY application_deadline DESC
