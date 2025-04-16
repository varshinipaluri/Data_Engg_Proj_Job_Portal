{{ config(
    materialized='view',
    schema='facts',
    tags=['facts']
) }}
WITH fact_data AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY base.job_title, base.company, base.location) AS job_posting_id,  

        comp.dim_company_id AS company_id,
        loc.dim_location_id AS location_id,
        role.dim_job_role_id AS job_role_id,
        sect.dim_industry_sector_id AS industry_sector_id,
        gender.dim_gender_requirement_id AS gender_requirement_id,
        exp.dim_experience_level_id AS experience_level_id,
        spec.dim_specialization_id AS specialization_id, 
        jtype.dim_job_type_id AS job_type_id,

        -- Fact attributes
        base.job_url,
        base.application_deadline,
        base.vacancies_count,
        base.salary_in_lpa,
        base.min_age,
        base.max_age,
        base.experience_years,
        base.skills,
        base.additional_skills

    FROM {{ ref('core_job_postings_final') }} base  
    LEFT JOIN {{ ref('dim_company') }} comp ON base.company = comp.company
    LEFT JOIN {{ ref('dim_location') }} loc ON base.location = loc.location
    LEFT JOIN {{ ref('dim_job_role') }} role ON base.job_role = role.job_role
    LEFT JOIN {{ ref('dim_industry_sector') }} sect ON base.industry_sector = sect.industry_sector
    LEFT JOIN {{ ref('dim_gender_requirement') }} gender ON base.gender_requirement = gender.gender_requirement
    LEFT JOIN {{ ref('dim_experience_level') }} exp ON base.experience_level = exp.experience_level
    LEFT JOIN {{ ref('dim_specialization') }} spec ON base.specialization = spec.specialization
    LEFT JOIN {{ ref('dim_job_type') }} jtype ON base.job_type = jtype.job_type
)

SELECT
    job_posting_id,
    job_url,
    company_id,
    location_id,
    job_role_id,
    industry_sector_id,
    gender_requirement_id,
    experience_level_id,
    specialization_id,
    job_type_id,  
    application_deadline,
    vacancies_count,
    salary_in_lpa,
    min_age,
    max_age,
    experience_years,
    skills,
    additional_skills
FROM fact_data
