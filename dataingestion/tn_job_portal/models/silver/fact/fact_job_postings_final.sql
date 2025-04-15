--models\silver\fact\fact_job_postings_final.sql
{{ config(
    materialized='table',
    description='Final fact table for job postings with foreign keys to dimensions and multi-valued attributes',
    schema='facts',
    tags=['facts']
) }}

WITH fact_data AS (
    SELECT

        f.job_posting_id,
        f.company_id,
        f.location_id,
        f.job_role_id,
        f.industry_sector_id,
        f.gender_requirement_id,
        f.experience_level_id,
        f.specialization_id,
        f.job_type,
        f.application_deadline,
        f.vacancies_count,
        f.salary_in_lpa,
        f.min_age,
        f.max_age,
        f.experience_years,
        
        -- Join with bridge tables for multi-valued attributes (skills, additional_skills)
        skill_bridge.skills_id AS skill_id,
        addskill_bridge.additional_skills_id AS addskill_id
    FROM {{ ref('fact_job_postings') }} f
    LEFT JOIN {{ ref('fact_job_postings_skills_bridge') }} skill_bridge
        ON f.job_posting_id = skill_bridge.job_posting_id
    LEFT JOIN {{ ref('fact_job_postings_additional_skills_bridge') }} addskill_bridge
        ON f.job_posting_id = addskill_bridge.job_posting_id
)

SELECT
    job_posting_id,
    company_id,
    location_id,
    job_role_id,
    industry_sector_id,
    gender_requirement_id,
    experience_level_id,
    specialization_id,
    job_type,
    application_deadline,
    vacancies_count,
    salary_in_lpa,
    min_age,
    max_age,
    experience_years,
    skill_id,
    addskill_id
FROM fact_data