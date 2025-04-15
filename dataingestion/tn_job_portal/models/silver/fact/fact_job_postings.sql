-- fact_job_postings.sql
{{ config(
    materialized='table',
    description='Fact table for job postings with foreign keys to dimensions',
    schema='gold_mart',
    tags=['mart_jobs']
) }}

WITH base AS (
    SELECT * FROM {{ ref('core_job_postings_final') }}
),

fact_data AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY job_title) AS job_posting_id,

        -- Use the correct ID column names from dimensions
        comp.dim_company_id AS company_id,
        loc.dim_location_id AS location_id,
        role.dim_job_role_id AS job_role_id,
        sect.dim_industry_sector_id AS industry_sector_id,
        gender.dim_gender_requirement_id AS gender_requirement_id,
        exp.dim_experience_level_id AS experience_level_id,
        spec.dim_specialization_id AS specialization_id,

        -- Fact attributes
        base.job_type,
        base.application_deadline,
        base.vacancies_count,
        base.salary_in_lpa,
        base.min_age,
        base.max_age,
        base.experience_years

    FROM base
    LEFT JOIN {{ ref('dim_company') }} comp ON base.company = comp.company
    LEFT JOIN {{ ref('dim_location') }} loc ON base.location = loc.location
    LEFT JOIN {{ ref('dim_job_role') }} role ON base.job_role = role.job_role
    LEFT JOIN {{ ref('dim_industry_sector') }} sect ON base.industry_sector = sect.industry_sector
    LEFT JOIN {{ ref('dim_gender_requirement') }} gender ON base.gender_requirement = gender.gender_requirement
    LEFT JOIN {{ ref('dim_experience_level') }} exp ON base.experience_level = exp.experience_level
    LEFT JOIN {{ ref('dim_specialization') }} spec ON base.specialization = spec.specialization
)

-- Reference bridge tables for multi-valued attributes
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
    f.experience_years
FROM fact_data f

LEFT JOIN {{ ref('fact_job_postings_skills_bridge') }} skill_bridge
    ON f.job_posting_id = skill_bridge.job_posting_id
LEFT JOIN {{ ref('fact_job_postings_additional_skills_bridge') }} addskill_bridge
    ON f.job_posting_id = addskill_bridge.job_posting_id;
