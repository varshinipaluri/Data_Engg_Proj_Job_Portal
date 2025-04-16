{{
  config(
    materialized='ephemeral',
    tags=['core_jobs'],
    description='Applies business transformations to cleaned job postings data'
  )
}}

WITH transformed AS (
  SELECT
    url AS job_url,
    job_title,
    job_role, 
    industry_sector,
    job_type,
    specialization,
    company,  
    location, 
    open_until AS application_deadline,
    skills,
    additional_skills,
    job_description,
    salary,

    -- Gender requirement transformation
    CASE
      WHEN LOWER(gender) = 'male' THEN 'male_only'
      WHEN LOWER(gender) = 'female' THEN 'female_only'
      WHEN LOWER(gender) = 'transgender' THEN 'transgender_only'
      ELSE 'any_gender'
    END AS gender_requirement,

    -- Age processing
    CASE
      WHEN age_limit = 'N/A' THEN NULL
      ELSE TRY_CAST(SPLIT_PART(age_limit, '-', 1) AS INTEGER)
    END AS min_age,

    CASE
      WHEN age_limit = 'N/A' THEN NULL
      ELSE TRY_CAST(SPLIT_PART(age_limit, '-', 2) AS INTEGER)
    END AS max_age,

    -- Experience processing
    CASE
      WHEN experience ILIKE '%fresher%' THEN 0
      ELSE COALESCE(
        TRY_CAST(REGEXP_REPLACE(REGEXP_SUBSTR(experience, '(\\d+)'), '[^0-9]', '') AS INTEGER), 
        0
      )
    END AS experience_years,
    
    -- Experience level categorization
    CASE
      WHEN experience_years = 0 THEN 'Fresher'
      WHEN experience_years BETWEEN 1 AND 3 THEN 'Junior'
      WHEN experience_years BETWEEN 4 AND 7 THEN 'Mid-level'
      WHEN experience_years BETWEEN 8 AND 15 THEN 'Senior'
      WHEN experience_years > 15 THEN 'Executive'
      ELSE 'Not specified'
    END AS experience_level,

    -- Annual salary transformation
    CASE
      -- Monthly salary range like "15000 - 25000 p.m"
      WHEN REGEXP_LIKE(LOWER(salary), '\\d+\\s*-\\s*\\d+\\s*(p\\.m|pm|per month)') THEN
        (
          TRY_CAST(REGEXP_SUBSTR(salary, '\\d+', 1, 1) AS FLOAT) +
          TRY_CAST(REGEXP_SUBSTR(salary, '\\d+', 1, 2) AS FLOAT)
        ) / 2 * 12

      -- Single monthly value like "15000 p.m"
      WHEN LOWER(salary) LIKE '%p.m%' OR LOWER(salary) LIKE '%pm%' OR LOWER(salary) LIKE '%per month%' THEN
        TRY_CAST(REGEXP_REPLACE(REGEXP_SUBSTR(salary, '\\d+(,\\d+)*'), ',', '') AS FLOAT) * 12

      -- LPA range like "3-5 LPA"
      WHEN REGEXP_LIKE(LOWER(salary), '\\d+\\s*-\\s*\\d+\\s*(lpa|lakh)') THEN
        (
          TRY_CAST(REGEXP_SUBSTR(salary, '\\d+', 1, 1) AS FLOAT) +
          TRY_CAST(REGEXP_SUBSTR(salary, '\\d+', 1, 2) AS FLOAT)
        ) / 2 * 100000

      -- Single LPA like "4 LPA"
      WHEN LOWER(salary) LIKE '%lpa%' OR LOWER(salary) LIKE '%lakh%' THEN
        TRY_CAST(REGEXP_REPLACE(salary, '[^0-9.]', '') AS FLOAT) * 100000

      -- Above 1 Lakh p.m (assume 110000 as base)
      WHEN LOWER(salary) LIKE '%above 1 lakh%' THEN 110000 * 12

      ELSE NULL
    END AS annual_salary,

    -- Convert to LPA
    CASE
      WHEN annual_salary IS NOT NULL THEN ROUND(annual_salary / 100000, 2)
      ELSE NULL
    END AS salary_in_lpa,

    COALESCE(openings, 0) AS vacancies_count

  FROM {{ ref('stg_job_postings_clean') }}
)

SELECT * FROM transformed
