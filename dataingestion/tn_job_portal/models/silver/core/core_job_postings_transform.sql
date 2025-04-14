{{
  config(
    materialized='ephemeral',
    tags=['core_jobs'],
    description='Applies business transformations to cleaned job postings data'
  )
}}

WITH transformed AS (
  SELECT
    *,
    -- Salary transformation (Snowflake compatible)
    CASE
      -- Monthly salary (convert to annual)
      WHEN LOWER(salary) LIKE '%p.m%' OR LOWER(salary) LIKE '%pm%' OR LOWER(salary) LIKE '%per month%' THEN
        TRY_CAST(
          REGEXP_REPLACE(
            REGEXP_SUBSTR(salary, '\\d{1,3}(,\\d{3})*'), 
            ',', ''
          ) AS FLOAT
        ) * 12

      -- LPA or Lakh salary
      WHEN LOWER(salary) LIKE '%lpa%' OR LOWER(salary) LIKE '%lakh%' THEN
        TRY_CAST(
          REGEXP_REPLACE(
            REGEXP_SUBSTR(salary, '\\d+(\\.\\d+)?'), 
            '[^0-9.]', ''
          ) AS FLOAT
        ) * 100000

      -- Salary range (midpoint)
      WHEN REGEXP_LIKE(salary, '\\d+\\s*-\\s*\\d+') THEN
        (
          TRY_CAST(REGEXP_SUBSTR(salary, '\\d+', 1, 1) AS FLOAT) +
          TRY_CAST(REGEXP_SUBSTR(salary, '\\d+', 1, 2) AS FLOAT)
        ) / 2

      ELSE NULL
    END AS annual_salary,
    
    -- Salary in LPA (Lakhs Per Annum)
    CASE
      WHEN annual_salary IS NOT NULL THEN annual_salary / 100000
      ELSE NULL
    END AS salary_in_lpa,

    -- Experience processing (Snowflake compatible)
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
    
    -- Gender standardization
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

    -- Openings
    COALESCE(openings, 0) AS vacancies_count
  FROM {{ ref('stg_job_postings_clean') }}
)

SELECT
  *
FROM transformed