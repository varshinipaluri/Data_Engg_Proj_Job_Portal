{{
  config(
    materialized='table',
    schema='bronze_staging',
    tags=['stage_jobs'],
    description='Cleaned staging model for job postings. Applies standardization and cleaning rules.'
  )
}}

SELECT

  -- URL
  url,

  -- Cleaned text fields using the clean_text macro
  CAST({{ clean_text('title') }} AS VARCHAR(255)) AS job_title,
  CAST({{ clean_text('job_role') }} AS VARCHAR(255)) AS job_role,
  CAST({{ clean_text('sector') }} AS VARCHAR(255)) AS industry_sector,
  CAST({{ clean_text('company') }} AS VARCHAR(255)) AS company,
  CAST({{ clean_text('location') }} AS VARCHAR(255)) AS location,
  CAST({{ clean_text('job_type') }} AS VARCHAR(255)) AS job_type,

  -- Standardized special fields with the clean_special_field macro
  CAST({{ field_standardization('specialization') }} AS VARCHAR(1000)) AS specialization,
  CAST({{ field_standardization('skills') }} AS VARCHAR(10000)) AS skills,
  CAST({{ field_standardization('additional_skills') }} AS VARCHAR(10000)) AS additional_skills,

  -- Cleaned description
  CAST(
    REGEXP_REPLACE(
      COALESCE(NULLIF(TRIM(description), ''), 'No description provided'), 
      '[\\r\\n\\t]+', ' '
    ) AS VARCHAR(100000)
  ) AS job_description,

 --Other fields
  openings,
  TO_DATE(open_until, 'DD-MM-YYYY') AS open_until,
  salary ,
  gender,
  age_limit,
  experience

FROM {{ ref('stg_job_postings_base') }}
