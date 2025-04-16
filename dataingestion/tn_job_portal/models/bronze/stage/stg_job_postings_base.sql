-- models/bronze/stage/stg_job_postings_base.sql
{{
  config(
    materialized='ephemeral',
    tags=['stage_jobs'],
  )
}}

SELECT
  url,
  title,
  company,
  sector,
  job_role,
  salary,
  specialization,
  location,
  gender,
  age_limit,
  openings,
  experience,
  job_type,
  open_until,
  description,
  skills,
  additional_skills
FROM {{ ref('raw_job_postings') }}

