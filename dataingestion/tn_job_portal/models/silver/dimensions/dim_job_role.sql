{{ config(
    materialized='table', 
    schema='dimensions', 
    tags=['dimensions']
) }}

{{ create_dimension_table('dim_job_Role', 'job_role') }}
