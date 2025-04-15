{{ config(
    materialized='table', 
    schema='dimensions', 
    tags=['dimensions']
) }}

{{ create_dimension_table('dim_gender_requirement', 'gender_requirement') }}
