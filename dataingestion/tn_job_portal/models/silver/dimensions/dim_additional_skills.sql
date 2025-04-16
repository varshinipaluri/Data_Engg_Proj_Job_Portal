{{ config(
    materialized='table', 
    schema='dimensions', 
    tags=['dimensions']
) }}

{{ create_dimension_table(
    'dim_additional_skills', 
    'additional_skills', 
    is_multivalued=True, 
    delimiter='|'
) }}
    