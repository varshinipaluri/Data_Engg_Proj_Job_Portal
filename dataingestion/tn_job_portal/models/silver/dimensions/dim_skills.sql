{{ config(
    materialized='table', 
    schema='dimensions', 
    tags=['dimensions']
) }}

{{ create_dimension_table(
    'dim_skills', 
    'skills', 
    is_multivalued=True, 
    delimiter='|'
) }}
