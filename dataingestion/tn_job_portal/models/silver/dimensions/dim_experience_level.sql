{{ config(
    materialized='table', 
    schema='dimensions', 
    tags=['dimensions']
) }}

{{ create_dimension_table('Dim_Experience_Level', 'experience_level') }}
