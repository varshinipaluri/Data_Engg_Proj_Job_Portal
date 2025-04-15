{{ config(
    materialized='table', 
    schema='dimensions', 
    tags=['dimensions']
) }}

{{ create_dimension_table('dim_industry_sector', 'industry_sector') }}
