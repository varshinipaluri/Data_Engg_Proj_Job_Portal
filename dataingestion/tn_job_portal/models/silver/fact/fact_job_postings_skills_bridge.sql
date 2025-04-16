-- fact_job_postings_skills_bridge.sql
{{ config(
    materialized='table',
    schema='facts',
    tags=['bridge']
) }}

{{ create_bridge_table(
    fact_table='fact_job_postings',  
    fact_column='skills', 
    dimension_table='dim_skills',
    dimension_column='skills',
    bridge_table_name='fact_job_postings_skills_bridge', 
    delimiter='|'  
) }}




