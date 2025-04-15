-- macros/create_bridge_table.sql
{% macro create_bridge_table(fact_table, fact_column, dimension_table, dimension_column, bridge_table_name, delimiter) %}
    {{ config(
        materialized='table',
        schema='facts',
        tags=['bridge']
    ) }}

    WITH bridge_data AS (
        SELECT
            job_posting_id,
            flattened.value::string AS {{ dimension_column }}
        FROM {{ ref(fact_table) }},
        LATERAL FLATTEN(input => SPLIT({{ ref(fact_table) }}.{{ fact_column }}, '{{ delimiter }}')) AS flattened
    )
    SELECT
        job_posting_id,
        {{ dimension_column }} AS {{ dimension_column }}_id
    FROM bridge_data
{% endmacro %}
