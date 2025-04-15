{% macro create_bridge_table(fact_table, fact_column, dimension_table, dimension_column, bridge_table_name, delimiter) %}
    {{ config(
        materialized='table',
        schema='facts',
        tags=['bridge']
    ) }}

    WITH bridge_data AS (
        SELECT
            job_posting_id,
            unnest(string_to_array({{ fact_table }}.{{ fact_column }}, '{{ delimiter }}')) AS {{ dimension_column }}
        FROM {{ ref(fact_table) }}
    )
    SELECT
        job_posting_id,
        {{ dimension_column }} AS {{ dimension_column }}_id
    FROM bridge_data;
{% endmacro %}
