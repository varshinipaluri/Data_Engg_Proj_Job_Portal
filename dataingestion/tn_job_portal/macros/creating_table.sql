{% macro create_dimension_table(table_name, column_name, is_multivalued=False, delimiter='|', ref_model='core_job_postings_final') %}

{% if is_multivalued %}
SELECT 
    ROW_NUMBER() OVER (ORDER BY value) AS {{ table_name }}_id,
    value AS {{ column_name }}
FROM {{ ref(ref_model) }},
LATERAL FLATTEN(input => SPLIT({{ column_name }}, '{{ delimiter }}'))
WHERE value IS NOT NULL

{% else %}
SELECT 
    ROW_NUMBER() OVER (ORDER BY {{ column_name }}) AS {{ table_name }}_id,
    {{ column_name }}
FROM (
    SELECT DISTINCT {{ column_name }}
    FROM {{ ref(ref_model) }}
    WHERE {{ column_name }} IS NOT NULL
) AS unique_{{ table_name }}

{% endif %}
{% endmacro %}
