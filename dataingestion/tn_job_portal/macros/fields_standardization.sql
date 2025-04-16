-- macros/field_standardization.sql
{% macro field_standardization(field, replace_char=',', target_char='|') %}
    COALESCE(
        NULLIF(TRIM(REGEXP_REPLACE({{ field }}, '{{ replace_char }}', '{{ target_char }}')), ''),
        'not_specified'
    )
{% endmacro %}