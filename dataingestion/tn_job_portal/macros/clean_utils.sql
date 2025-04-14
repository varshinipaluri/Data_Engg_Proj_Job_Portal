-- macros/clean_utils.sql

{% macro clean_text(field, default="not_specified") %}
    COALESCE(NULLIF(TRIM({{ field }}), ''), '{{ default }}')
{% endmacro %}
