-- macros/clean_text.sql

{% macro clean_text(field, default="not_specified") %}
    COALESCE(NULLIF(TRIM({{ field }}), ''), '{{ default }}')
{% endmacro %}
