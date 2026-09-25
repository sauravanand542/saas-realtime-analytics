{% macro add_months(date_expr, months) %}
    {%- if target.type == 'snowflake' -%}
        cast(dateadd(month, {{ months }}, {{ date_expr }}) as date)
    {%- else -%}
        cast({{ date_expr }} + interval '{{ months }} months' as date)
    {%- endif -%}
{% endmacro %}
