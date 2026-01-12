{% macro assign_cohort(date_col) %}
    date_trunc('month', {{ date_col }})::date
{% endmacro %}