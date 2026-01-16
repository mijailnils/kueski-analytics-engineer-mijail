{% macro calculate_financial_margin(revenue, funding_cost) %}
    coalesce({{ revenue }}, 0) - coalesce({{ funding_cost }}, 0)
{% endmacro %}