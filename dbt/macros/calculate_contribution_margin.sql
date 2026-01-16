{% macro calculate_contribution_margin(revenue, funding_cost, cogs) %}
    coalesce({{ revenue }}, 0) - 
    coalesce({{ funding_cost }}, 0) - 
    coalesce({{ cogs }}, 0)
{% endmacro %}