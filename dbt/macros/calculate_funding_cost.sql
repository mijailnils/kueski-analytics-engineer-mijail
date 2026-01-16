{% macro calculate_funding_cost(charge_off_amount) %}
    coalesce({{ charge_off_amount }}, 0)
{% endmacro %}