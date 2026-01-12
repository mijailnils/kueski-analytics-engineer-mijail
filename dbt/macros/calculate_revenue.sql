{% macro calculate_revenue(interest, fees, penalty, tax_interest, tax_fees, tax_penalty) %}
    coalesce({{ interest }}, 0) + 
    coalesce({{ fees }}, 0) + 
    coalesce({{ penalty }}, 0) +
    coalesce({{ tax_interest }}, 0) +
    coalesce({{ tax_fees }}, 0) +
    coalesce({{ tax_penalty }}, 0)
{% endmacro %}