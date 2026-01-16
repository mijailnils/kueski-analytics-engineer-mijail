{% macro calculate_cogs(cac, servicing_cost, collection_cost) %}
    coalesce({{ cac }}, 0) + 
    coalesce({{ servicing_cost }}, 0) + 
    coalesce({{ collection_cost }}, 0)
{% endmacro %}