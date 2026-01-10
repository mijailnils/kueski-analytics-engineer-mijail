{% macro accrual_amount(principal, annual_rate, days_expr) -%}
  ( ({{ principal }}) * ({{ annual_rate }}) / 365.0 ) * ({{ days_expr }})
{%- endmacro %}
