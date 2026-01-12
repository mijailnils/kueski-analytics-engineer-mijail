{% macro dpd_bucket(days_past_due_col) %}
    case
        when {{ days_past_due_col }} = 0 then 'Current'
        when {{ days_past_due_col }} between 1 and 30 then 'DPD 1-30'
        when {{ days_past_due_col }} between 31 and 60 then 'DPD 31-60'
        when {{ days_past_due_col }} between 61 and 90 then 'DPD 61-90'
        when {{ days_past_due_col }} > 90 then 'DPD 90+'
        else 'Unknown'
    end
{% endmacro %}