{% snapshot loan_metrics_snapshot %}
    {{
      config(
        target_schema='snapshots',
        unique_key='loan_id',
        strategy='check',
        check_cols=['total_repaid','last_repayment_date']
      )
    }}

select * from {{ ref('repayments_agg') }}

{% endsnapshot %}
