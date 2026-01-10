{{
    config(
        materialized='view',
        tags=['intermediate', 'vintages']
    )
}}

-- Filter loans to Q1 2025 vintages (January, February, March)
-- These are the three vintages we need to analyze per challenge requirements

with q1_vintages as (
    select *
    from {{ ref('int_loans_latest') }}
    where vintage_month in ('2025-01', '2025-02', '2025-03')
)

select * from q1_vintages