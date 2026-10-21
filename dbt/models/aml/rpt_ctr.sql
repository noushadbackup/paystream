-- Cash Transaction Report: every customer-day whose cash in, or cash out, reaches the reporting
-- threshold. One row per customer, day and direction; report_id is stable across rebuilds, so a
-- resubmission replaces rather than duplicates.
with days as (
    select customer_id, business_date, 'CASH_IN' as direction, cash_in_bdt as amount_bdt, cash_ins as transactions
    from {{ ref('aml_customer_day') }} where cash_in_bdt >= {{ var('ctr_threshold_bdt') }}
    union all
    select customer_id, business_date, 'CASH_OUT', cash_out_bdt, null
    from {{ ref('aml_customer_day') }} where cash_out_bdt >= {{ var('ctr_threshold_bdt') }}
)

select
    'CTR-' || cast(business_date as varchar) || '-' || direction || '-' || customer_id as report_id,
    customer_id,
    business_date,
    direction,
    amount_bdt,
    transactions,
    {{ var('ctr_threshold_bdt') }} as threshold_bdt
from days
