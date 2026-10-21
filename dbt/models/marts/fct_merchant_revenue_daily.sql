select
    event_date,
    receiver_id as merchant_id,
    count(*) as payments,
    count(distinct sender_id) as unique_customers,
    sum(amount_paisa) as revenue_paisa
from {{ ref('stg_payments') }}
where txn_type = 'MERCHANT_PAY' and status = 'SUCCESS'
group by 1, 2
