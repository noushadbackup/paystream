-- Agent float: e-money an agent holds. Cash-in spends float (agent sends e-money to the customer),
-- cash-out earns it back (customer sends e-money to the agent). A day of large negative net change
-- means the agent needs a float top-up from a distributor.
with movements as (
    select event_date, sender_id as agent_id, -amount_paisa as float_change, 'CASH_IN' as kind
    from {{ ref('stg_payments') }}
    where txn_type = 'CASH_IN' and status = 'SUCCESS'
    union all
    select event_date, receiver_id as agent_id, amount_paisa as float_change, 'CASH_OUT' as kind
    from {{ ref('stg_payments') }}
    where txn_type = 'CASH_OUT' and status = 'SUCCESS'
)

select
    event_date,
    agent_id,
    sum(case when kind = 'CASH_IN' then -float_change else 0 end) as cash_in_paisa,
    sum(case when kind = 'CASH_OUT' then float_change else 0 end) as cash_out_paisa,
    sum(float_change) as net_float_change_paisa,
    count(*) as transactions
from movements
group by 1, 2
