-- Each customer's money movements per business day (Asia/Dhaka), successful payments only: the
-- unit AML thresholds and typologies are defined on. A customer is a personal account (U...).
with legs as (
    select receiver_id as customer_id, event_time, 'CASH_IN' as leg, amount_paisa, sender_id as counterparty
    from {{ ref('stg_payments') }} where status = 'SUCCESS' and txn_type = 'CASH_IN'
    union all
    select sender_id, event_time, 'CASH_OUT', amount_paisa, receiver_id
    from {{ ref('stg_payments') }} where status = 'SUCCESS' and txn_type = 'CASH_OUT'
    union all
    select receiver_id, event_time, 'P2P_IN', amount_paisa, sender_id
    from {{ ref('stg_payments') }} where status = 'SUCCESS' and txn_type = 'P2P'
    union all
    select sender_id, event_time, 'P2P_OUT', amount_paisa, receiver_id
    from {{ ref('stg_payments') }} where status = 'SUCCESS' and txn_type = 'P2P'
),

near_limit as (
    select {{ var('per_txn_limit_bdt') }} * {{ var('structuring_near_limit_share') }} * 100 as paisa
)

select
    l.customer_id,
    {{ dhaka_date('l.event_time') }} as business_date,
    sum(case when leg = 'CASH_IN' then amount_paisa else 0 end) / 100.0 as cash_in_bdt,
    sum(case when leg = 'CASH_IN' then 1 else 0 end) as cash_ins,
    sum(case when leg = 'CASH_IN' and amount_paisa >= n.paisa then 1 else 0 end) as near_limit_cash_ins,
    count(distinct case when leg = 'CASH_IN' and amount_paisa >= n.paisa then counterparty end) as near_limit_agents,
    sum(case when leg = 'CASH_OUT' then amount_paisa else 0 end) / 100.0 as cash_out_bdt,
    sum(case when leg = 'P2P_IN' then amount_paisa else 0 end) / 100.0 as p2p_in_bdt,
    count(distinct case when leg = 'P2P_IN' then counterparty end) as p2p_in_senders,
    sum(case when leg = 'P2P_OUT' then amount_paisa else 0 end) / 100.0 as p2p_out_bdt
from legs l
cross join near_limit n
where l.customer_id like 'U%'
group by 1, 2
