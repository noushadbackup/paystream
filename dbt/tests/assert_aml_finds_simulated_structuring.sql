-- Recall on the simulator: every structuring episode the generator produced that is complete in the
-- data (enough near-limit cash-ins, at more than one agent, in one business day) is a STRUCTURING hit.
-- Episodes cut off by the end of the data are left out: they do not meet the definition yet.
with episodes as (
    select l.account_id, {{ dhaka_date('p.event_time') }} as business_date
    from {{ ref('stg_fraud_labels') }} l
    join {{ ref('stg_payments') }} p on p.txn_id = l.txn_id
    where l.scenario = 'structuring'
    group by 1, 2
    having count(*) >= {{ var('structuring_min_cash_ins') }} and count(distinct p.sender_id) >= 2
)

select e.*
from episodes e
left join {{ ref('aml_typology_hits') }} h
  on h.customer_id = e.account_id and h.business_date = e.business_date and h.typology = 'STRUCTURING'
where h.customer_id is null
