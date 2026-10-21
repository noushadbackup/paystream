-- What the inline checks did with the money, per day and outcome: how much was stopped before it
-- moved, and how much labelled fraud was approved anyway. Fraud is known only once labelled, so
-- recent days fill in as cases close.
with fraud as (
    select distinct txn_id from {{ ref('stg_fraud_labels') }} where disposition = 'FRAUD'
)

select
    d.decision_date,
    d.outcome,
    count(*) as decisions,
    sum(d.amount_paisa) / 100.0 as amount_bdt,
    sum(case when d.held_by_model then 1 else 0 end) as held_by_model,
    sum(case when d.degraded then 1 else 0 end) as degraded,
    sum(case when f.txn_id is not null then 1 else 0 end) as labelled_fraud,
    sum(case when f.txn_id is not null then d.amount_paisa else 0 end) / 100.0 as labelled_fraud_bdt,
    {{ p95('d.latency_us / 1000.0') }} as latency_p95_ms
from {{ ref('stg_decisions') }} d
left join fraud f on f.txn_id = d.txn_id
group by 1, 2
