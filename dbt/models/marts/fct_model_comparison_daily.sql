-- Champion and challenger on the same payments, per day: how far apart each puts fraud and
-- legitimate traffic. The promotion job makes the decision; this is what people look at first.
with fraud as (
    select distinct txn_id from {{ ref('stg_fraud_labels') }} where disposition = 'FRAUD'
),

scored as (
    select decision_date, txn_id, 'champion' as role, model_version as version, model_score as score
    from {{ ref('stg_decisions') }} where model_version is not null
    union all
    select decision_date, txn_id, 'challenger', challenger_version, challenger_score
    from {{ ref('stg_decisions') }} where challenger_version is not null
)

select
    s.decision_date,
    s.role,
    s.version,
    count(*) as scored,
    sum(case when f.txn_id is not null then 1 else 0 end) as labelled_fraud,
    avg(case when f.txn_id is not null then s.score end) as mean_score_fraud,
    avg(case when f.txn_id is null then s.score end) as mean_score_other
from scored s
left join fraud f on f.txn_id = s.txn_id
group by 1, 2, 3
