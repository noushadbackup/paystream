-- How each rule performs, per day: volume, reach, latency, and how many of its alerts land on
-- accounts known to be fraudulent. Shadow rules are reported apart, which is the point of running
-- them: the numbers a rule would have had before it is allowed to act. Accounts an analyst cleared
-- (NOT_FRAUD) count against the rule as known false positives.
with labelled as (
    select distinct account_id from {{ ref('stg_fraud_labels') }} where disposition = 'FRAUD'
),

cleared as (
    select distinct account_id from {{ ref('stg_fraud_labels') }} where disposition = 'NOT_FRAUD'
    except
    select account_id from labelled
)

select
    a.event_date,
    a.rule_name,
    a.shadow,
    max(a.rule_version) as rule_version,
    count(*) as alerts,
    count(distinct a.account_id) as keys_flagged,
    sum(case when l.account_id is not null then 1 else 0 end) as alerts_on_labelled,
    cast(sum(case when l.account_id is not null then 1 else 0 end) as double) / count(*) as precision_vs_labels,
    sum(case when c.account_id is not null then 1 else 0 end) as alerts_on_cleared,
    {{ p95(millis_between('a.ingest_time', 'a.detected_at')) }} as detection_p95_ms
from {{ ref('stg_fraud_alerts') }} a
left join labelled l on l.account_id = a.account_id
left join cleared c on c.account_id = a.account_id
group by 1, 2, 3
