-- Alerts are delivered at-least-once; alert_id is deterministic, so one row per id is the truth.
with ranked as (
    select *, row_number() over (partition by alert_id order by detected_at) as n
    from {{ source('silver', 'fraud_alerts') }}
)

select alert_id, rule, account_id, trigger_txn_id, event_time, cast(event_time as date) as event_date,
       ingest_time, detected_at, score, details
from ranked
where n = 1
