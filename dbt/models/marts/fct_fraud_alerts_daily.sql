select
    event_date,
    rule_name as rule,
    count(*) as alerts,
    count(distinct account_id) as accounts_flagged,
    {{ p95(millis_between('ingest_time', 'detected_at')) }} as detection_p95_ms
from {{ ref('stg_fraud_alerts') }}
where not shadow
group by 1, 2
