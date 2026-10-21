-- The slow, correct numbers: built from silver, so late events count on the day they happened.
-- Compare with gold.kpi_minute (streaming) to see how much lateness moves the totals.
select
    event_date,
    txn_type,
    count(*) as txn_count,
    sum(case when status = 'SUCCESS' then 1 else 0 end) as success_count,
    sum(case when status = 'SUCCESS' then amount_paisa else 0 end) as success_amount_paisa,
    round(1.0 * sum(case when status = 'FAILED' then 1 else 0 end) / count(*), 4) as failure_rate,
    count(distinct sender_id) as unique_senders,
    sum(case when ingest_time > event_time + interval '1' minute then 1 else 0 end) as late_count
from {{ ref('stg_payments') }}
group by 1, 2
