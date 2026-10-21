-- Second line of defence for dedup: Flink drops retries within its state TTL (2 h);
-- anything older that got through is removed here, keeping the earliest-ingested copy.
with ranked as (
    select
        *,
        row_number() over (partition by txn_id order by ingest_time) as copy_number
    from {{ source('silver', 'payments') }}
)

select
    txn_id,
    event_time,
    cast(event_time as date) as event_date,
    ingest_time,
    txn_type,
    sender_id,
    receiver_id,
    amount_paisa,
    device_id,
    district,
    status,
    coalesce(channel, 'UNKNOWN') as channel
from ranked
where copy_number = 1
