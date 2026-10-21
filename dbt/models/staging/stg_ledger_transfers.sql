select
    txn_id,
    txn_type,
    amount_paisa,
    event_time,
    cast(event_time as date) as event_date,
    posted_at
from {{ source('silver', 'ledger_transfers') }}
