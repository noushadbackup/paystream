-- Daily reconciliation: every successful gateway payment must be posted in the ledger exactly once,
-- with the same amount, and the ledger must contain nothing the gateway never saw.
with stream as (
    select txn_id, event_date, amount_paisa
    from {{ ref('stg_payments') }}
    where status = 'SUCCESS'
),

ledger as (
    select txn_id, event_date, amount_paisa from {{ ref('stg_ledger_transfers') }}
),

joined as (
    select
        coalesce(s.event_date, l.event_date) as event_date,
        s.txn_id as stream_txn,
        l.txn_id as ledger_txn,
        s.amount_paisa as stream_amount,
        l.amount_paisa as ledger_amount
    from stream s
    full outer join ledger l on s.txn_id = l.txn_id
)

select
    event_date,
    count(stream_txn) as stream_txns,
    count(ledger_txn) as ledger_txns,
    coalesce(sum(stream_amount), 0) as stream_amount_paisa,
    coalesce(sum(ledger_amount), 0) as ledger_amount_paisa,
    sum(case when ledger_txn is null then 1 else 0 end) as missing_in_ledger,
    sum(case when stream_txn is null then 1 else 0 end) as missing_in_stream,
    sum(case when stream_amount <> ledger_amount then 1 else 0 end) as amount_mismatches,
    case
        when sum(case when ledger_txn is null or stream_txn is null or stream_amount <> ledger_amount
                      then 1 else 0 end) = 0 then 'MATCHED'
        else 'BROKEN'
    end as status
from joined
group by 1
