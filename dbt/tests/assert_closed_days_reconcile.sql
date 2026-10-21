-- A closed day (before today, UTC) must reconcile to the paisa. Today is still in flight:
-- late events and CDC lag can legitimately leave it unmatched for a while.
select *
from {{ ref('recon_stream_vs_ledger') }}
where event_date < current_date
  and status <> 'MATCHED'
