-- Double-entry invariant, checked again on the lake copy: every transfer's legs sum to zero.
select txn_id, sum(amount_paisa) as total, count(*) as legs
from {{ ref('stg_ledger_entries') }}
group by txn_id
having sum(amount_paisa) <> 0 or count(*) <> 2
