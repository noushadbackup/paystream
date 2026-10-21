select txn_id, leg, account_id, amount_paisa, posted_at
from {{ source('silver', 'ledger_entries') }}
