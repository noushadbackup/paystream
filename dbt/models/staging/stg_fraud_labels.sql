-- Ground truth: simulator labels, and analyst verdicts from closed cases (FRAUD or NOT_FRAUD).
-- Labels written before v2 have no source or disposition: they came from the simulator and mean fraud.
select distinct txn_id, scenario, account_id,
       coalesce(source, 'simulator') as source,
       coalesce(disposition, 'FRAUD') as disposition,
       case_id
from {{ source('silver', 'fraud_labels') }}
