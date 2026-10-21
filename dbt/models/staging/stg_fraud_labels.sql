-- Ground truth: which transactions belong to a fraud pattern and which account (or device) it is about.
select distinct txn_id, scenario, account_id
from {{ source('silver', 'fraud_labels') }}
