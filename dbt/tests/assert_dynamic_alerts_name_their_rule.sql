-- An analyst-rule alert that does not say which rule and version fired cannot be audited.
select alert_id
from {{ ref('stg_fraud_alerts') }}
where (rule = 'DYNAMIC') <> (rule_id is not null and rule_version is not null)
