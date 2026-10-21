-- For sharing outside the AML team (regulator statistics, partner banks, research): customer ids
-- are replaced by salted pseudonyms, so the same customer can be followed across days without
-- being identified. The salt is a deployment secret.
select
    business_date,
    typology,
    {{ pseudonymize('customer_id') }} as customer_token,
    amount_bdt
from {{ ref('aml_typology_hits') }}
