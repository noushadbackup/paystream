-- Suspicious Transaction Report candidates: one row per customer, from AML typologies and from
-- fraud cases an analyst confirmed. The money-laundering reporting officer decides what is filed;
-- this is the worklist, with the evidence gathered in one place.
with leads as (
    select customer_id, business_date, typology, amount_bdt, cast(null as varchar) as case_id
    from {{ ref('aml_typology_hits') }}
    union all
    select l.account_id, {{ dhaka_date('p.event_time') }}, 'CONFIRMED_FRAUD_CASE', p.amount_paisa / 100.0, l.case_id
    from {{ ref('stg_fraud_labels') }} l
    join {{ ref('stg_payments') }} p on p.txn_id = l.txn_id
    where l.source = 'analyst' and l.disposition = 'FRAUD' and l.account_id like 'U%'
)

select
    customer_id,
    min(business_date) as first_date,
    max(business_date) as last_date,
    {{ list_agg('typology') }} as typologies,
    count(distinct typology) as typology_count,
    count(distinct business_date) as days_with_hits,
    sum(amount_bdt) as amount_bdt,
    {{ list_agg('case_id') }} as case_ids
from leads
group by 1
