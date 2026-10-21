-- Money-laundering typologies, per customer and business day. A hit is a lead for the AML team,
-- not a conclusion: it feeds the STR candidate list, where a person decides.
--   STRUCTURING   several cash-ins just under the single-transaction limit, at more than one agent
--   PASS_THROUGH  most of the day's inflow leaves again the same day (layering, mule accounts)
--   FAN_IN        many distinct senders in a day (collection account)
select customer_id, business_date, 'STRUCTURING' as typology, cash_in_bdt as amount_bdt,
       cast(near_limit_cash_ins as varchar) || ' cash-ins near the limit at ' || cast(near_limit_agents as varchar)
           || ' agents' as evidence
from {{ ref('aml_customer_day') }}
where near_limit_cash_ins >= {{ var('structuring_min_cash_ins') }} and near_limit_agents >= 2

union all

select customer_id, business_date, 'PASS_THROUGH', cash_in_bdt + p2p_in_bdt,
       'in ' || cast(round(cash_in_bdt + p2p_in_bdt) as varchar) || ', out '
           || cast(round(cash_out_bdt + p2p_out_bdt) as varchar)
from {{ ref('aml_customer_day') }}
where cash_in_bdt + p2p_in_bdt >= {{ var('pass_through_min_bdt') }}
  and cash_out_bdt + p2p_out_bdt >= {{ var('pass_through_share') }} * (cash_in_bdt + p2p_in_bdt)

union all

select customer_id, business_date, 'FAN_IN', p2p_in_bdt,
       cast(p2p_in_senders as varchar) || ' distinct senders'
from {{ ref('aml_customer_day') }}
where p2p_in_senders >= {{ var('fan_in_min_senders') }}
