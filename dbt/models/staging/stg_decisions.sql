-- Inline decisions, one row per decision (the log is at-least-once). Model fields are null for
-- decisions made before a champion was deployed.
select distinct
    decision_id, txn_id, decided_at, cast(decided_at as date) as decision_date, outcome, reasons,
    risk_score, degraded, latency_us, txn_type, sender_id, amount_paisa,
    reasons like '%MODEL_SCORE%' as held_by_model,
    model_version, model_score, challenger_version, challenger_score
from {{ source('silver', 'decisions') }}
