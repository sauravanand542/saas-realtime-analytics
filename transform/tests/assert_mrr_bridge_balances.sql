-- State (ending MRR) must equal the previous ending MRR plus this month's flows.
-- Churn and contraction are negative, so the identity is a sum.
select
    month_start,
    beginning_mrr_cents,
    new_mrr_cents,
    expansion_mrr_cents,
    contraction_mrr_cents,
    churn_mrr_cents,
    reactivation_mrr_cents,
    ending_mrr_cents
from {{ ref('fct_mrr_monthly') }}
where ending_mrr_cents != (
    beginning_mrr_cents
    + new_mrr_cents
    + expansion_mrr_cents
    + reactivation_mrr_cents
    + contraction_mrr_cents
    + churn_mrr_cents
)
