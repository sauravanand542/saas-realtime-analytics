-- Logo churn uses the orgs that were paying at the start of the month as the
-- denominator, and only those orgs in the numerator. The rate is therefore
-- null (no starting orgs) or between 0 and 1 inclusive.
select
    month_start,
    active_orgs_start,
    churned_orgs,
    logo_churn_rate
from {{ ref('fct_mrr_monthly') }}
where
    logo_churn_rate < 0
    or logo_churn_rate > 1
    or (active_orgs_start = 0 and logo_churn_rate is not null)
    or (churned_orgs > active_orgs_start)
