-- Company-level MRR bridge.
-- ending_mrr_cents is the sum of each org's last known MRR at month end (state).
-- The component columns are the sum of event deltas in that month (flow).
-- assert_mrr_bridge_balances checks that the two methods agree.
-- Contraction and churn are stored as negative cent amounts.
with ending as (
    select
        month_start,
        sum(mrr_cents_after) as ending_mrr_cents,
        sum(case when is_paying then 1 else 0 end) as active_orgs_end
    from {{ ref('int_org_month_mrr') }}
    group by month_start
),

flows as (
    select
        occurred_month as month_start,
        sum(case when movement_type = 'new' then mrr_delta_cents else 0 end) as new_mrr_cents,
        sum(
            case when movement_type = 'expansion' then mrr_delta_cents else 0 end
        ) as expansion_mrr_cents,
        sum(
            case when movement_type = 'contraction' then mrr_delta_cents else 0 end
        ) as contraction_mrr_cents,
        sum(case when movement_type = 'churn' then mrr_delta_cents else 0 end) as churn_mrr_cents,
        sum(
            case when movement_type = 'reactivation' then mrr_delta_cents else 0 end
        ) as reactivation_mrr_cents,
        count(distinct case when movement_type = 'new' then org_id end) as new_orgs
    from {{ ref('int_subscription_movements') }}
    group by occurred_month
),

starting_churn as (
    select
        movements.occurred_month as month_start,
        count(distinct movements.org_id) as churned_orgs
    from {{ ref('int_subscription_movements') }} as movements
    inner join {{ ref('int_org_month_mrr') }} as previous_month
        on
            movements.org_id = previous_month.org_id
            and {{ add_months('movements.occurred_month', -1) }} = previous_month.month_start
            and previous_month.is_paying
    where movements.movement_type = 'churn'
    group by movements.occurred_month
),

bridged as (
    select
        ending.month_start,
        coalesce(
            lag(ending.ending_mrr_cents) over (order by ending.month_start),
            0
        ) as beginning_mrr_cents,
        coalesce(flows.new_mrr_cents, 0) as new_mrr_cents,
        coalesce(flows.expansion_mrr_cents, 0) as expansion_mrr_cents,
        coalesce(flows.contraction_mrr_cents, 0) as contraction_mrr_cents,
        coalesce(flows.churn_mrr_cents, 0) as churn_mrr_cents,
        coalesce(flows.reactivation_mrr_cents, 0) as reactivation_mrr_cents,
        ending.ending_mrr_cents,
        coalesce(
            lag(ending.active_orgs_end) over (order by ending.month_start),
            0
        ) as active_orgs_start,
        ending.active_orgs_end,
        coalesce(flows.new_orgs, 0) as new_orgs,
        coalesce(starting_churn.churned_orgs, 0) as churned_orgs
    from ending
    left join flows
        on ending.month_start = flows.month_start
    left join starting_churn
        on ending.month_start = starting_churn.month_start
)

select
    month_start,
    beginning_mrr_cents,
    new_mrr_cents,
    expansion_mrr_cents,
    contraction_mrr_cents,
    churn_mrr_cents,
    reactivation_mrr_cents,
    ending_mrr_cents,
    active_orgs_start,
    active_orgs_end,
    new_orgs,
    churned_orgs,
    case
        when active_orgs_start = 0 then null
        else cast(churned_orgs as double) / cast(active_orgs_start as double)
    end as logo_churn_rate,
    {{ dbt.current_timestamp() }} as _dbt_built_at
from bridged
