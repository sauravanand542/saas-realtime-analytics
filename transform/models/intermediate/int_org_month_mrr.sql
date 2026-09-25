-- Last known MRR for each organization at each month end.
-- The month spine is business time (occurred_at), not ingest time.
with recursive months as (
    select
        cast(date_trunc('month', min(occurred_at)) as date) as month_start,
        cast(date_trunc('month', max(occurred_at)) as date) as max_month
    from {{ ref('int_subscription_movements') }}

    union all

    select
        {{ add_months('month_start', 1) }} as month_start,
        max_month
    from months
    where month_start < max_month
),

month_dim as (
    select month_start
    from months
    where month_start is not null
),

movements as (
    select
        org_id,
        occurred_at,
        event_id,
        mrr_cents_after
    from {{ ref('int_subscription_movements') }}
),

org_asof as (
    select
        month_dim.month_start,
        movements.org_id,
        movements.mrr_cents_after,
        row_number() over (
            partition by month_dim.month_start, movements.org_id
            order by movements.occurred_at desc, movements.event_id desc
        ) as row_num
    from month_dim
    inner join movements
        on movements.occurred_at < cast({{ add_months('month_dim.month_start', 1) }} as timestamp)
),

org_end as (
    select
        month_start,
        org_id,
        mrr_cents_after
    from org_asof
    where row_num = 1
)

select
    org_id || '-' || cast(month_start as varchar) as org_month_key,
    month_start,
    org_id,
    mrr_cents_after,
    mrr_cents_after > 0 as is_paying
from org_end
