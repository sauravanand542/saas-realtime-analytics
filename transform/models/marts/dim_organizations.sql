with user_counts as (
    select
        org_id,
        count(*) as user_count,
        sum(case when status = 'active' then 1 else 0 end) as active_user_count
    from {{ ref('stg_app__users') }}
    group by org_id
),

last_login as (
    select
        org_id,
        max(case when success then logged_in_at end) as last_successful_login_at
    from {{ ref('stg_app__logins') }}
    group by org_id
)

select
    orgs.org_id,
    orgs.org_name,
    orgs.domain,
    orgs.industry,
    orgs.employee_band,
    orgs.country_code,
    orgs.status as org_status,
    orgs.created_at,
    subs.subscription_id,
    subs.plan,
    subs.status as subscription_status,
    subs.mrr_cents,
    subs.seats,
    subs.started_at as subscription_started_at,
    subs.canceled_at,
    coalesce(user_counts.user_count, 0) as user_count,
    coalesce(user_counts.active_user_count, 0) as active_user_count,
    last_login.last_successful_login_at,
    coalesce(
        orgs.status = 'active'
        and subs.status in ('trialing', 'active', 'past_due'),
        false
    ) as is_active,
    coalesce(
        subs.mrr_cents > 0
        and subs.status in ('active', 'past_due'),
        false
    ) as is_paying,
    {{ dbt.current_timestamp() }} as _dbt_built_at
from {{ ref('stg_app__organizations') }} as orgs
left join {{ ref('stg_app__subscriptions') }} as subs
    on orgs.org_id = subs.org_id
left join user_counts
    on orgs.org_id = user_counts.org_id
left join last_login
    on orgs.org_id = last_login.org_id
