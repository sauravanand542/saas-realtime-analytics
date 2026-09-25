-- Rebuilt in full from fct_logins so a late-arriving login restates its day.
-- The fact is incremental; this aggregate is not.
select
    cast(date_trunc('day', logged_in_at) as date) as activity_date,
    org_id,
    count(*) as login_count,
    sum(case when success then 1 else 0 end) as successful_login_count,
    count(distinct user_id) as distinct_users,
    {{ dbt.current_timestamp() }} as _dbt_built_at
from {{ ref('fct_logins') }}
group by
    cast(date_trunc('day', logged_in_at) as date),
    org_id
