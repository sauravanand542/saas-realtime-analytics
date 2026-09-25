select
    event_id,
    subscription_id,
    org_id,
    event_type,
    movement_type,
    from_plan,
    to_plan,
    mrr_cents_before,
    mrr_cents_after,
    mrr_delta_cents,
    occurred_at,
    recorded_at,
    occurred_month,
    is_late_arriving,
    {{ dbt.current_timestamp() }} as _dbt_built_at
from {{ ref('int_subscription_movements') }}
