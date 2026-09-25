select
    invoice_id,
    org_id,
    subscription_id,
    amount_cents,
    currency,
    status,
    period_start,
    period_end,
    issued_at,
    paid_at,
    recorded_at,
    {{ dbt.current_timestamp() }} as _dbt_built_at
from {{ ref('stg_app__invoices') }}
