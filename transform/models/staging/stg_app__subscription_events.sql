with source as (
    {{ current_entity('subscription_events') }}
),

renamed as (
    select
        event_id,
        subscription_id,
        org_id,
        lower(event_type) as event_type,
        lower(from_plan) as from_plan,
        lower(to_plan) as to_plan,
        mrr_cents_before,
        mrr_cents_after,
        occurred_at,
        recorded_at,
        datediff('day', occurred_at, recorded_at) >= 2 as is_late_arriving,
        _loaded_at,
        _batch_id,
        _source_system
    from source
)

select * from renamed
