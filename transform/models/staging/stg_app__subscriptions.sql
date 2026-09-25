with source as (
    select * from {{ source('app', 'subscriptions') }}
),

renamed as (
    select
        subscription_id,
        org_id,
        lower(trim(plan)) as plan,
        lower(status) as status,
        -- Kept as recorded. A negative value is an application bug and must fail
        -- the upstream test rather than being coerced to zero.
        mrr_cents,
        seats,
        started_at,
        canceled_at,
        updated_at,
        _loaded_at,
        _batch_id,
        _source_system
    from source
)

select * from renamed
