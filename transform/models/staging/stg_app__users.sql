with source as (
    select * from {{ source('app', 'users') }}
),

renamed as (
    select
        user_id,
        org_id,
        email,
        full_name,
        lower(role) as role,
        lower(status) as status,
        created_at,
        updated_at,
        _loaded_at,
        _batch_id,
        _source_system
    from source
)

select * from renamed
