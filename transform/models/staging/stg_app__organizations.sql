with source as (
    {{ current_entity('organizations') }}
),

renamed as (
    select
        org_id,
        name as org_name,
        domain,
        industry,
        employee_band,
        country_code,
        lower(status) as status,
        created_at,
        updated_at,
        _loaded_at,
        _batch_id,
        _source_system
    from source
)

select * from renamed
