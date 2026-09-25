with source as (
    {{ current_entity('logins') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by coalesce(client_event_id, login_id)
        order by recorded_at desc, login_id desc
    ) = 1
),

renamed as (
    select
        login_id,
        client_event_id,
        user_id,
        org_id,
        lower(idp) as idp,
        coalesce(success, false) as success,
        ip_country,
        logged_in_at,
        recorded_at,
        datediff('day', logged_in_at, recorded_at) >= 2 as is_late_arriving,
        _loaded_at,
        _batch_id,
        _source_system
    from deduped
)

select * from renamed
