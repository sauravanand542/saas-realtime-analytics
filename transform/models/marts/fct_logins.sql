{% set login_strategy = 'merge' if target.type == 'snowflake' else 'delete+insert' %}

{{
    config(
        materialized='incremental',
        unique_key='login_id',
        incremental_strategy=login_strategy,
        on_schema_change='append_new_columns',
    )
}}

-- Incremental on warehouse load time, not logged_in_at.
-- A login that happened last week but was recorded today still has a new
-- _loaded_at, so it is picked up. The daily aggregate is rebuilt from this
-- fact, which restates the old day.
with logins as (
    select src.*
    from {{ ref('stg_app__logins') }} as src
    {% if is_incremental() %}
        where src._loaded_at > (
            select coalesce(max(existing._loaded_at), cast('1970-01-01' as timestamp))
            from {{ this }} as existing
        )
    {% endif %}
),

known_users as (
    select user_id
    from {{ ref('stg_app__users') }}
)

select
    logins.login_id,
    logins.client_event_id,
    logins.user_id,
    logins.org_id,
    logins.idp,
    logins.success,
    logins.ip_country,
    logins.logged_in_at,
    logins.recorded_at,
    logins.is_late_arriving,
    known_users.user_id is not null as is_known_user,
    logins._loaded_at,
    {{ dbt.current_timestamp() }} as _dbt_built_at
from logins
left join known_users
    on logins.user_id = known_users.user_id
