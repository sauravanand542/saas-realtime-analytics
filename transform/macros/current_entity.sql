{% macro current_entity(table) %}
    {%- set entities = {
        'organizations': {
            'pk': 'org_id',
            'columns': [
                'org_id', 'name', 'domain', 'industry', 'employee_band',
                'country_code', 'status', 'created_at', 'updated_at',
            ],
        },
        'users': {
            'pk': 'user_id',
            'columns': [
                'user_id', 'org_id', 'email', 'full_name', 'role', 'status',
                'created_at', 'updated_at',
            ],
        },
        'subscriptions': {
            'pk': 'subscription_id',
            'columns': [
                'subscription_id', 'org_id', 'plan', 'status', 'mrr_cents',
                'seats', 'started_at', 'canceled_at', 'updated_at',
            ],
        },
        'logins': {
            'pk': 'login_id',
            'columns': [
                'login_id', 'client_event_id', 'user_id', 'org_id', 'idp',
                'success', 'ip_country', 'logged_in_at', 'recorded_at',
            ],
        },
        'subscription_events': {
            'pk': 'event_id',
            'columns': [
                'event_id', 'subscription_id', 'org_id', 'event_type',
                'from_plan', 'to_plan', 'mrr_cents_before', 'mrr_cents_after',
                'occurred_at', 'recorded_at',
            ],
        },
        'invoices': {
            'pk': 'invoice_id',
            'columns': [
                'invoice_id', 'org_id', 'subscription_id', 'amount_cents',
                'currency', 'status', 'period_start', 'period_end',
                'issued_at', 'paid_at', 'recorded_at',
            ],
        },
    } -%}
    {%- set pk = entities[table]['pk'] -%}
    {%- set columns = entities[table]['columns'] -%}
with batch_rows as (
    select
    {%- for column in columns %}
        {{ column }},
    {%- endfor %}
        _loaded_at,
        _batch_id,
        _source_system,
        cast(null as varchar) as _op,
        cast(null as bigint) as _source_lsn,
        cast(null as bigint) as _kafka_offset,
        cast(null as timestamp) as _source_ts,
        false as _deleted,
        _loaded_at as _version_ts,
        0 as _source_rank
    from {{ source('app', table) }}
),

cdc_ranked as (
    select
    {%- for column in columns %}
        {{ column }},
    {%- endfor %}
        _loaded_at,
        _batch_id,
        _source_system,
        _op,
        _source_lsn,
        _kafka_offset,
        _source_ts,
        coalesce(_deleted, false) as _deleted,
        coalesce(_source_ts, _loaded_at) as _version_ts,
        1 as _source_rank,
        row_number() over (
            partition by {{ pk }}
            order by _kafka_offset desc nulls last, _source_lsn desc nulls last
        ) as _cdc_rank
    from {{ source('app_cdc', table ~ '_cdc') }}
),

latest_cdc as (
    select
    {%- for column in columns %}
        {{ column }},
    {%- endfor %}
        _loaded_at,
        _batch_id,
        _source_system,
        _op,
        _source_lsn,
        _kafka_offset,
        _source_ts,
        _deleted,
        _version_ts,
        _source_rank
    from cdc_ranked
    where _cdc_rank = 1
),

combined as (
    select
    {%- for column in columns %}
        {{ column }},
    {%- endfor %}
        _loaded_at,
        _batch_id,
        _source_system,
        _op,
        _source_lsn,
        _kafka_offset,
        _source_ts,
        _deleted,
        _version_ts,
        _source_rank
    from latest_cdc
    union all
    select
    {%- for column in columns %}
        {{ column }},
    {%- endfor %}
        _loaded_at,
        _batch_id,
        _source_system,
        _op,
        _source_lsn,
        _kafka_offset,
        _source_ts,
        _deleted,
        _version_ts,
        _source_rank
    from batch_rows
),

picked as (
    select
    {%- for column in columns %}
        {{ column }},
    {%- endfor %}
        _loaded_at,
        _batch_id,
        _source_system,
        _deleted,
        row_number() over (
            partition by {{ pk }}
            order by _version_ts desc nulls last, _source_rank desc
        ) as _pick
    from combined
)

select
    {%- for column in columns %}
    {{ column }},
    {%- endfor %}
    _loaded_at,
    _batch_id,
    _source_system
from picked
where
    _pick = 1
    and not _deleted
{% endmacro %}
