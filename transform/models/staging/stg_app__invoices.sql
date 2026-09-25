with source as (
    {{ current_entity('invoices') }}
),

renamed as (
    select
        invoice_id,
        org_id,
        subscription_id,
        amount_cents,
        upper(currency) as currency,
        lower(status) as status,
        period_start,
        period_end,
        issued_at,
        paid_at,
        recorded_at,
        _loaded_at,
        _batch_id,
        _source_system
    from source
)

select * from renamed
