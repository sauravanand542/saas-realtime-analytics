with events as (
    select
        event_id,
        subscription_id,
        org_id,
        event_type,
        from_plan,
        to_plan,
        coalesce(mrr_cents_before, 0) as mrr_cents_before,
        coalesce(mrr_cents_after, 0) as mrr_cents_after,
        occurred_at,
        recorded_at,
        is_late_arriving,
        cast(date_trunc('month', occurred_at) as date) as occurred_month
    from {{ ref('stg_app__subscription_events') }}
),

classified as (
    select
        event_id,
        subscription_id,
        org_id,
        event_type,
        from_plan,
        to_plan,
        mrr_cents_before,
        mrr_cents_after,
        mrr_cents_after - mrr_cents_before as mrr_delta_cents,
        occurred_at,
        recorded_at,
        occurred_month,
        is_late_arriving,
        case
            when
                mrr_cents_before = 0
                and mrr_cents_after > 0
                and event_type = 'reactivated'
                then 'reactivation'
            when
                mrr_cents_before = 0
                and mrr_cents_after > 0
                then 'new'
            when
                mrr_cents_after > mrr_cents_before
                and mrr_cents_before > 0
                then 'expansion'
            when
                mrr_cents_after < mrr_cents_before
                and mrr_cents_after > 0
                then 'contraction'
            when
                mrr_cents_before > 0
                and mrr_cents_after = 0
                then 'churn'
            else 'other'
        end as movement_type
    from events
)

select * from classified
