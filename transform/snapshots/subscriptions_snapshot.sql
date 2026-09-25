{% snapshot subscriptions_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='subscription_id',
        strategy='check',
        check_cols=['plan', 'status', 'mrr_cents', 'seats', 'canceled_at'],
        hard_deletes='ignore',
    )
}}

-- Check strategy, not timestamp. updated_at moves when the app touches the row,
-- including touches that do not change the commercial columns. We only want a
-- new SCD2 version when plan, status, MRR, seats, or canceled_at change.
-- Snapshotted from staging so plan casing is already normalized. A raw casing
-- glitch would otherwise look like a plan change.
    select
        subscription_id,
        org_id,
        plan,
        status,
        mrr_cents,
        seats,
        started_at,
        canceled_at,
        updated_at
    from {{ ref('stg_app__subscriptions') }}

{% endsnapshot %}
