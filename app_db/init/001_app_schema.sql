-- Operational schema for the simulated B2B SaaS application.
-- This is the "production app database", not the warehouse.
-- There is intentionally no CHECK (mrr_cents >= 0). A bad application write
-- has to be caught by the warehouse tests. See scripts/break_it.py.

create schema if not exists app;

create table if not exists app.organizations (
    org_id uuid primary key,
    name text not null,
    domain text,
    industry text,
    employee_band text,
    country_code char(2),
    status text not null,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create table if not exists app.users (
    user_id uuid primary key,
    org_id uuid not null references app.organizations (org_id),
    email text,
    full_name text,
    role text not null,
    status text not null,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

-- user_id is not a foreign key. The generator occasionally writes a login
-- whose user does not exist yet, which is the late-arriving dimension case.
create table if not exists app.logins (
    login_id uuid primary key,
    client_event_id text,
    user_id uuid,
    org_id uuid not null references app.organizations (org_id),
    idp text,
    success boolean not null,
    ip_country char(2),
    logged_in_at timestamptz not null,
    recorded_at timestamptz not null
);

create table if not exists app.subscriptions (
    subscription_id uuid primary key,
    org_id uuid not null unique references app.organizations (org_id),
    plan text not null,
    status text not null,
    mrr_cents integer not null,
    seats integer not null,
    started_at timestamptz not null,
    canceled_at timestamptz,
    updated_at timestamptz not null
);

create table if not exists app.subscription_events (
    event_id uuid primary key,
    subscription_id uuid not null references app.subscriptions (subscription_id),
    org_id uuid not null references app.organizations (org_id),
    event_type text not null,
    from_plan text,
    to_plan text,
    mrr_cents_before integer,
    mrr_cents_after integer,
    occurred_at timestamptz not null,
    recorded_at timestamptz not null
);

create table if not exists app.invoices (
    invoice_id uuid primary key,
    org_id uuid not null references app.organizations (org_id),
    subscription_id uuid references app.subscriptions (subscription_id),
    amount_cents integer not null,
    currency char(3) not null,
    status text not null,
    period_start date not null,
    period_end date not null,
    issued_at timestamptz not null,
    paid_at timestamptz,
    recorded_at timestamptz not null
);

create index if not exists organizations_updated_at_idx
    on app.organizations (updated_at);
create index if not exists users_updated_at_idx
    on app.users (updated_at);
create index if not exists subscriptions_updated_at_idx
    on app.subscriptions (updated_at);
create index if not exists logins_recorded_at_idx
    on app.logins (recorded_at);
create index if not exists subscription_events_recorded_at_idx
    on app.subscription_events (recorded_at);
create index if not exists invoices_recorded_at_idx
    on app.invoices (recorded_at);
