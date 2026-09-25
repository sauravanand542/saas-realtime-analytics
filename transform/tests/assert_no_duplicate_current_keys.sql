-- Staging must expose one current row per business key after the batch/CDC merge.
select
    org_id as entity_key,
    'organizations' as entity
from {{ ref('stg_app__organizations') }}
group by org_id
having count(*) > 1

union all

select
    user_id,
    'users'
from {{ ref('stg_app__users') }}
group by user_id
having count(*) > 1

union all

select
    subscription_id,
    'subscriptions'
from {{ ref('stg_app__subscriptions') }}
group by subscription_id
having count(*) > 1

union all

select
    login_id,
    'logins'
from {{ ref('stg_app__logins') }}
group by login_id
having count(*) > 1

union all

select
    event_id,
    'subscription_events'
from {{ ref('stg_app__subscription_events') }}
group by event_id
having count(*) > 1

union all

select
    invoice_id,
    'invoices'
from {{ ref('stg_app__invoices') }}
group by invoice_id
having count(*) > 1
