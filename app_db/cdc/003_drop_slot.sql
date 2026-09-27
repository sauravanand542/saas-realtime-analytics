-- Drop the Debezium slot so Postgres can recycle WAL past it.
-- Applied by `make cdc-reset`, and by `make cdc-down DROP_SLOT=1`.
-- Safe to re-run. Refuses to drop a slot Connect is still using.

do $$
declare
    slot_active boolean;
begin
    select active into slot_active
    from pg_replication_slots
    where slot_name = 'saas_app_slot';

    if not found then
        raise notice 'replication slot saas_app_slot is not present';
        return;
    end if;

    if slot_active then
        raise exception
            'replication slot saas_app_slot is still active. Stop Connect with make cdc-down, then retry.';
    end if;

    perform pg_drop_replication_slot('saas_app_slot');
end
$$;
