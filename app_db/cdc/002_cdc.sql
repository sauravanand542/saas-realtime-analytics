-- Logical replication objects for Debezium.
-- Applied by `make cdc-prepare` (app_db/cdc/002_cdc.sql). This file is not in
-- app_db/init, so a plain `make up` does not create the publication.
-- wal_level=logical is set on the base Postgres service in docker-compose.yml.
-- Safe to re-run on a fresh volume or an existing one.

do $$
begin
    if current_setting('wal_level') <> 'logical' then
        raise exception
            'wal_level is %. CDC needs logical. The Compose Postgres service sets wal_level=logical; a server started another way does not.',
            current_setting('wal_level');
    end if;
end
$$;

do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'replicator') then
        create role replicator with replication login password 'replicator';
    end if;
end
$$;

grant usage on schema app to replicator;
grant select on all tables in schema app to replicator;
alter default privileges in schema app grant select on tables to replicator;

do $$
begin
    if not exists (select 1 from pg_publication where pubname = 'app_publication') then
        create publication app_publication for table
            app.organizations,
            app.users,
            app.logins,
            app.subscriptions,
            app.subscription_events,
            app.invoices;
    end if;
end
$$;
