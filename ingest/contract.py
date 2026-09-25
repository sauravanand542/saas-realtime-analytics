"""Column contract for the raw landing tables.

Batch ingest writes current rows to `raw.<table>` with `_source_system =
postgres_batch`. The CDC consumer writes one row per change to
`raw.<table>_cdc` with `_source_system = debezium` and the same business
columns. Staging picks the latest version per key. Extra JSON fields stay in
`_extra` until the contract and the staging select learn them.
"""

from __future__ import annotations

SOURCE_SYSTEM = "postgres_batch"

METADATA_COLUMNS = [
    ("_loaded_at", "timestamp"),
    ("_batch_id", "varchar"),
    ("_source_system", "varchar"),
]

# mode "full" reloads current state. mode "upsert" walks a watermark and
# deletes+inserts the primary key so a replay of the lookback window is safe.
TABLES: dict[str, dict] = {
    "organizations": {
        "pk": "org_id",
        "mode": "full",
        "watermark_column": "updated_at",
        "columns": [
            ("org_id", "varchar"),
            ("name", "varchar"),
            ("domain", "varchar"),
            ("industry", "varchar"),
            ("employee_band", "varchar"),
            ("country_code", "varchar"),
            ("status", "varchar"),
            ("created_at", "timestamp"),
            ("updated_at", "timestamp"),
        ],
    },
    "users": {
        "pk": "user_id",
        "mode": "full",
        "watermark_column": "updated_at",
        "columns": [
            ("user_id", "varchar"),
            ("org_id", "varchar"),
            ("email", "varchar"),
            ("full_name", "varchar"),
            ("role", "varchar"),
            ("status", "varchar"),
            ("created_at", "timestamp"),
            ("updated_at", "timestamp"),
        ],
    },
    "subscriptions": {
        "pk": "subscription_id",
        "mode": "full",
        "watermark_column": "updated_at",
        "columns": [
            ("subscription_id", "varchar"),
            ("org_id", "varchar"),
            ("plan", "varchar"),
            ("status", "varchar"),
            ("mrr_cents", "bigint"),
            ("seats", "bigint"),
            ("started_at", "timestamp"),
            ("canceled_at", "timestamp"),
            ("updated_at", "timestamp"),
        ],
    },
    "logins": {
        "pk": "login_id",
        "mode": "upsert",
        "watermark_column": "recorded_at",
        "columns": [
            ("login_id", "varchar"),
            ("client_event_id", "varchar"),
            ("user_id", "varchar"),
            ("org_id", "varchar"),
            ("idp", "varchar"),
            ("success", "boolean"),
            ("ip_country", "varchar"),
            ("logged_in_at", "timestamp"),
            ("recorded_at", "timestamp"),
        ],
    },
    "subscription_events": {
        "pk": "event_id",
        "mode": "upsert",
        "watermark_column": "recorded_at",
        "columns": [
            ("event_id", "varchar"),
            ("subscription_id", "varchar"),
            ("org_id", "varchar"),
            ("event_type", "varchar"),
            ("from_plan", "varchar"),
            ("to_plan", "varchar"),
            ("mrr_cents_before", "bigint"),
            ("mrr_cents_after", "bigint"),
            ("occurred_at", "timestamp"),
            ("recorded_at", "timestamp"),
        ],
    },
    "invoices": {
        "pk": "invoice_id",
        "mode": "upsert",
        "watermark_column": "recorded_at",
        "columns": [
            ("invoice_id", "varchar"),
            ("org_id", "varchar"),
            ("subscription_id", "varchar"),
            ("amount_cents", "bigint"),
            ("currency", "varchar"),
            ("status", "varchar"),
            ("period_start", "date"),
            ("period_end", "date"),
            ("issued_at", "timestamp"),
            ("paid_at", "timestamp"),
            ("recorded_at", "timestamp"),
        ],
    },
}


def warehouse_columns(table: str) -> list[tuple[str, str]]:
    return list(TABLES[table]["columns"]) + list(METADATA_COLUMNS)


def create_table_sql(table: str) -> str:
    spec = TABLES[table]
    columns = ",\n        ".join(f"{name} {typ}" for name, typ in warehouse_columns(table))
    return (
        f"create table if not exists raw.{table} (\n"
        f"        {columns},\n"
        f"        primary key ({spec['pk']})\n"
        f")"
    )
