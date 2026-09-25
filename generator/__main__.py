"""CLI for the simulated application database.

Examples:
  python -m generator --mode seed --seed 42
  python -m generator --mode tick
  python -m generator --mode tick --seed 7
"""

from __future__ import annotations

import argparse
import logging
import random
from datetime import timedelta
from random import Random

import psycopg

from generator.config import PLAN_MRR
from generator.db import connect, executemany, fetch_all, truncate_app
from generator.simulate import (
    INSERT_EVENTS,
    INSERT_INVOICES,
    INSERT_LOGINS,
    INSERT_ORGANIZATIONS,
    INSERT_SUBSCRIPTIONS,
    INSERT_USERS,
    build_organization,
    build_users,
    build_world,
    deterministic_uuid,
    faker_for,
    make_login,
    utcnow,
)

logger = logging.getLogger("generator")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate the SaaS application database")
    parser.add_argument("--mode", choices=["seed", "tick"], required=True)
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Deterministic seed. Seed mode defaults to 42 when omitted.",
    )
    parser.add_argument("--orgs", type=int, default=40)
    parser.add_argument("--history-days", type=int, default=540)
    parser.add_argument("--login-days", type=int, default=40)
    return parser.parse_args()


def _insert_world(conn: psycopg.Connection, world: dict[str, list[dict]]) -> None:
    executemany(conn, INSERT_ORGANIZATIONS, world["organizations"])
    executemany(conn, INSERT_USERS, world["users"])
    executemany(conn, INSERT_SUBSCRIPTIONS, world["subscriptions"])
    executemany(conn, INSERT_EVENTS, world["subscription_events"])
    executemany(conn, INSERT_INVOICES, world["invoices"])
    executemany(conn, INSERT_LOGINS, world["logins"])


def _log_counts(conn: psycopg.Connection) -> None:
    rows = fetch_all(
        conn,
        """
        select 'organizations' as entity, count(*) as n from app.organizations
        union all select 'users', count(*) from app.users
        union all select 'subscriptions', count(*) from app.subscriptions
        union all select 'subscription_events', count(*) from app.subscription_events
        union all select 'invoices', count(*) from app.invoices
        union all select 'logins', count(*) from app.logins
        order by 1
        """,
    )
    summary = ", ".join(f"{row['entity']}={row['n']}" for row in rows)
    logger.info("app row counts: %s", summary)


def run_seed(
    conn: psycopg.Connection,
    seed: int,
    orgs: int,
    history_days: int,
    login_days: int,
) -> None:
    logger.info(
        "Seeding %s organizations with seed %s. This truncates the app tables.",
        orgs,
        seed,
    )
    rng = Random(seed)
    fake = faker_for(seed)
    world = build_world(
        rng,
        fake,
        orgs=orgs,
        history_days=history_days,
        login_days=login_days,
    )
    with conn.transaction():
        truncate_app(conn)
        _insert_world(conn, world)
    _log_counts(conn)


def _org_count(conn: psycopg.Connection) -> int:
    rows = fetch_all(conn, "select count(*) as n from app.organizations")
    return int(rows[0]["n"])


def run_tick(
    conn: psycopg.Connection,
    seed: int | None,
    orgs: int,
    history_days: int,
    login_days: int,
) -> None:
    if _org_count(conn) == 0:
        bootstrap = 42 if seed is None else seed
        logger.info("Application database is empty. Bootstrapping with seed %s.", bootstrap)
        run_seed(conn, bootstrap, orgs, history_days, login_days)
        return

    if seed is None:
        seed = random.SystemRandom().randint(1, 1_000_000_000)
        logger.info("Tick using a random seed (%s). Pass --seed to reproduce this tick.", seed)
    else:
        logger.info("Tick using seed %s.", seed)

    rng = Random(seed)
    fake = faker_for(seed)
    now = utcnow()
    _apply_tick(conn, rng, fake, now)
    _log_counts(conn)


def _apply_tick(conn: psycopg.Connection, rng: Random, fake, now) -> None:
    # The emptiness check above starts a transaction. Roll it back so the write
    # block below is the outer transaction and actually commits.
    conn.rollback()
    with conn.transaction():
        _apply_tick_in_transaction(conn, rng, fake, now)


def _apply_tick_in_transaction(conn: psycopg.Connection, rng: Random, fake, now) -> None:
    new_orgs = []
    new_users = []
    new_subs = []
    new_events = []
    new_invoices = []
    new_logins = []

    for _ in range(rng.randint(0, 2)):
        built = build_organization(
            rng,
            fake,
            now,
            history_days=1,
            login_days=1,
            with_history=False,
            born_at=now - timedelta(hours=rng.randint(1, 10)),
        )
        new_orgs.append(built["organization"])
        new_users.extend(built["users"])
        new_subs.append(built["subscription"])
        new_events.extend(built["events"])
        new_invoices.extend(built["invoices"])
        new_logins.extend(built["logins"])

    organizations = fetch_all(
        conn,
        """
        select org_id, domain, created_at
        from app.organizations
        order by org_id
        """,
    )
    for org in organizations:
        if rng.random() > 0.12:
            continue
        new_users.extend(
            build_users(
                rng,
                fake,
                org["org_id"],
                org["domain"] or "joined.example",
                now - timedelta(hours=1),
                now,
                count=1,
                include_owner=False,
            )
        )

    subscriptions = fetch_all(
        conn,
        """
        select subscription_id, org_id, plan, status, mrr_cents, seats, started_at, canceled_at
        from app.subscriptions
        order by subscription_id
        """,
    )
    subscription_updates = []
    for sub in subscriptions:
        # Leave a deliberately broken row alone so the failure demo stays broken.
        if sub["mrr_cents"] < 0:
            continue
        if rng.random() > 0.08:
            continue
        change = _plan_change(rng, sub, now)
        if change is not None:
            new_events.append(change["event"])
            subscription_updates.append(change)

    users = fetch_all(
        conn,
        """
        select user_id, org_id, status
        from app.users
        where status = 'active'
        order by user_id
        """,
    )
    for user in users:
        if rng.random() > 0.35:
            continue
        when = now - timedelta(minutes=rng.randint(1, 180))
        late = rng.random() < 0.1
        login_when = when - timedelta(days=rng.randint(1, 4)) if late else when
        new_logins.append(
            make_login(
                rng,
                user["org_id"],
                user["user_id"],
                login_when,
                now,
                late_probability=1.0 if late else 0.0,
            )
        )

    if new_logins:
        original = rng.choice(new_logins)
        duplicate = dict(original)
        duplicate["login_id"] = deterministic_uuid(rng)
        duplicate["recorded_at"] = now
        new_logins.append(duplicate)
        org_id = rng.choice(subscriptions)["org_id"] if subscriptions else new_orgs[0]["org_id"]
        new_logins.append(
            make_login(
                rng,
                org_id,
                deterministic_uuid(rng),
                now - timedelta(hours=rng.randint(1, 12)),
                now,
            )
        )

    executemany(conn, INSERT_ORGANIZATIONS, new_orgs)
    executemany(conn, INSERT_USERS, new_users)
    executemany(conn, INSERT_SUBSCRIPTIONS, new_subs)
    executemany(conn, INSERT_EVENTS, new_events)
    executemany(conn, INSERT_INVOICES, new_invoices)
    executemany(conn, INSERT_LOGINS, new_logins)
    _apply_subscription_updates(conn, subscription_updates, now)
    _refresh_open_invoices(conn, rng, now)


PLAN_DOWN = {"enterprise": "growth", "growth": "starter", "starter": "trial"}
PLAN_UP = {"trial": "starter", "starter": "growth", "growth": "enterprise"}


def _plan_change(rng: Random, sub: dict, now) -> dict | None:
    plan = str(sub["plan"]).lower()
    status = sub["status"]
    mrr = int(sub["mrr_cents"])
    if plan not in PLAN_MRR:
        return None

    new_plan = plan
    new_status = status
    canceled_at = sub["canceled_at"]
    event_type = None
    before, after = mrr, mrr

    if status == "canceled":
        new_plan = rng.choice(["starter", "growth"])
        event_type = "reactivated"
        before, after = 0, PLAN_MRR[new_plan]
        new_status = "active"
        canceled_at = None
    elif status == "trialing":
        new_plan = "starter"
        event_type = "upgraded"
        before, after = mrr, PLAN_MRR[new_plan]
        new_status = "active"
        canceled_at = None
    else:
        roll = rng.random()
        if roll < 0.4 and plan in PLAN_UP:
            new_plan = PLAN_UP[plan]
            event_type = "upgraded"
            before, after = mrr, PLAN_MRR[new_plan]
            new_status = "active"
            canceled_at = None
        elif roll < 0.6 and plan in PLAN_DOWN and PLAN_DOWN[plan] != "trial":
            new_plan = PLAN_DOWN[plan]
            event_type = "downgraded"
            before, after = mrr, PLAN_MRR[new_plan]
            new_status = "active"
            canceled_at = None
        elif roll < 0.85:
            event_type = "canceled"
            before, after = mrr, 0
            new_status = "canceled"
            canceled_at = now
        else:
            return None

    if event_type is None:
        return None

    return {
        "event": {
            "event_id": deterministic_uuid(rng),
            "subscription_id": sub["subscription_id"],
            "org_id": sub["org_id"],
            "event_type": event_type,
            "from_plan": plan,
            "to_plan": new_plan,
            "mrr_cents_before": before,
            "mrr_cents_after": after,
            "occurred_at": now - timedelta(minutes=rng.randint(1, 30)),
            "recorded_at": now,
        },
        "subscription_id": sub["subscription_id"],
        "org_id": sub["org_id"],
        "plan": new_plan,
        "status": new_status,
        "mrr_cents": after,
        "canceled_at": canceled_at,
        "org_status": "churned" if new_status == "canceled" else "active",
    }


def _apply_subscription_updates(conn: psycopg.Connection, updates: list[dict], now) -> None:
    with conn.cursor() as cur:
        for change in updates:
            cur.execute(
                """
                update app.subscriptions
                set plan = %s, status = %s, mrr_cents = %s, canceled_at = %s, updated_at = %s
                where subscription_id = %s
                """,
                (
                    change["plan"],
                    change["status"],
                    change["mrr_cents"],
                    change["canceled_at"],
                    now,
                    change["subscription_id"],
                ),
            )
            cur.execute(
                """
                update app.organizations
                set status = %s, updated_at = %s
                where org_id = %s
                """,
                (change["org_status"], now, change["org_id"]),
            )


def _refresh_open_invoices(conn: psycopg.Connection, rng: Random, now) -> None:
    open_invoices = fetch_all(
        conn,
        """
        select invoice_id
        from app.invoices
        where status = 'open'
        order by issued_at
        limit 3
        """,
    )
    if open_invoices and rng.random() < 0.7:
        chosen = open_invoices[0]["invoice_id"]
        with conn.cursor() as cur:
            cur.execute(
                """
                update app.invoices
                set status = 'paid', paid_at = %s, recorded_at = %s
                where invoice_id = %s
                """,
                (now, now, chosen),
            )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = _parse_args()
    conn = connect()
    try:
        if args.mode == "seed":
            run_seed(
                conn,
                42 if args.seed is None else args.seed,
                args.orgs,
                args.history_days,
                args.login_days,
            )
        else:
            run_tick(conn, args.seed, args.orgs, args.history_days, args.login_days)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
