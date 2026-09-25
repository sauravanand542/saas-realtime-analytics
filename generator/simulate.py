"""Build realistic organization histories and small ongoing mutations.

The seed path is deterministic for a given --seed. A tick is deterministic
only when both the seed and the current database contents are the same.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from random import Random
from uuid import UUID

from faker import Faker

from generator.config import (
    COUNTRIES,
    EMPLOYEE_BANDS,
    IDPS,
    INDUSTRIES,
    PLAN_MRR,
    PLAN_ORDER,
    SEAT_RANGES,
)

INSERT_ORGANIZATIONS = """
insert into app.organizations (
    org_id, name, domain, industry, employee_band, country_code,
    status, created_at, updated_at
) values (
    %(org_id)s, %(name)s, %(domain)s, %(industry)s, %(employee_band)s,
    %(country_code)s, %(status)s, %(created_at)s, %(updated_at)s
)
"""

INSERT_USERS = """
insert into app.users (
    user_id, org_id, email, full_name, role, status, created_at, updated_at
) values (
    %(user_id)s, %(org_id)s, %(email)s, %(full_name)s, %(role)s, %(status)s,
    %(created_at)s, %(updated_at)s
)
"""

INSERT_SUBSCRIPTIONS = """
insert into app.subscriptions (
    subscription_id, org_id, plan, status, mrr_cents, seats,
    started_at, canceled_at, updated_at
) values (
    %(subscription_id)s, %(org_id)s, %(plan)s, %(status)s, %(mrr_cents)s,
    %(seats)s, %(started_at)s, %(canceled_at)s, %(updated_at)s
)
"""

INSERT_EVENTS = """
insert into app.subscription_events (
    event_id, subscription_id, org_id, event_type, from_plan, to_plan,
    mrr_cents_before, mrr_cents_after, occurred_at, recorded_at
) values (
    %(event_id)s, %(subscription_id)s, %(org_id)s, %(event_type)s, %(from_plan)s,
    %(to_plan)s, %(mrr_cents_before)s, %(mrr_cents_after)s, %(occurred_at)s,
    %(recorded_at)s
)
"""

INSERT_INVOICES = """
insert into app.invoices (
    invoice_id, org_id, subscription_id, amount_cents, currency, status,
    period_start, period_end, issued_at, paid_at, recorded_at
) values (
    %(invoice_id)s, %(org_id)s, %(subscription_id)s, %(amount_cents)s, %(currency)s,
    %(status)s, %(period_start)s, %(period_end)s, %(issued_at)s, %(paid_at)s,
    %(recorded_at)s
)
"""

INSERT_LOGINS = """
insert into app.logins (
    login_id, client_event_id, user_id, org_id, idp, success,
    ip_country, logged_in_at, recorded_at
) values (
    %(login_id)s, %(client_event_id)s, %(user_id)s, %(org_id)s, %(idp)s,
    %(success)s, %(ip_country)s, %(logged_in_at)s, %(recorded_at)s
)
"""


def utcnow() -> datetime:
    return datetime.now(UTC)


def deterministic_uuid(rng: Random) -> UUID:
    value = rng.getrandbits(128)
    value &= ~(0xF << 76)
    value |= 0x4 << 76
    value &= ~(0x3 << 62)
    value |= 0x2 << 62
    return UUID(int=value)


def faker_for(seed: int | None) -> Faker:
    fake = Faker("en_US")
    if seed is not None:
        fake.seed_instance(seed)
    return fake


def _domain(name: str, rng: Random) -> str:
    slug = re.sub(r"[^a-z0-9]", "", name.lower())[:24]
    if not slug:
        slug = f"org{rng.randint(1000, 9999)}"
    return f"{slug}.example"


def _add_months(day: date, months: int) -> date:
    month_index = day.month - 1 + months
    year = day.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def _month_start(value: datetime) -> date:
    return value.date().replace(day=1)


def _stamp_recorded(
    rng: Random,
    occurred_at: datetime,
    now: datetime,
    late_probability: float,
) -> datetime:
    if rng.random() < late_probability:
        recorded = occurred_at + timedelta(days=rng.randint(2, 6), hours=rng.randint(0, 10))
    else:
        recorded = occurred_at + timedelta(minutes=rng.randint(1, 90))
    if recorded > now:
        recorded = now
    if recorded < occurred_at:
        recorded = occurred_at
    return recorded


def _step_plan(plan: str, direction: int) -> str | None:
    canonical = plan.lower()
    if canonical not in PLAN_ORDER:
        return None
    index = PLAN_ORDER.index(canonical) + direction
    if index < 0 or index >= len(PLAN_ORDER):
        return None
    return PLAN_ORDER[index]


def _event(
    rng: Random,
    subscription_id: UUID,
    org_id: UUID,
    event_type: str,
    from_plan: str | None,
    to_plan: str | None,
    before: int,
    after: int,
    occurred_at: datetime,
    now: datetime,
) -> dict:
    return {
        "event_id": deterministic_uuid(rng),
        "subscription_id": subscription_id,
        "org_id": org_id,
        "event_type": event_type,
        "from_plan": from_plan,
        "to_plan": to_plan,
        "mrr_cents_before": before,
        "mrr_cents_after": after,
        "occurred_at": occurred_at,
        "recorded_at": _stamp_recorded(rng, occurred_at, now, late_probability=0.05),
    }


def evolve_subscription(
    rng: Random,
    subscription_id: UUID,
    org_id: UUID,
    born_at: datetime,
    now: datetime,
    *,
    with_history: bool,
) -> tuple[dict, list[dict]]:
    """Walk a subscription from birth to now. MRR always comes from PLAN_MRR."""
    plan = "trial" if rng.random() < 0.45 else "starter"
    status = "trialing" if plan == "trial" else "active"
    mrr = PLAN_MRR[plan]
    events = [_event(rng, subscription_id, org_id, "created", None, plan, 0, mrr, born_at, now)]
    canceled_at = None

    if with_history:
        cursor = born_at
        while True:
            cursor = cursor + timedelta(days=rng.randint(28, 40))
            if cursor >= now - timedelta(days=1):
                break
            plan, status, mrr, canceled_at = _advance(
                rng, events, subscription_id, org_id, plan, status, mrr, canceled_at, cursor, now
            )

    seat_plan = plan.lower() if plan.lower() in SEAT_RANGES else "starter"
    low, high = SEAT_RANGES[seat_plan]
    stored_plan = plan.upper() if rng.random() < 0.03 else plan
    if status == "canceled" and rng.random() < 0.05:
        # Messy source row: canceled with no timestamp. Staging warns, it does not fail.
        canceled_at = None
    subscription = {
        "subscription_id": subscription_id,
        "org_id": org_id,
        "plan": stored_plan,
        "status": status,
        "mrr_cents": mrr,
        "seats": rng.randint(low, high),
        "started_at": born_at,
        "canceled_at": canceled_at,
        "updated_at": events[-1]["occurred_at"],
    }
    return subscription, events


def _advance(
    rng: Random,
    events: list[dict],
    subscription_id: UUID,
    org_id: UUID,
    plan: str,
    status: str,
    mrr: int,
    canceled_at: datetime | None,
    cursor: datetime,
    now: datetime,
) -> tuple[str, str, int, datetime | None]:
    if status == "canceled":
        if rng.random() < 0.3:
            plan = rng.choice(["starter", "growth"])
            after = PLAN_MRR[plan]
            events.append(
                _event(
                    rng, subscription_id, org_id, "reactivated", None, plan, 0, after, cursor, now
                )
            )
            return plan, "active", after, None
        return plan, status, mrr, canceled_at

    roll = rng.random()
    if status == "trialing":
        if roll < 0.55:
            after = PLAN_MRR["starter"]
            events.append(
                _event(
                    rng,
                    subscription_id,
                    org_id,
                    "upgraded",
                    "trial",
                    "starter",
                    mrr,
                    after,
                    cursor,
                    now,
                )
            )
            return "starter", "active", after, None
        if roll < 0.7:
            events.append(
                _event(rng, subscription_id, org_id, "canceled", plan, plan, mrr, 0, cursor, now)
            )
            return plan, "canceled", 0, cursor
        return plan, status, mrr, canceled_at

    if roll < 0.1:
        nxt = _step_plan(plan, 1)
        if nxt and nxt != "trial":
            after = PLAN_MRR[nxt]
            events.append(
                _event(rng, subscription_id, org_id, "upgraded", plan, nxt, mrr, after, cursor, now)
            )
            return nxt, "active", after, None
    elif roll < 0.16:
        nxt = _step_plan(plan, -1)
        if nxt and nxt != "trial":
            after = PLAN_MRR[nxt]
            events.append(
                _event(
                    rng, subscription_id, org_id, "downgraded", plan, nxt, mrr, after, cursor, now
                )
            )
            return nxt, "active", after, None
    elif roll < 0.22:
        events.append(
            _event(rng, subscription_id, org_id, "canceled", plan, plan, mrr, 0, cursor, now)
        )
        return plan, "canceled", 0, cursor
    elif roll < 0.24 and status == "active":
        # Rare billing delinquency. MRR stays put; the status is what changed.
        return plan, "past_due", mrr, None
    return plan, status, mrr, canceled_at


def build_users(
    rng: Random,
    fake: Faker,
    org_id: UUID,
    domain: str,
    born_at: datetime,
    now: datetime,
    count: int | None = None,
    *,
    include_owner: bool = True,
) -> list[dict]:
    span_days = max(1, (now - born_at).days)
    total = count if count is not None else rng.randint(2, 8)
    users = []
    for index in range(total):
        created = born_at + timedelta(days=rng.randint(0, span_days), hours=rng.randint(0, 20))
        if created > now:
            created = now
        first = fake.first_name()
        last = fake.last_name()
        user_id = deterministic_uuid(rng)
        local = re.sub(r"[^a-z]", "", first.lower()) or "user"
        email = f"{local}.{str(user_id)[:8]}@{domain}"
        if rng.random() < 0.04:
            email = None
        if include_owner and index == 0:
            role = "owner"
        else:
            role = rng.choice(["admin", "member", "member", "member"])
        status = "deactivated" if index > 0 and rng.random() < 0.06 else "active"
        users.append(
            {
                "user_id": user_id,
                "org_id": org_id,
                "email": email,
                "full_name": f"{first} {last}",
                "role": role,
                "status": status,
                "created_at": created,
                "updated_at": created,
            }
        )
    return users


def make_login(
    rng: Random,
    org_id: UUID,
    user_id: UUID | None,
    when: datetime,
    now: datetime,
    *,
    client_event_id: str | None = None,
    late_probability: float = 0.08,
) -> dict:
    return {
        "login_id": deterministic_uuid(rng),
        "client_event_id": client_event_id or str(deterministic_uuid(rng)),
        "user_id": user_id,
        "org_id": org_id,
        "idp": None if rng.random() < 0.03 else rng.choice(IDPS),
        "success": rng.random() > 0.03,
        "ip_country": rng.choice(COUNTRIES),
        "logged_in_at": when,
        "recorded_at": _stamp_recorded(rng, when, now, late_probability),
    }


def build_logins(
    rng: Random,
    users: Sequence[dict],
    org_id: UUID,
    now: datetime,
    login_days: int,
    canceled_at: datetime | None,
) -> list[dict]:
    logins: list[dict] = []
    window_start = now - timedelta(days=login_days)
    window_end = now if canceled_at is None else min(now, canceled_at)
    for user in users:
        start = max(window_start, user["created_at"])
        if start >= window_end:
            continue
        day = start.date()
        last_day = window_end.date()
        while day <= last_day:
            weekday = day.weekday()
            probability = 0.42 if weekday < 5 else 0.1
            if user["status"] != "active":
                probability *= 0.15
            if rng.random() < probability:
                draws = 2 if weekday < 5 and rng.random() < 0.08 else 1
                for _ in range(draws):
                    when = datetime(
                        day.year,
                        day.month,
                        day.day,
                        rng.randint(8, 18),
                        rng.randint(0, 59),
                        tzinfo=UTC,
                    )
                    if window_start <= when <= window_end:
                        logins.append(make_login(rng, org_id, user["user_id"], when, now))
            day += timedelta(days=1)
    return logins


def build_invoices(
    rng: Random,
    subscription: dict,
    events: Sequence[dict],
    now: datetime,
) -> list[dict]:
    if not events:
        return []
    ordered = sorted(events, key=lambda item: item["occurred_at"])
    cursor = _month_start(ordered[0]["occurred_at"])
    end = _month_start(now)
    index = 0
    running = 0
    invoices = []
    while cursor <= end:
        while index < len(ordered) and _month_start(ordered[index]["occurred_at"]) == cursor:
            running = ordered[index]["mrr_cents_after"]
            index += 1
        if running > 0:
            invoices.append(
                _invoice(
                    rng,
                    subscription,
                    cursor,
                    running,
                    now,
                    is_current=cursor == end,
                )
            )
        cursor = _add_months(cursor, 1)
    return invoices


def _invoice(
    rng: Random,
    subscription: dict,
    period_start: date,
    amount_cents: int,
    now: datetime,
    *,
    is_current: bool,
) -> dict:
    period_end = _add_months(period_start, 1) - timedelta(days=1)
    issued_at = datetime(
        period_start.year, period_start.month, period_start.day, 12, 0, tzinfo=UTC
    )
    roll = rng.random()
    currency = "EUR" if roll < 0.08 else "GBP" if roll < 0.12 else "USD"
    if rng.random() < 0.02:
        currency = currency.lower()
    if is_current:
        status = "open"
        paid_at = None
        recorded_at = now
    elif rng.random() < 0.02:
        status = "void"
        paid_at = None
        recorded_at = issued_at + timedelta(days=rng.randint(1, 5))
    else:
        status = "paid"
        paid_at = datetime(
            period_end.year, period_end.month, period_end.day, 15, 0, tzinfo=UTC
        ) + timedelta(days=rng.randint(0, 6))
        if paid_at > now:
            paid_at = now
        if rng.random() < 0.03:
            paid_at = None
        recorded_at = paid_at or issued_at
    return {
        "invoice_id": deterministic_uuid(rng),
        "org_id": subscription["org_id"],
        "subscription_id": subscription["subscription_id"],
        "amount_cents": amount_cents,
        "currency": currency,
        "status": status,
        "period_start": period_start,
        "period_end": period_end,
        "issued_at": issued_at,
        "paid_at": paid_at,
        "recorded_at": recorded_at,
    }


def build_organization(
    rng: Random,
    fake: Faker,
    now: datetime,
    *,
    history_days: int,
    login_days: int,
    with_history: bool,
    born_at: datetime | None = None,
) -> dict:
    if born_at is None:
        born_at = now - timedelta(days=rng.randint(21, history_days), hours=rng.randint(0, 23))
    name = fake.company()
    domain = _domain(name, rng)
    org_id = deterministic_uuid(rng)
    subscription_id = deterministic_uuid(rng)
    subscription, events = evolve_subscription(
        rng, subscription_id, org_id, born_at, now, with_history=with_history
    )
    users = build_users(rng, fake, org_id, domain, born_at, now)
    logins = build_logins(rng, users, org_id, now, login_days, subscription["canceled_at"])
    invoices = build_invoices(rng, subscription, events, now)
    organization = {
        "org_id": org_id,
        "name": name,
        "domain": domain,
        "industry": rng.choice(INDUSTRIES),
        "employee_band": rng.choice(EMPLOYEE_BANDS),
        "country_code": rng.choice(COUNTRIES),
        "status": "churned" if subscription["status"] == "canceled" else "active",
        "created_at": born_at,
        "updated_at": subscription["updated_at"],
    }
    return {
        "organization": organization,
        "users": users,
        "subscription": subscription,
        "events": events,
        "invoices": invoices,
        "logins": logins,
    }


def build_world(
    rng: Random,
    fake: Faker,
    *,
    orgs: int,
    history_days: int,
    login_days: int,
    now: datetime | None = None,
) -> dict[str, list[dict]]:
    now = now or utcnow()
    organizations = []
    users = []
    subscriptions = []
    events = []
    invoices = []
    logins = []
    for _ in range(orgs):
        built = build_organization(
            rng,
            fake,
            now,
            history_days=history_days,
            login_days=login_days,
            with_history=True,
        )
        organizations.append(built["organization"])
        users.extend(built["users"])
        subscriptions.append(built["subscription"])
        events.extend(built["events"])
        invoices.extend(built["invoices"])
        logins.extend(built["logins"])

    if logins:
        sample_size = min(12, len(logins))
        for original in rng.sample(logins, k=sample_size):
            duplicate = dict(original)
            duplicate["login_id"] = deterministic_uuid(rng)
            duplicate["recorded_at"] = min(
                now, original["recorded_at"] + timedelta(minutes=rng.randint(1, 30))
            )
            logins.append(duplicate)
        for _ in range(5):
            org = rng.choice(organizations)
            when = now - timedelta(days=rng.randint(1, 10), hours=rng.randint(0, 12))
            logins.append(
                make_login(rng, org["org_id"], deterministic_uuid(rng), when, now)
            )

    return {
        "organizations": organizations,
        "users": users,
        "subscriptions": subscriptions,
        "subscription_events": events,
        "invoices": invoices,
        "logins": logins,
    }
