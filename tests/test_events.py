from __future__ import annotations

from collections import Counter, defaultdict
from itertools import islice

from streamplat.events import Faults, Storefront, schedule

FUNNEL = ["PAGE_VIEW", "ADD_TO_CART", "CHECKOUT_STARTED", "PAYMENT_FAILED", "PAYMENT_SUCCEEDED"]


def take(n: int, seed: int = 7, faults: Faults | None = None):
    return list(islice(schedule("run", seed, faults or Faults()), n))


def test_same_seed_same_schedule():
    a, b = take(5000), take(5000)
    assert [(e.seq, e.key, e.record, e.poison, e.late_by_ms, e.delay_ms) for e in a] == [
        (e.seq, e.key, e.record, e.poison, e.late_by_ms, e.delay_ms) for e in b
    ]


def test_different_seed_different_schedule():
    assert [e.record for e in take(200, 1)] != [e.record for e in take(200, 2)]


def test_event_ids_follow_the_slot_sequence():
    plan = take(1000)
    assert [e.seq for e in plan] == list(range(1000))
    for e in plan:
        if e.record is not None:
            assert e.record["event_id"] == f"run-{e.seq:09d}"


def test_fault_rates_are_close_to_configured():
    faults = Faults(late=0.02, out_of_order=0.1, duplicate=0.05, poison=0.01)
    plan = take(50_000, faults=faults)
    n = len(plan)
    poison = sum(e.poison is not None for e in plan)
    late = sum(e.late_by_ms > 0 for e in plan)
    dup = sum(e.duplicate_after_ms is not None for e in plan)
    assert 0.008 < poison / n < 0.012
    assert 0.016 < late / n < 0.024
    assert 0.04 < dup / n < 0.06
    assert {e.poison for e in plan if e.poison} == {
        "not_avro",
        "unknown_schema",
        "truncated",
        "rule_violation",
    }


def test_late_events_are_older_than_the_watermark_could_wait():
    plan = take(20_000)
    late = [e.late_by_ms for e in plan if e.late_by_ms]
    assert late and min(late) >= 180_000
    ooo = [e.delay_ms for e in plan if e.delay_ms]
    # Out-of-order delays stay inside Flink's 5 s bound, so they are on time.
    assert ooo and max(ooo) <= 3_000


def test_sessions_walk_the_funnel_in_order():
    shop = Storefront("run", seed=11, concurrency=50)
    by_session: dict[str, list[str]] = defaultdict(list)
    for i in range(20_000):
        key, record = shop.next_record(f"run-{i}")
        by_session[key].append(record["event_type"])
    stages = Counter()
    for events in by_session.values():
        ranks = [FUNNEL.index(t) if t != "PAYMENT_SUCCEEDED" else 3 for t in events]
        assert ranks == sorted(ranks), events
        stages.update(set(events))
    # Every stage of the funnel occurs, and each is rarer than the one before.
    assert stages["PAGE_VIEW"] > stages["ADD_TO_CART"] > stages["CHECKOUT_STARTED"]
    assert stages["CHECKOUT_STARTED"] >= stages["PAYMENT_SUCCEEDED"] > 0
