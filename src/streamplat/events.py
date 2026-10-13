"""A seeded storefront: who browses, what they add, who pays — and what goes wrong.

Everything here is a pure function of the seed. The wall clock only enters when the
generator stamps ``event_time``, so two runs with the same seed send the same
events, the same duplicates and the same poison, in the same order.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Literal

PoisonKind = Literal["not_avro", "unknown_schema", "truncated", "rule_violation"]
POISON_KINDS: tuple[PoisonKind, ...] = ("not_avro", "unknown_schema", "truncated", "rule_violation")

N_PRODUCTS = 500
N_CUSTOMERS = 50_000


@dataclass(frozen=True)
class Faults:
    """Per-event probabilities. ``poison`` is per slot; the rest apply to valid events."""

    late: float = 0.01
    late_by_ms: tuple[int, int] = (180_000, 300_000)
    out_of_order: float = 0.05
    ooo_delay_ms: tuple[int, int] = (200, 3_000)
    duplicate: float = 0.02
    dup_delay_ms: tuple[int, int] = (0, 2_000)
    poison: float = 0.001


@dataclass
class Emission:
    """One slot of the send schedule."""

    seq: int
    key: str
    record: dict[str, Any] | None  # None for poison that is not a record at all
    poison: PoisonKind | None = None
    late_by_ms: int = 0
    delay_ms: int = 0  # out-of-order: hold the send this long
    duplicate_after_ms: int | None = None  # resend the same bytes after this long
    junk: bytes = b""


@dataclass
class _Session:
    session_id: str
    customer_id: str | None
    client: str
    currency: str
    steps: list[dict[str, Any]] = field(default_factory=list)


class Storefront:
    """Concurrent shopper sessions, interleaved the way a real storefront interleaves them."""

    def __init__(self, run_id: str, seed: int, concurrency: int = 200) -> None:
        self.run_id = run_id
        self.rng = random.Random(seed)
        prices = random.Random(seed ^ 0x5EED)
        self.prices = {f"P{i:04d}": prices.randint(299, 24_999) for i in range(1, N_PRODUCTS + 1)}
        self.products = list(self.prices)
        self._n_sessions = 0
        self._n_orders = 0
        self.active = [self._new_session() for _ in range(concurrency)]

    def _new_session(self) -> _Session:
        rng = self.rng
        self._n_sessions += 1
        s = _Session(
            session_id=f"{self.run_id}-s{self._n_sessions:07d}",
            customer_id=f"C{rng.randrange(N_CUSTOMERS):05d}" if rng.random() < 0.6 else None,
            client="WEB" if rng.random() < 0.75 else "API",
            currency=rng.choices(["USD", "EUR", "GBP"], weights=[80, 12, 8])[0],
        )
        s.steps = self._journey(s)
        return s

    def _journey(self, s: _Session) -> list[dict[str, Any]]:
        """page views, then maybe a cart, a checkout and a payment (which may fail and retry)."""
        rng = self.rng
        steps: list[dict[str, Any]] = []
        viewed = rng.sample(self.products, k=rng.randint(1, 6))
        steps.append({"event_type": "PAGE_VIEW", "page": "/"})
        for p in viewed:
            steps.append({"event_type": "PAGE_VIEW", "page": f"/products/{p}", "product_id": p})
        if rng.random() >= 0.35:
            return steps
        cart = viewed[: rng.randint(1, min(2, len(viewed)))]
        total = 0
        for p in cart:
            qty = rng.randint(1, 3)
            total += qty * self.prices[p]
            steps.append({"event_type": "ADD_TO_CART", "product_id": p, "quantity": qty})
        if rng.random() >= 0.5:
            return steps
        self._n_orders += 1
        order = f"{self.run_id}-o{self._n_orders:07d}"
        steps.append({"event_type": "CHECKOUT_STARTED", "order_id": order, "amount_cents": total})
        attempts = 1 if rng.random() < 0.85 else 2
        for attempt in range(attempts):
            ok = attempt == attempts - 1 and rng.random() < 0.9
            steps.append(
                {
                    "event_type": "PAYMENT_SUCCEEDED" if ok else "PAYMENT_FAILED",
                    "order_id": order,
                    "amount_cents": total,
                }
            )
        return steps

    def next_record(self, event_id: str) -> tuple[str, dict[str, Any]]:
        """The next event of a random live session, as an Avro record without ``event_time``."""
        i = self.rng.randrange(len(self.active))
        s = self.active[i]
        step = s.steps.pop(0)
        if not s.steps:
            self.active[i] = self._new_session()
        record: dict[str, Any] = {
            "event_id": event_id,
            "event_type": step["event_type"],
            "event_time": 0,
            "session_id": s.session_id,
            "customer_id": s.customer_id,
            "client": s.client,
            "page": step.get("page"),
            "product_id": step.get("product_id"),
            "quantity": step.get("quantity"),
            "order_id": step.get("order_id"),
            "amount_cents": step.get("amount_cents"),
            "currency": s.currency,
            "is_probe": False,
        }
        return s.session_id, record


def break_rule(record: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    """A record that is valid Avro and invalid business: the gatekeeper's rules must catch it."""
    bad = dict(record)
    choice = rng.randrange(3)
    if choice == 0:
        bad.update(event_type="ADD_TO_CART", product_id="P0001", quantity=0)
    elif choice == 1:
        bad.update(
            event_type="PAYMENT_SUCCEEDED", order_id=f"{record['event_id']}-o", amount_cents=-1
        )
    else:
        bad.update(currency="XXX")
    return bad


def schedule(run_id: str, seed: int, faults: Faults, concurrency: int = 200) -> Iterator[Emission]:
    """The infinite, deterministic send schedule for one run."""
    shop = Storefront(run_id, seed, concurrency)
    rng = random.Random(seed ^ 0xFA17)
    seq = 0
    while True:
        event_id = f"{run_id}-{seq:09d}"
        if rng.random() < faults.poison:
            kind = POISON_KINDS[rng.randrange(len(POISON_KINDS))]
            key, record = shop.next_record(event_id)
            if kind == "rule_violation":
                yield Emission(seq, key, break_rule(record, rng), poison=kind)
            elif kind == "truncated":
                yield Emission(seq, key, record, poison=kind)
            else:
                junk = b'{"event":"page_view","id":' + str(seq).encode() + b"}"
                yield Emission(seq, f"{run_id}-poison-{seq}", None, poison=kind, junk=junk)
        else:
            key, record = shop.next_record(event_id)
            em = Emission(seq, key, record)
            if rng.random() < faults.late:
                em.late_by_ms = rng.randint(*faults.late_by_ms)
            elif rng.random() < faults.out_of_order:
                em.delay_ms = rng.randint(*faults.ooo_delay_ms)
            if rng.random() < faults.duplicate:
                em.duplicate_after_ms = rng.randint(*faults.dup_delay_ms)
            yield em
        seq += 1
