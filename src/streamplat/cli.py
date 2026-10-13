"""streamplat: the generator, the gate, the probe and the checks, as one command."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from .events import Faults
from .settings import Settings


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="streamplat")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("bootstrap", help="create topics, set compatibility, register v1")
    sub.add_parser("gatekeeper", help="run the contract gate (raw -> valid | dlq)")

    ex = sub.add_parser("exporter", help="run the freshness probe and Kafka health exporter")
    ex.add_argument("--interval", type=float, default=5.0)
    ex.add_argument("--threshold", type=float, default=15.0)
    ex.add_argument("--role", choices=["probe", "kafka"], default="probe")

    g = sub.add_parser("generate", help="send the seeded storefront at a fixed rate")
    g.add_argument("--rate", type=float, default=1000)
    g.add_argument("--duration", type=float, default=60)
    g.add_argument("--seed", type=int, default=7)
    g.add_argument("--run-id", default=None)
    g.add_argument("--late", type=float, default=0.01)
    g.add_argument("--ooo", type=float, default=0.05)
    g.add_argument("--dup", type=float, default=0.02)
    g.add_argument("--poison", type=float, default=0.001)
    g.add_argument("--schema", default="v1.avsc", help="contract file to produce with")
    g.add_argument("--ledger-dir", type=Path, default=Path("runs"))

    r = sub.add_parser("reconcile", help="count a run at every layer against its ledger")
    r.add_argument("ledger", type=Path)
    r.add_argument("--layers", default="kafka,clickhouse,iceberg")
    r.add_argument("--wait", type=float, default=0, help="retry until exact or this many s")

    lat = sub.add_parser("latency", help="event time to queryable in ClickHouse, for one run")
    lat.add_argument("ledger", type=Path)

    sub.add_parser("contract-check", help="prove the registry refuses breaking changes")

    args = p.parse_args(argv)
    settings = Settings()

    if args.cmd == "bootstrap":
        from . import bootstrap

        bootstrap.main(settings)
    elif args.cmd == "gatekeeper":
        from . import gatekeeper

        gatekeeper.main(settings)
    elif args.cmd == "exporter":
        from . import exporter

        exporter.main(settings, args.role, args.interval, args.threshold)
    elif args.cmd == "generate":
        from . import generator

        run_id = args.run_id or time.strftime("r%m%d%H%M%S")
        ledger = generator.run(
            settings,
            run_id,
            args.rate,
            args.duration,
            args.seed,
            Faults(late=args.late, out_of_order=args.ooo, duplicate=args.dup, poison=args.poison),
            args.ledger_dir / f"{run_id}.json",
            args.schema,
        )
        summary = {
            "run_id": run_id,
            "ledger": str(args.ledger_dir / f"{run_id}.json"),
            "slots": ledger.slots,
            "sends": ledger.sends,
            "acked": ledger.acked,
            "failed": ledger.failed,
            "expected_valid": ledger.expected_valid,
            "poison": len(ledger.poison_seqs),
            "errors": ledger.errors,
        }
        print(json.dumps(summary))
    elif args.cmd == "reconcile":
        from . import reconcile

        layers = args.layers.split(",")
        deadline = time.time() + args.wait
        report = reconcile.reconcile(settings, args.ledger, layers)
        # Windows close and ClickHouse flushes a little after the run: read again,
        # but only the groups of layers that are not exact yet.
        while not report["exact"] and time.time() < deadline:
            time.sleep(5)
            again = reconcile.reconcile(settings, args.ledger, reconcile.failing_groups(report))
            report["layers"].update(again["layers"])
            report["exact"] = not reconcile.failing_groups(report)
        print(json.dumps(report, indent=1))
        return 0 if report["exact"] else 1
    elif args.cmd == "latency":
        from . import reconcile

        print(json.dumps(reconcile.latency(settings, args.ledger)))
    elif args.cmd == "contract-check":
        from . import contracts

        report = contracts.check(settings)
        print(json.dumps(report, indent=1))
        return 0 if report["passed"] else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
