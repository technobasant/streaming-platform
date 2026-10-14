"""Run the measurements in MEASUREMENTS.md against the running stack (`make up` first).

Each experiment drives load with the generator container, injects its fault from the
host with the Docker CLI, watches Flink's REST API and Prometheus, then reconciles
the run at every layer. Raw results go to results/<experiment>.json.

    uv run python scripts/measure.py load --runs 3
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
# Run ids must never repeat: the ClickHouse checks select a run by its id prefix.
SESSION = time.strftime("%H%M")
RESULTS = ROOT / "results"
FLINK = "http://localhost:18081"
PROM = "http://localhost:19090"
CH = "http://localhost:18123"
CH_AUTH = ("default", "lumen")
COMPOSE = ["docker", "compose"]


def sh(*cmd: str, check: bool = True, capture: bool = True) -> str:
    r = subprocess.run(cmd, cwd=ROOT, capture_output=capture, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}: {r.stderr[-2000:]}")
    return r.stdout


def tools(*args: str, check: bool = True) -> str:
    return sh(
        *COMPOSE, "run", "--rm", "--no-deps", "-T", "-v", f"{ROOT}/runs:/app/runs",
        "gatekeeper", *args, check=check,
    )  # fmt: skip


class Generator(threading.Thread):
    """The generator container, in the background, so faults can be injected mid-run."""

    def __init__(self, run_id: str, rate: float, duration: float, *extra: str) -> None:
        super().__init__(daemon=True)
        self.cmd = [
            *COMPOSE, "--profile", "load", "run", "--rm", "-T", "generator", "generate",
            "--rate", str(rate), "--duration", str(duration), "--run-id", run_id, *extra,
        ]  # fmt: skip
        self.summary: dict[str, Any] = {}
        self.started = time.time()

    def run(self) -> None:
        out = sh(*self.cmd)
        self.summary = json.loads(out.strip().splitlines()[-1])


def reconcile(run_id: str, wait: int = 240) -> dict[str, Any]:
    for attempt in range(3):
        r = subprocess.run(
            [*COMPOSE, "run", "--rm", "--no-deps", "-T", "-v", f"{ROOT}/runs:/app/runs",
             "gatekeeper", "reconcile", f"runs/{run_id}.json", "--wait", str(wait)],
            cwd=ROOT, capture_output=True, text=True,
        )  # fmt: skip
        try:
            report: dict[str, Any] = json.loads(r.stdout)
            return report
        except json.JSONDecodeError:
            print(f"reconcile {run_id} attempt {attempt}: {r.stderr[-800:]}", file=sys.stderr)
    raise RuntimeError(f"reconcile {run_id} failed three times")


def latency(run_id: str) -> dict[str, Any]:
    result: dict[str, Any] = json.loads(tools("latency", f"runs/{run_id}.json").strip())
    return result


def prom(query: str) -> float | None:
    r = httpx.get(f"{PROM}/api/v1/query", params={"query": query}, timeout=10).json()
    res = r["data"]["result"]
    return float(res[0]["value"][1]) if res else None


def alerts() -> dict[str, str]:
    data = httpx.get(f"{PROM}/api/v1/alerts", timeout=10).json()["data"]["alerts"]
    return {a["labels"]["alertname"]: a["state"] for a in data}


def flink_jobs() -> dict[str, dict[str, Any]]:
    jobs = httpx.get(f"{FLINK}/jobs/overview", timeout=10).json()["jobs"]
    live = [j for j in jobs if j["state"] not in ("CANCELED", "FINISHED", "FAILED")]
    return {j["name"]: j for j in live}


def jobs_healthy() -> bool:
    jobs = flink_jobs()
    return len(jobs) == 2 and all(
        j["state"] == "RUNNING" and j["tasks"]["running"] == j["tasks"]["total"]
        for j in jobs.values()
    )


def checkpoints(jid: str) -> dict[str, Any]:
    body: dict[str, Any] = httpx.get(f"{FLINK}/jobs/{jid}/checkpoints", timeout=10).json()
    return body


def wait_until(pred: Any, timeout: float, step: float = 0.25) -> float | None:
    """Seconds until ``pred()`` is true, or None on timeout."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if pred():
                return time.time() - t0
        except Exception:
            pass
        time.sleep(step)
    return None


def settle(timeout: float = 300) -> None:
    """Healthy jobs, no firing alerts, and drained consumer groups before the next run."""
    wait_until(jobs_healthy, timeout)
    wait_until(lambda: not any(s == "firing" for s in alerts().values()), timeout, 2)
    wait_until(lambda: (prom("max(sp_consumer_lag)") or 0) < 500, timeout, 2)


def ch_query(sql: str) -> tuple[str, float]:
    t0 = time.perf_counter()
    r = httpx.post(CH, content=sql, auth=CH_AUTH, timeout=60)
    r.raise_for_status()
    return r.text, (time.perf_counter() - t0) * 1000


def save(name: str, runs: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    RESULTS.mkdir(exist_ok=True)
    env = {
        "docker": sh("docker", "version", "--format", "{{.Server.Version}}").strip(),
        "cpus": sh("docker", "info", "--format", "{{.NCPU}}").strip(),
        "mem_bytes": sh("docker", "info", "--format", "{{.MemTotal}}").strip(),
    }
    body = {"experiment": name, "summary": summary, "environment": env, "runs": runs}
    (RESULTS / f"{name}.json").write_text(json.dumps(body, indent=1) + "\n")
    print(json.dumps(summary, indent=1))


def layers_exact(report: dict[str, Any]) -> dict[str, bool]:
    return {k: bool(v.get("exact", v.get("matches_ledger"))) for k, v in report["layers"].items()}


def med(xs: list[float | None]) -> float | None:
    vals = [x for x in xs if x is not None]
    return round(statistics.median(vals), 3) if vals else None


# --------------------------------------------------------------------------- load
def exp_load(runs: int, rates: list[int], duration: int) -> None:
    # Resumable: runs already in results/load.json are kept, and a run whose ledger
    # exists (generated, then the script died) is reconciled without resending.
    path = RESULTS / "load.json"
    out: list[dict[str, Any]] = json.loads(path.read_text())["runs"] if path.exists() else []
    done = {r["latency"]["run_id"] for r in out}
    for rate in rates:
        for i in range(runs):
            run_id = f"load{SESSION}r{rate}x{i}"
            if run_id in done:
                continue
            g = Generator(run_id, rate, duration)
            if not (ROOT / "runs" / f"{run_id}.json").exists():
                settle()
                g.start()
                g.join()
            rec = reconcile(run_id)
            lat = latency(run_id)
            out.append({"rate": rate, "generator": g.summary, "latency": lat, "exact": rec["exact"],
                        "layers": layers_exact(rec), "reconcile": rec})  # fmt: skip
            print(rate, i, lat, rec["exact"], file=sys.stderr)
            save("load", out, _load_summary(out))
    save("load", out, _load_summary(out))


def _load_summary(out: list[dict[str, Any]]) -> dict[str, Any]:
    rates = sorted({r["rate"] for r in out})
    summary = {}
    for rate in rates:
        rs = [r for r in out if r["rate"] == rate]
        summary[str(rate)] = {
            "runs": len(rs),
            "achieved_send_rate": med([r["latency"]["achieved_send_rate"] for r in rs]),
            "p50_s": med([r["latency"]["p50_s"] for r in rs]),
            "p95_s": med([r["latency"]["p95_s"] for r in rs]),
            "p99_s": med([r["latency"]["p99_s"] for r in rs]),
            "max_s": max(r["latency"]["max_s"] for r in rs),
            "all_exact": all(r["exact"] for r in rs),
        }
    return summary


# --------------------------------------------------------------------- broker kill
def controller_leader() -> int:
    out = sh(*COMPOSE, "exec", "-T", "kafka-1", "/opt/kafka/bin/kafka-metadata-quorum.sh",
             "--bootstrap-server", "localhost:9092", "describe", "--status")  # fmt: skip
    for line in out.splitlines():
        if line.startswith("LeaderId:"):
            return int(line.split()[1])
    raise RuntimeError(out)


def exp_broker_kill(runs: int, rate: int) -> None:
    out: list[dict[str, Any]] = []
    for i in range(runs):
        settle()
        victim = f"kafka-{controller_leader()}"
        run_id = f"bkill{SESSION}x{i}"
        g = Generator(run_id, rate, 100)
        g.start()
        time.sleep(30)
        t_kill = time.time()
        sh(*COMPOSE, "kill", "-s", "SIGKILL", victim)
        urp_seen = wait_until(lambda: (prom("sp_under_replicated_partitions") or 0) > 0, 30, 1)
        time.sleep(max(0, 30 - (time.time() - t_kill)))
        sh(*COMPOSE, "start", victim)
        isr_back = wait_until(lambda: prom("sp_under_replicated_partitions") == 0, 300, 1)
        g.join()
        rec = reconcile(run_id)
        lat = latency(run_id)
        ledger = json.loads((ROOT / "runs" / f"{run_id}.json").read_text())
        t0 = ledger["started_at_ms"] / 1000
        kill_sec = int(t_kill - t0)
        tl = {int(k): v for k, v in ledger["timeline"].items()}
        window = [tl.get(s, [0, 0, 0, 0]) for s in range(kill_sec - 1, kill_sec + 15)]
        steady = [tl[s] for s in range(5, kill_sec - 2) if s in tl]
        restarts = {n: j.get("numRestarts") for n, j in _job_details().items()}
        out.append({
            "victim": victim, "was_controller": True, "kill_at_s": round(t_kill - t0, 1),
            "urp_visible_after_s": urp_seen, "isr_restored_after_restart_s": isr_back,
            "producer_failed": ledger["failed"], "producer_errors": ledger["errors"],
            "max_ack_ms_steady": round(max(b[2] for b in steady), 1),
            "max_ack_ms_after_kill": round(max(b[2] for b in window), 1),
            "acked_per_s_after_kill": [b[0] for b in window],
            "gatekeeper_aborts": prom("sum(sp_gatekeeper_aborted_transactions_total)"),
            "flink_restarts": restarts, "latency": lat, "exact": rec["exact"],
            "layers": layers_exact(rec), "reconcile": rec,
        })  # fmt: skip
        print(i, victim, urp_seen, isr_back, rec["exact"], file=sys.stderr)
    summary = {
        "runs": len(out),
        "producer_failed_total": sum(r["producer_failed"] for r in out),
        "max_ack_ms_after_kill_median": med([r["max_ack_ms_after_kill"] for r in out]),
        "max_ack_ms_steady_median": med([r["max_ack_ms_steady"] for r in out]),
        "isr_restored_after_restart_s_median": med(
            [r["isr_restored_after_restart_s"] for r in out]
        ),
        "p99_s_median": med([r["latency"]["p99_s"] for r in out]),
        "all_exact": all(r["exact"] for r in out),
    }
    save("broker-kill", out, summary)


def _job_details() -> dict[str, dict[str, Any]]:
    details = {}
    for name, j in flink_jobs().items():
        d = httpx.get(f"{FLINK}/jobs/{j['jid']}", timeout=10).json()
        restarts = prom(f'flink_jobmanager_job_numRestarts{{job_name="{name}"}}')
        details[name] = {"jid": j["jid"], "state": d["state"], "numRestarts": restarts}
    return details


# ------------------------------------------------------------------------ TM kill
def tm_offsets(service: str) -> dict[tuple[str, str], tuple[float, float]]:
    """(job, partition) -> (currentOffset, committedOffset) for events.valid, from one TM."""
    text = sh(*COMPOSE, "exec", "-T", service, "curl", "-s", "localhost:9249", check=False)
    cur: dict[tuple[str, str], float] = {}
    com: dict[tuple[str, str], float] = {}
    for line in text.splitlines():
        if "KafkaSourceReader_topic_partition_" not in line or 'topic="events.valid"' not in line:
            continue
        labels = dict(
            kv.split("=", 1) for kv in line[line.index("{") + 1 : line.index("}")].split(",")
        )
        key = (labels["job_name"].strip('"'), labels["partition"].strip('"'))
        value = float(line.rsplit(" ", 1)[1])
        if "currentOffset" in line:
            cur[key] = value
        elif "committedOffset" in line:
            com[key] = value
    return {k: (cur[k], com.get(k, 0.0)) for k in cur}


def busiest_tm() -> tuple[str, int]:
    """The TaskManager service running the most slots, and how many it runs."""
    tms = httpx.get(f"{FLINK}/taskmanagers", timeout=10).json()["taskmanagers"]
    tm = max(tms, key=lambda t: t["slotsNumber"] - t["freeSlots"])
    ip = tm["id"].split(":")[0]
    for service in ("taskmanager-1", "taskmanager-2"):
        cid = sh(*COMPOSE, "ps", "-q", service).strip()
        fmt = "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"
        if cid and sh("docker", "inspect", "-f", fmt, cid).strip() == ip:
            return service, tm["slotsNumber"] - tm["freeSlots"]
    raise RuntimeError(f"no TaskManager container has IP {ip}")


def exp_tm_kill(runs: int, rate: int) -> None:
    out: list[dict[str, Any]] = []
    for i in range(runs):
        settle()
        victim, busy_slots = busiest_tm()
        run_id = f"tmkill{SESSION}x{i}"
        g = Generator(run_id, rate, 90)
        g.start()
        time.sleep(30)
        before = {n: j["jid"] for n, j in flink_jobs().items()}
        offsets = {**tm_offsets("taskmanager-1"), **tm_offsets("taskmanager-2")}
        in_flight = sum(c - k for c, k in offsets.values() if c > 0 and k > 0)
        t_kill = time.time()
        sh(*COMPOSE, "kill", "-s", "SIGKILL", victim)
        # Flink notices through the lost RPC connection or the 10 s heartbeat timeout.
        noticed = wait_until(lambda: any(j["state"] != "RUNNING" or
                                         j["tasks"]["running"] < j["tasks"]["total"]
                                         for j in flink_jobs().values()), 60, 0.1)  # fmt: skip
        running = wait_until(jobs_healthy, 300, 0.1)
        t_running = time.time()
        restored: dict[str, Any] = {}
        for name, jid in before.items():

            def _ckpt_after(jid: str = jid, t_running: float = t_running) -> bool:
                c = checkpoints(jid)["latest"]["completed"]
                return bool(c and c["latest_ack_timestamp"] / 1000 > t_running)

            first_ckpt = wait_until(_ckpt_after, 120, 0.25)
            info = checkpoints(jid)["latest"]["restored"] or {}
            restored[name] = {
                "restored_checkpoint": info.get("id"),
                "restored_at_s": round(info["restore_timestamp"] / 1000 - t_kill, 2)
                if info else None,
                "first_checkpoint_after_running_s": first_ckpt,
            }  # fmt: skip
        sh(*COMPOSE, "start", victim)
        g.join()
        rec = reconcile(run_id)
        lat = latency(run_id)
        out.append({
            "victim": victim, "victim_busy_slots": busy_slots,
            "kill_at_s": round(t_kill - g.started, 1),
            "failure_noticed_s": noticed, "all_tasks_running_s": running,
            "jobs": restored,
            "clean_source_records_since_last_checkpoint": in_flight,
            "events_clean_aborted_and_rewritten":
                rec["layers"]["events.clean"]["aborted_and_rewritten"],
            "latency": lat, "exact": rec["exact"], "layers": layers_exact(rec), "reconcile": rec,
        })  # fmt: skip
        print(i, noticed, running, restored, rec["exact"], file=sys.stderr)
    summary = {
        "runs": len(out),
        "failure_noticed_s_median": med([r["failure_noticed_s"] for r in out]),
        "all_tasks_running_s_median": med([r["all_tasks_running_s"] for r in out]),
        "events_clean_aborted_and_rewritten": [
            r["events_clean_aborted_and_rewritten"] for r in out
        ],
        "source_records_since_checkpoint_at_kill": [
            r["clean_source_records_since_last_checkpoint"] for r in out
        ],
        "p99_s": [r["latency"]["p99_s"] for r in out],
        "max_s": [r["latency"]["max_s"] for r in out],
        "all_exact": all(r["exact"] for r in out),
    }
    save("tm-kill", out, summary)


# -------------------------------------------------------------------------- stall
def exp_stall(runs: int, rate: int, hold: int) -> None:
    out: list[dict[str, Any]] = []
    tms = ["taskmanager-1", "taskmanager-2"]
    for i in range(runs):
        settle()
        run_id = f"stall{SESSION}x{i}"
        g = Generator(run_id, rate, 60 + hold + 120)
        g.start()
        time.sleep(60)
        t_pause = time.time()
        sh(*COMPOSE, "pause", *tms)
        pending = wait_until(lambda: alerts().get("FreshnessBudgetFastBurn") in
                             ("pending", "firing"), hold, 1)  # fmt: skip
        firing = wait_until(lambda: alerts().get("FreshnessBudgetFastBurn") == "firing",
                            max(1, hold - (time.time() - t_pause)), 1)  # fmt: skip
        fired_at = (time.time() - t_pause) if firing is not None else None
        time.sleep(max(0, hold - (time.time() - t_pause)))
        t_resume = time.time()
        sh(*COMPOSE, "unpause", *tms)
        recovered = wait_until(jobs_healthy, 300, 0.5)
        # Starts when the jobs are healthy again, not at the unpause.
        resolved = wait_until(lambda: "FreshnessBudgetFastBurn" not in alerts(), 600, 1)
        since_resume = round(time.time() - t_resume, 2) if resolved is not None else None
        burn5m = prom("sp:freshness_error_ratio:rate5m")
        g.join()
        rec = reconcile(run_id, wait=300)
        out.append({
            "paused_s": round(t_resume - t_pause, 1),
            "fast_burn_pending_after_s": pending, "fast_burn_firing_after_s": fired_at,
            "jobs_running_after_resume_s": recovered,
            "fast_burn_resolved_after_jobs_running_s": resolved,
            "fast_burn_resolved_after_resume_s": since_resume,
            "error_ratio_5m_at_resolve": burn5m,
            "slow_burn_state_at_end": alerts().get("FreshnessBudgetSlowBurn", "inactive"),
            "exact": rec["exact"], "layers": layers_exact(rec), "reconcile": rec,
        })  # fmt: skip
        print(i, out[-1]["fast_burn_firing_after_s"], resolved, rec["exact"], file=sys.stderr)
    summary = {
        "runs": len(out),
        "fast_burn_firing_after_s_median": med([r["fast_burn_firing_after_s"] for r in out]),
        "fast_burn_resolved_after_resume_s_median": med(
            [r["fast_burn_resolved_after_resume_s"] for r in out]
        ),
        "fast_burn_resolved_after_jobs_running_s_median": med(
            [r["fast_burn_resolved_after_jobs_running_s"] for r in out]
        ),
        "jobs_running_after_resume_s_median": med([r["jobs_running_after_resume_s"] for r in out]),
        "all_exact": all(r["exact"] for r in out),
    }
    save("stall", out, summary)


# ----------------------------------------------------------------------- contract
def exp_contract(runs: int) -> None:
    out: list[dict[str, Any]] = []
    for _ in range(runs):
        out.append(json.loads(tools("contract-check")))
    # A producer built on a breaking schema, against the live subject.
    r = subprocess.run(
        [*COMPOSE, "run", "--rm", "--no-deps", "-T", "gatekeeper", "generate", "--rate", "10",
         "--duration", "5", "--run-id", f"breaking{SESSION}",
         "--schema", "breaking/add-required-field.avsc"],
        cwd=ROOT, capture_output=True, text=True,
    )  # fmt: skip
    errors = [ln for ln in r.stderr.splitlines() if "RegistryError" in ln or "HTTP 4" in ln]
    refused = f"exit {r.returncode}: {errors[-1] if errors else r.stderr[-300:]}"
    summary = {
        "runs": len(out),
        "all_passed": all(r["passed"] for r in out),
        "breaking_refused_with_409": {
            name: all(r["breaking"][name]["register"]["status"] == 409 for r in out)
            for name in out[0]["breaking"]
        },
        "register_ms_median": med([b["register_ms"] for r in out for b in r["breaking"].values()]),
        "registry_reasons": {
            name: out[0]["breaking"][name]["messages"][0][:200] for name in out[0]["breaking"]
        },
        "live_producer_with_breaking_schema": refused,
    }
    save("contract", out, summary)


def exp_v2_rollout(rate: int) -> None:
    """Register v2 on the live subject, produce with it; the v1 gatekeeper must accept all."""
    settle()
    ct = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    reg = httpx.Client(base_url="http://localhost:18085", timeout=10, headers=ct)
    v2 = (ROOT / "contracts" / "clickstream" / "v2.avsc").read_text()
    r = reg.post("/subjects/clickstream.raw-value/versions", json={"schema": v2})
    g = Generator(f"v2roll{SESSION}", rate, 30, "--schema", "v2.avsc")
    g.start()
    g.join()
    rec = reconcile(f"v2roll{SESSION}")
    save("v2-rollout", [rec], {"registered": r.json(), "exact": rec["exact"],
                                "dlq": rec["layers"]["events.dlq"]})  # fmt: skip


# -------------------------------------------------------------------------- query
QUERIES = {
    "funnel_last_5m": """
        SELECT countIf(event_type = 'page_view') AS views,
               countIf(event_type = 'add_to_cart') AS carts,
               countIf(event_type = 'checkout_started') AS checkouts,
               countIf(event_type = 'payment_succeeded') AS paid,
               sumIf(amount_cents, event_type = 'payment_succeeded') / 100 AS revenue
        FROM lumen.events FINAL
        WHERE NOT is_probe AND event_time > now64(3) - INTERVAL 5 MINUTE""",
    "events_per_10s_last_15m": """
        SELECT window_start, event_type, events FROM lumen.window_counts FINAL
        WHERE window_start > now() - INTERVAL 15 MINUTE ORDER BY window_start""",
    "conversion_by_client_last_15m": """
        SELECT client, count() AS sessions, avg(reached_cart) AS cart_rate,
               avg(converted) AS conversion, sum(revenue_cents) / 100 AS revenue
        FROM lumen.sessions FINAL
        WHERE session_end > now() - INTERVAL 15 MINUTE GROUP BY client""",
    "top_products_last_5m": """
        SELECT product_id, count() AS adds FROM lumen.events FINAL
        WHERE event_type = 'add_to_cart' AND event_time > now64(3) - INTERVAL 5 MINUTE
        GROUP BY product_id ORDER BY adds DESC LIMIT 10""",
    "newest_event_age_s": """
        SELECT dateDiff('millisecond', max(event_time), now64(3)) / 1000
        FROM lumen.events WHERE NOT is_probe""",
}


def exp_query(runs: int, rate: int, repeats: int = 20) -> None:
    out: list[dict[str, Any]] = []
    for i in range(runs):
        settle()
        g = Generator(f"query{SESSION}x{i}", rate, 120)
        g.start()
        time.sleep(60)
        rows = int(ch_query("SELECT count() FROM lumen.events")[0])
        timings: dict[str, list[float]] = {k: [] for k in QUERIES}
        nofinal: list[float] = []
        for _ in range(repeats):
            for name, sql in QUERIES.items():
                timings[name].append(ch_query(sql + " FORMAT Null")[1])
            nofinal.append(ch_query(QUERIES["funnel_last_5m"].replace(" FINAL", "") +
                                    " FORMAT Null")[1])  # fmt: skip
        age = float(ch_query(QUERIES["newest_event_age_s"])[0])
        g.join()
        out.append({
            "table_rows": rows, "newest_event_age_s": age,
            "ms": {k: {"p50": med(v), "p95": round(sorted(v)[int(0.95 * len(v)) - 1], 1)}
                   for k, v in timings.items()},
            "funnel_without_final_p50_ms": med(nofinal),
        })  # fmt: skip
        print(i, out[-1]["ms"], file=sys.stderr)
    summary = {
        "runs": len(out),
        "table_rows": [r["table_rows"] for r in out],
        "p50_ms_median": {k: med([r["ms"][k]["p50"] for r in out]) for k in QUERIES},
        "p95_ms_median": {k: med([r["ms"][k]["p95"] for r in out]) for k in QUERIES},
        "funnel_without_final_p50_ms": med([r["funnel_without_final_p50_ms"] for r in out]),
    }
    save("query", out, summary)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("experiment", choices=["load", "broker-kill", "tm-kill", "stall",
                                           "contract", "v2-rollout", "query"])  # fmt: skip
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--rate", type=int, default=2000)
    p.add_argument("--rates", default="1000,3000,6000")
    p.add_argument("--duration", type=int, default=90)
    p.add_argument("--hold", type=int, default=150)
    a = p.parse_args()
    if a.experiment == "load":
        exp_load(a.runs, [int(r) for r in a.rates.split(",")], a.duration)
    elif a.experiment == "broker-kill":
        exp_broker_kill(a.runs, a.rate)
    elif a.experiment == "tm-kill":
        exp_tm_kill(a.runs, a.rate)
    elif a.experiment == "stall":
        exp_stall(a.runs, 500, a.hold)
    elif a.experiment == "contract":
        exp_contract(a.runs)
    elif a.experiment == "v2-rollout":
        exp_v2_rollout(1000)
    elif a.experiment == "query":
        exp_query(a.runs, a.rate)


if __name__ == "__main__":
    main()
