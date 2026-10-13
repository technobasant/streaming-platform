# 7. A freshness SLO measured by probes, alerted on burn rate

Status: accepted · 2026-10

## Context

The promise to people querying ClickHouse is about freshness: what happened a few
seconds ago is queryable. The obvious signal — `now() - max(event_time)` in the table —
alerts on every quiet period and says nothing when late events keep the maximum old.

## Decision

* **SLI**: the share of probe events that become queryable in ClickHouse within 15 s
  of their event time. The exporter sends one every 5 s through the front door
  (`clickstream.raw`, valid Avro, `is_probe=true`), watches ClickHouse for it, and
  resolves it once: good if it landed in time, bad when 15 s pass without it.
* **SLO**: 99%. Probes are excluded from every business table and from the lake.
* **Alerts**: multi-window, multi-burn-rate from the SRE Workbook — 14.4× over a
  long and a short window, and 6× over another pair. The production pairs (1 h/5 m,
  6 h/30 m) are divided by 12 so a stall pages in about a minute in a demo; the
  rule file says so.

## Consequences

* A probe measures the whole path, gate and both Flink transactions included, and
  keeps measuring at zero traffic. It does not measure each shopper event; per-event
  latency comes from the reconciler (`streamplat latency`).
* With no probes resolved, the ratio is undefined and the burn alerts cannot fire;
  `FreshnessProbeSilent` covers a dead exporter.
* No Alertmanager: alerts are read from Prometheus. Routing and paging are out of
  scope.
