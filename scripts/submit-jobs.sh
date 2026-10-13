#!/usr/bin/env bash
# Submit each Flink SQL job in flink/sql unless a job of that name is already running.
set -euo pipefail
FLINK=${FLINK_URL:-http://localhost:18081}

running() {
  curl -sf "$FLINK/jobs/overview" |
    python3 -c 'import json,sys; print("\n".join(j["name"] for j in json.load(sys.stdin)["jobs"] if j["state"] in ("RUNNING","CREATED","RESTARTING","INITIALIZING")))'
}

for f in flink/sql/[0-9]*.sql; do
  name=$(sed -nE "s/^SET 'pipeline.name' = '([^']+)';/\1/p" "$f")
  if running | grep -qx "$name"; then
    echo "job '$name' already running"
    continue
  fi
  echo "submitting $f as '$name'"
  docker compose exec -T jobmanager ./bin/sql-client.sh -f "/opt/flink/sql/$(basename "$f")" >/tmp/sp-sql-client.log 2>&1 ||
    { cat /tmp/sp-sql-client.log; exit 1; }
  if grep -q "\[ERROR\]" /tmp/sp-sql-client.log; then
    grep -A3 "\[ERROR\]" /tmp/sp-sql-client.log >&2
    exit 1
  fi
  grep -E "Job ID" /tmp/sp-sql-client.log || true
done

# Wait until every job reports RUNNING with all tasks deployed.
for _ in $(seq 1 60); do
  states=$(curl -sf "$FLINK/jobs/overview" | python3 -c '
import json, sys
jobs = [j for j in json.load(sys.stdin)["jobs"] if j["state"] not in ("CANCELED", "FINISHED", "FAILED")]
ok = jobs and all(j["state"] == "RUNNING" and j["tasks"]["running"] == j["tasks"]["total"] for j in jobs)
print("ok" if ok else " ".join(f"{j['"'"'name'"'"']}={j['"'"'state'"'"']}" for j in jobs))')
  [ "$states" = ok ] && { echo "all jobs running"; exit 0; }
  sleep 2
done
echo "jobs not running: $states" >&2
exit 1
