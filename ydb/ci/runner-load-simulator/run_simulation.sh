#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
DATA="${ROOT}/data/jobs_14d.json"
CAPACITY="${ROOT}/vendor/runner_capacity.yml"
REPORT="${ROOT}/data/simulation_report.json"

if [[ ! -f "$DATA" ]]; then
  echo "Collecting 14 days of self-hosted job history..."
  python3 "${ROOT}/collect_jobs.py" --days 14 --output "$DATA"
fi

echo "Running pool simulation (baseline vs pr_check_parallel)..."
python3 "${ROOT}/simulate.py" --data "$DATA" --capacity "$CAPACITY" --output "$REPORT"
