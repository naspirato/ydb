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

echo "Analyzing PR push→result cycles..."
python3 "${ROOT}/analyze_cycles.py" --data "$DATA"

echo "Generating charts and team report..."
python3 "${ROOT}/generate_charts.py" --data "$DATA" --report "$REPORT"

echo "Augmenting historical queue times (GitHub created→started)..."
python3 "${ROOT}/augment_queue_times.py" --data "$DATA"

echo "Queue timeline (10-min workday profiles)..."
python3 "${ROOT}/queue_timeline.py" --data "$DATA"

echo "Done. Open ${ROOT}/data/TEAM_REPORT.md and ${ROOT}/data/charts/"
