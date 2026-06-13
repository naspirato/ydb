#!/usr/bin/env python3
"""Collect self-hosted workflow job intervals from GitHub Actions (last N days)."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO = "ydb-platform/ydb"

WORKFLOW_NAMES = (
    "PR-check",
    "Postcommit_asan",
    "Postcommit_relwithdebinfo",
    "Nightly-Build",
    "Run-tests",
    "Regression-run",
    "Regression-run_Large",
    "Regression-run_Small_and_Medium",
    "Regression-run_stress",
    "Regression-run_compatibility",
    "Regression-whitelist-run",
    "Collect-analytics-run",
    "Collect-analytics-fast-run",
    "Run and debug tests",
    "Compare-ydb-configs-in-branches",
    "Update Muted tests",
    "Publish docker image",
    "Prewarm-Ccache",
)

MEANINGFUL_CONCLUSIONS = {"success", "failure", "timed_out"}
MAX_WORKERS = 12


def gh_api(path: str, *, retries: int = 5) -> Any:
    for attempt in range(retries):
        try:
            out = subprocess.check_output(
                ["gh", "api", path],
                stderr=subprocess.STDOUT,
                text=True,
            )
            return json.loads(out)
        except subprocess.CalledProcessError as exc:
            if attempt + 1 == retries:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError("unreachable")


def load_workflow_ids() -> dict[str, int]:
    data = gh_api(f"repos/{REPO}/actions/workflows?per_page=100")
    by_name = {w["name"]: w["id"] for w in data["workflows"]}
    missing = [name for name in WORKFLOW_NAMES if name not in by_name]
    if missing:
        print(f"warning: workflows not found: {missing}", file=sys.stderr)
    return {name: by_name[name] for name in WORKFLOW_NAMES if name in by_name}


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def preset_label(labels: list[str]) -> str | None:
    for label in labels:
        if label.startswith("build-preset-"):
            return label
    return None


def is_auto_provisioned(labels: list[str]) -> bool:
    lowered = {x.lower() for x in labels}
    return "self-hosted" in lowered and "auto-provisioned" in lowered


def iter_runs(workflow_id: int, since_iso: str):
    page = 1
    while True:
        data = gh_api(
            f"repos/{REPO}/actions/workflows/{workflow_id}/runs"
            f"?created=>={since_iso}&per_page=100&page={page}"
        )
        runs = data.get("workflow_runs", [])
        if not runs:
            break
        for run in runs:
            yield run
        if len(runs) < 100:
            break
        page += 1


def fetch_jobs(run: dict[str, Any]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    page = 1
    while True:
        data = gh_api(
            f"repos/{REPO}/actions/runs/{run['id']}/jobs?per_page=100&page={page}"
        )
        chunk = data.get("jobs", [])
        jobs.extend(chunk)
        if len(chunk) < 100:
            break
        page += 1
    return jobs


def normalize_job(
    job: dict[str, Any],
    *,
    workflow_name: str,
    run: dict[str, Any],
) -> dict[str, Any] | None:
    labels = job.get("labels") or []
    if not is_auto_provisioned(labels):
        return None
    preset = preset_label(labels)
    if not preset:
        return None
    started = parse_ts(job.get("started_at"))
    completed = parse_ts(job.get("completed_at"))
    if not started or not completed or completed <= started:
        return None
    pr_numbers = [pr["number"] for pr in run.get("pull_requests") or []]
    return {
        "job_id": job["id"],
        "run_id": run["id"],
        "workflow_name": workflow_name,
        "job_name": job.get("name") or "",
        "preset": preset,
        "status": job.get("status"),
        "conclusion": job.get("conclusion"),
        "started_at": started.isoformat(),
        "completed_at": completed.isoformat(),
        "duration_sec": (completed - started).total_seconds(),
        "runner_name": job.get("runner_name") or "",
        "pr_number": pr_numbers[0] if pr_numbers else None,
        "run_conclusion": run.get("conclusion"),
        "head_branch": run.get("head_branch") or "",
    }


def collect(days: int, output: Path) -> dict[str, Any]:
    since = datetime.now(timezone.utc) - timedelta(days=days)
    since_iso = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    workflow_ids = load_workflow_ids()

    jobs: list[dict[str, Any]] = []
    stats = Counter()
    pending_runs: list[tuple[str, dict[str, Any]]] = []

    for workflow_name, workflow_id in workflow_ids.items():
        print(f"listing runs: {workflow_name}", flush=True)
        for run in iter_runs(workflow_id, since_iso):
            stats["runs_seen"] += 1
            if run.get("status") != "completed":
                stats["runs_skipped_incomplete"] += 1
                continue
            if run.get("conclusion") not in MEANINGFUL_CONCLUSIONS:
                stats["runs_skipped_conclusion"] += 1
                continue
            pending_runs.append((workflow_name, run))

    print(f"fetching jobs for {len(pending_runs)} runs with {MAX_WORKERS} workers", flush=True)

    def worker(item: tuple[str, dict[str, Any]]) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
        workflow_name, run = item
        return workflow_name, run, fetch_jobs(run)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(worker, item) for item in pending_runs]
        for idx, future in enumerate(as_completed(futures), start=1):
            workflow_name, run, run_jobs = future.result()
            stats["runs_with_jobs"] += 1
            for job in run_jobs:
                normalized = normalize_job(job, workflow_name=workflow_name, run=run)
                if normalized:
                    jobs.append(normalized)
                    stats["jobs_kept"] += 1
                else:
                    stats["jobs_dropped"] += 1
            if idx % 200 == 0:
                print(f"  processed {idx}/{len(pending_runs)} runs, jobs={len(jobs)}", flush=True)

    payload = {
        "repo": REPO,
        "since": since_iso,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "workflow_names": list(workflow_ids.keys()),
        "stats": dict(stats),
        "jobs": jobs,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload["stats"], indent=2), flush=True)
    print(f"wrote {len(jobs)} jobs to {output}", flush=True)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent / "data" / "jobs_14d.json",
    )
    args = parser.parse_args()
    collect(args.days, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
