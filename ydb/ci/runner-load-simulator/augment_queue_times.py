#!/usr/bin/env python3
"""Backfill created_at / queue_wait_sec into an existing jobs JSON dump."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

REPO = "ydb-platform/ydb"
MAX_WORKERS = 12


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def gh_api(path: str) -> dict:
    out = subprocess.check_output(["gh", "api", path], stderr=subprocess.STDOUT, text=True)
    return json.loads(out)


def fetch_run_jobs(run_id: int) -> dict[int, dict]:
    result: dict[int, dict] = {}
    page = 1
    while True:
        data = gh_api(
            f"repos/{REPO}/actions/runs/{run_id}/jobs?per_page=100&page={page}"
        )
        chunk = data.get("jobs", [])
        for job in chunk:
            created = parse_ts(job.get("created_at"))
            started = parse_ts(job.get("started_at"))
            qw = None
            if created and started and started > created:
                qw = (started - created).total_seconds()
            result[job["id"]] = {
                "created_at": created.isoformat() if created else None,
                "queue_wait_sec": qw,
            }
        if len(chunk) < 100:
            break
        page += 1
    return result


def augment(data_path: Path) -> None:
    payload = json.loads(data_path.read_text(encoding="utf-8"))
    jobs = payload["jobs"]
    run_ids = sorted({j["run_id"] for j in jobs})
    print(f"fetching job timestamps for {len(run_ids)} runs...", file=sys.stderr)

    meta: dict[int, dict] = {}

    def worker(rid: int) -> tuple[int, dict[int, dict]]:
        return rid, fetch_run_jobs(rid)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(worker, rid) for rid in run_ids]
        for idx, fut in enumerate(as_completed(futures), 1):
            rid, job_map = fut.result()
            meta[rid] = job_map
            if idx % 200 == 0:
                print(f"  {idx}/{len(run_ids)}", file=sys.stderr)

    updated = 0
    for job in jobs:
        info = meta.get(job["run_id"], {}).get(job["job_id"])
        if not info:
            continue
        if info["created_at"]:
            job["created_at"] = info["created_at"]
        if info["queue_wait_sec"] is not None:
            job["queue_wait_sec"] = info["queue_wait_sec"]
            updated += 1

    payload["queue_augmented_at"] = datetime.now().astimezone().isoformat()
    data_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"updated queue_wait_sec on {updated}/{len(jobs)} jobs", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data",
        type=Path,
        default=Path(__file__).resolve().parent / "data" / "jobs_14d.json",
    )
    args = parser.parse_args()
    augment(args.data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
