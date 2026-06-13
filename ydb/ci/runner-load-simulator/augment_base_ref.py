#!/usr/bin/env python3
"""Backfill pull-request base branch (target) into collected jobs JSON."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

REPO = "ydb-platform/ydb"
ROOT = Path(__file__).resolve().parent
DEFAULT_DATA = ROOT / "data" / "jobs_14d.json"


def fetch_base_ref(pr_number: int) -> str:
    try:
        return subprocess.check_output(
            ["gh", "api", f"repos/{REPO}/pulls/{pr_number}", "--jq", ".base.ref"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except subprocess.CalledProcessError:
        return ""


def augment(path: Path) -> dict[str, int]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    cache: dict[int, str] = {}
    updated = 0
    for job in payload.get("jobs", []):
        pr = job.get("pr_number")
        if not pr:
            continue
        if pr not in cache:
            cache[pr] = fetch_base_ref(pr)
            time.sleep(0.03)
        base_ref = cache[pr]
        if job.get("base_ref") != base_ref:
            job["base_ref"] = base_ref
            updated += 1
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return {"prs": len(cache), "jobs_updated": updated}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    args = parser.parse_args()
    stats = augment(args.data)
    print(json.dumps(stats, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
