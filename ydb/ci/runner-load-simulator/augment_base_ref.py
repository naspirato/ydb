#!/usr/bin/env python3
"""Backfill PR target branch (base ref) into collected jobs JSON."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path

REPO = "ydb-platform/ydb"

ROOT = Path(__file__).resolve().parent
DEFAULT_DATA = ROOT / "data" / "jobs_14d.json"

MUTED_RE = re.compile(
    r"^update-muted-ya_(.+?)_(?:relwithdebinfo|release-asan|release-tsan|release-msan)$"
)
MERGE_STABLE_RE = re.compile(r"^(stable-[\w-]+)-merge-[0-9a-f]+$")
CHERRY_STABLE_RE = re.compile(r"^cherry-pick-(stable-[\w-]+)-")


def fetch_base_ref_by_pr(pr_number: int) -> str:
    try:
        return subprocess.check_output(
            ["gh", "api", f"repos/{REPO}/pulls/{pr_number}", "--jq", ".base.ref"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except subprocess.CalledProcessError:
        return ""


def fetch_base_ref_by_head(head_branch: str) -> str:
    if not head_branch:
        return ""
    try:
        return subprocess.check_output(
            [
                "gh",
                "api",
                f"repos/{REPO}/pulls?head=ydb-platform:{head_branch}&state=all&per_page=1",
                "--jq",
                ".[0].base.ref // empty",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except subprocess.CalledProcessError:
        return ""


def infer_base_ref(head_branch: str) -> str:
    """Best-effort target branch from head name when PR metadata is missing."""
    head = head_branch or ""
    if not head:
        return ""

    muted = MUTED_RE.match(head)
    if muted:
        return muted.group(1)

    merge = MERGE_STABLE_RE.match(head)
    if merge:
        return merge.group(1)

    cherry = CHERRY_STABLE_RE.match(head)
    if cherry:
        return cherry.group(1)

    if head in {"main", "master"}:
        return "main"

    if head.startswith("merge-main-"):
        return "main"

    if head.startswith("merge-rightlib-"):
        return "main"

    if head.startswith("stable-") and "-merge-" not in head:
        return head.split("_")[0]

    return ""


def resolve_base_ref(
    job: dict,
    *,
    pr_cache: dict[int, str],
    head_cache: dict[str, str],
    use_head_lookup: bool,
) -> str:
    existing = job.get("base_ref") or ""
    if existing:
        return existing

    pr_number = job.get("pr_number")
    if pr_number:
        if pr_number not in pr_cache:
            pr_cache[pr_number] = fetch_base_ref_by_pr(pr_number)
            time.sleep(0.03)
        if pr_cache[pr_number]:
            return pr_cache[pr_number]

    head = job.get("head_branch") or ""
    inferred = infer_base_ref(head)
    if inferred:
        return inferred

    if not use_head_lookup or not head:
        return ""

    if head not in head_cache:
        head_cache[head] = fetch_base_ref_by_head(head)
        time.sleep(0.04)
    return head_cache[head]


def augment(path: Path, *, use_head_lookup: bool = True) -> dict[str, int]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    pr_cache: dict[int, str] = {}
    head_cache: dict[str, str] = {}
    updated = 0
    sources = {"kept": 0, "pr": 0, "infer": 0, "head_lookup": 0, "empty": 0}

    for job in payload.get("jobs", []):
        before = job.get("base_ref") or ""
        if before:
            sources["kept"] += 1
            continue

        head = job.get("head_branch") or ""
        inferred = infer_base_ref(head)
        if job.get("pr_number"):
            resolved = resolve_base_ref(
                job, pr_cache=pr_cache, head_cache=head_cache, use_head_lookup=False
            )
            source = "pr"
        elif inferred:
            resolved = inferred
            source = "infer"
        elif use_head_lookup:
            resolved = resolve_base_ref(
                job, pr_cache=pr_cache, head_cache=head_cache, use_head_lookup=True
            )
            source = "head_lookup" if resolved else "empty"
        else:
            resolved = ""
            source = "empty"

        if resolved:
            job["base_ref"] = resolved
            if resolved != before:
                updated += 1
            sources[source] += 1
        else:
            sources["empty"] += 1

    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return {
        "prs_cached": len(pr_cache),
        "heads_cached": len(head_cache),
        "jobs_updated": updated,
        **sources,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument(
        "--no-head-lookup",
        action="store_true",
        help="Skip GitHub pulls?head= lookup for unknown feature branches",
    )
    args = parser.parse_args()
    stats = augment(args.data, use_head_lookup=not args.no_head_lookup)
    print(json.dumps(stats, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
