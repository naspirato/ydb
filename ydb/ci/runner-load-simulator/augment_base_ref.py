#!/usr/bin/env python3
"""Resolve PR target branch (base_ref) from GitHub Pull Request API, not workflow runs."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

REPO = "ydb-platform/ydb"

ROOT = Path(__file__).resolve().parent
DEFAULT_DATA = ROOT / "data" / "jobs_14d.json"

MUTED_RE = re.compile(
    r"^update-muted-ya_(.+?)_(?:relwithdebinfo|release-asan|release-tsan|release-msan)$"
)
MERGE_STABLE_RE = re.compile(r"^(stable-[\w-]+)-merge-[0-9a-f]+$")
CHERRY_STABLE_RE = re.compile(r"^cherry-pick-(stable-[\w-]+)-")


@dataclass
class PrMeta:
    pr_number: int | None
    base_ref: str
    source: str


def gh_json(path: str) -> object:
    out = subprocess.check_output(
        ["gh", "api", path],
        text=True,
        stderr=subprocess.DEVNULL,
    )
    return json.loads(out)


def fetch_pr_by_number(pr_number: int) -> PrMeta:
    try:
        data = gh_json(f"repos/{REPO}/pulls/{pr_number}")
        return PrMeta(
            pr_number=int(data["number"]),
            base_ref=str(data["base"]["ref"]),
            source="pr_number",
        )
    except (subprocess.CalledProcessError, KeyError, TypeError, ValueError):
        return PrMeta(None, "", "missing")


def fetch_pr_by_head(head_branch: str) -> PrMeta:
    if not head_branch:
        return PrMeta(None, "", "missing")
    try:
        data = gh_json(
            f"repos/{REPO}/pulls"
            f"?head=ydb-platform:{head_branch}&state=all&per_page=1"
        )
        if not data:
            return PrMeta(None, "", "missing")
        pr = data[0]
        return PrMeta(
            pr_number=int(pr["number"]),
            base_ref=str(pr["base"]["ref"]),
            source="head_lookup",
        )
    except (subprocess.CalledProcessError, KeyError, TypeError, ValueError, IndexError):
        return PrMeta(None, "", "missing")


def infer_base_ref(head_branch: str) -> str:
    """Fallback when no PR exists in GitHub (bots, muted updates, merge branches)."""
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

    if head.startswith(("merge-main-", "merge-rightlib-")):
        return "main"

    if head.startswith("stable-") and "-merge-" not in head:
        return head.split("_")[0]

    return ""


def resolve_run_meta(
    *,
    pr_number: int | None,
    head_branch: str,
    pr_cache: dict[int, PrMeta],
    head_cache: dict[str, PrMeta],
) -> PrMeta:
    if pr_number:
        if pr_number not in pr_cache:
            pr_cache[pr_number] = fetch_pr_by_number(pr_number)
            time.sleep(0.03)
        meta = pr_cache[pr_number]
        if meta.base_ref:
            return meta

    head = head_branch or ""
    if head:
        if head not in head_cache:
            head_cache[head] = fetch_pr_by_head(head)
            time.sleep(0.04)
        meta = head_cache[head]
        if meta.base_ref:
            return meta

    inferred = infer_base_ref(head)
    if inferred:
        return PrMeta(pr_number, inferred, "inferred")

    return PrMeta(pr_number, "", "missing")


def augment(path: Path) -> dict[str, int]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    jobs = payload.get("jobs", [])

    run_info: dict[int, dict[str, object]] = {}
    for job in jobs:
        if job.get("workflow_name") != "PR-check":
            continue
        rid = int(job["run_id"])
        info = run_info.setdefault(
            rid,
            {"head_branch": job.get("head_branch") or "", "pr_number": job.get("pr_number")},
        )
        if job.get("pr_number") and not info.get("pr_number"):
            info["pr_number"] = job["pr_number"]
        if job.get("head_branch") and not info.get("head_branch"):
            info["head_branch"] = job["head_branch"]

    pr_cache: dict[int, PrMeta] = {}
    head_cache: dict[str, PrMeta] = {}
    run_meta: dict[int, PrMeta] = {}
    sources: dict[str, int] = {}

    for rid, info in run_info.items():
        meta = resolve_run_meta(
            pr_number=info.get("pr_number"),  # type: ignore[arg-type]
            head_branch=str(info.get("head_branch") or ""),
            pr_cache=pr_cache,
            head_cache=head_cache,
        )
        run_meta[rid] = meta
        sources[meta.source] = sources.get(meta.source, 0) + 1

    updated_jobs = 0
    for job in jobs:
        rid = int(job["run_id"])
        meta = run_meta.get(rid)
        if not meta:
            continue
        changed = False
        if meta.pr_number and job.get("pr_number") != meta.pr_number:
            job["pr_number"] = meta.pr_number
            changed = True
        if meta.base_ref and job.get("base_ref") != meta.base_ref:
            job["base_ref"] = meta.base_ref
            changed = True
        if meta.base_ref:
            job["base_ref_source"] = meta.source
        if changed:
            updated_jobs += 1

    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return {
        "pr_check_runs": len(run_info),
        "prs_cached": len(pr_cache),
        "heads_cached": len(head_cache),
        "jobs_updated": updated_jobs,
        **sources,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    args = parser.parse_args()
    stats = augment(args.data)
    print(json.dumps(stats, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
