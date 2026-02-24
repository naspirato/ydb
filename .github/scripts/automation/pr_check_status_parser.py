#!/usr/bin/env python3
"""
Parse PR-check status comments from github-actions and build report.json links for Copilot.

Status comment format (see .github/scripts/tests/comment-pr.py and generate-summary.py). Example body: automation/run_ex.txt.
  <!-- status pr=PR_NUM, preset=PRESET, run=RUN_NUMBER -->
  :green_circle: / :red_circle: / :yellow_circle: ...
  Links like: .../PR-check/WORKFLOW_RUN_ID/ya-x86-64/try_1/ya-test.html ...

We replace ya-test.html with report.json and collect try_1, try_2, try_3 so Copilot
can analyze all tries. We need both build types (relwithdebinfo → ya-x86-64,
asan → ya-x86-64-asan) to know overall stage.

Autodebug comments we post use markup so the script can infer stage:
  <!-- autodebug-stage stage=STAGE pr=PR preset=PRESET run_id=WORKFLOW_RUN_ID -->
  STAGE is one of: verify | closure_request | checklist_request
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional, Set, Tuple

# Header written by comment-pr.py
STATUS_HEADER_RE = re.compile(
    r"<!--\s*status\s+pr=(\d+),\s*preset=([^,]+),\s*run=(\d+)\s*-->",
    re.IGNORECASE,
)
# Storage URL: .../PR-check/WORKFLOW_RUN_ID/ya-x86-64 or ya-x86-64-asan, then /try_N/...
STORAGE_TRY_RE = re.compile(
    r"https://storage\.yandexcloud\.net/ydb-gh-logs/[^/]+/[^/]+/PR-check/(\d+)/(ya-x86-64(?:-asan)?)/try_(\d+)/"
)
# Outcome: green = passed, red = failed (last retry). Ignore yellow (retrying/size warning).
OUTCOME_TEST_RE = re.compile(
    r":(green_circle|red_circle):"
)

# Our comment markup (so script can tell which stage a comment is for).
# Optional repost_no_reply=1 marks a re-post after "Copilot finished but no reply"; second such re-post triggers autodebug_broken.
AUTODEBUG_STAGE_HEADER_RE = re.compile(
    r"<!--\s*autodebug-stage\s+stage=(\w+)\s+pr=(\d+)\s+preset=([^\s]+)\s+run_id=(\d+)(?:\s+repost_no_reply=1)?\s*-->",
    re.IGNORECASE,
)
AUTODEBUG_STAGES = ("verify", "closure_request", "checklist_request")

# Track that we already added ok-to-test for this PR (run 1 = after first finish, run 2 = after closure_request); optional sha= for "no new commits" check
AUTODEBUG_OK_TO_TEST_ADDED_RE = re.compile(
    r"<!--\s*autodebug-ok-to-test-added\s+pr=(\d+)\s+run=(\d+)(?:\s+sha=([a-f0-9]+))?\s*-->",
    re.IGNORECASE,
)


def parse_ok_to_test_added_runs(comment_bodies: List[str], pr_number: int) -> Set[int]:
    """Return set of run numbers (1, 2, ...) for which we already posted ok-to-test-added for this PR."""
    runs: Set[int] = set()
    for body in comment_bodies:
        for m in AUTODEBUG_OK_TO_TEST_ADDED_RE.finditer(body or ""):
            if int(m.group(1)) == pr_number:
                runs.add(int(m.group(2)))
    return runs


def get_last_ok_to_test_head_sha(
    comment_bodies_oldest_to_newest: List[str], pr_number: int
) -> Optional[str]:
    """Return head SHA from the most recent ok-to-test-added comment for this PR, or None."""
    sha: Optional[str] = None
    for body in comment_bodies_oldest_to_newest:
        for m in AUTODEBUG_OK_TO_TEST_ADDED_RE.finditer(body or ""):
            if int(m.group(1)) == pr_number and m.group(3):
                sha = m.group(3)
    return sha


@dataclass
class BuildStatus:
    """Parsed status for one build (one preset / one comment)."""
    pr_number: int
    preset: str  # e.g. relwithdebinfo
    run_number: int  # from header (GITHUB_RUN_NUMBER)
    workflow_run_id: int  # from storage URL (github.run_id)
    path_suffix: str  # ya-x86-64 or ya-x86-64-asan
    outcome: str  # "green_circle" | "red_circle" | "yellow_circle"
    try_numbers: List[int]  # e.g. [1, 2, 3]
    report_json_urls: List[str] = field(default_factory=list)


def parse_status_comment(body: str) -> Optional[BuildStatus]:
    """
    Parse a single PR-check status comment from github-actions.
    Returns BuildStatus or None if not a status comment.
    """
    if not body or "<!-- status" not in body:
        return None
    m = STATUS_HEADER_RE.search(body)
    if not m:
        return None
    pr_number = int(m.group(1))
    preset = m.group(2).strip()
    run_number = int(m.group(3))

    # Find all try_N URLs to get workflow_run_id, path_suffix, and try numbers
    run_id: Optional[int] = None
    path_suffix: Optional[str] = None
    try_nums: List[int] = []
    for m2 in STORAGE_TRY_RE.finditer(body):
        run_id = int(m2.group(1))
        path_suffix = m2.group(2)
        try_nums.append(int(m2.group(3)))
    if run_id is None or path_suffix is None:
        return BuildStatus(
            pr_number=pr_number,
            preset=preset,
            run_number=run_number,
            workflow_run_id=0,
            path_suffix="",
            outcome="white_circle",
            try_numbers=[],
            report_json_urls=[],
        )

    try_nums = sorted(set(try_nums))
    base = f"https://storage.yandexcloud.net/ydb-gh-logs/ydb-platform/ydb/PR-check/{run_id}/{path_suffix}"
    report_json_urls = [f"{base}/try_{n}/report.json" for n in try_nums]

    # Last test outcome (green or red); ignore yellow_circle so "Build successful" + size warning still counts as green
    outcomes = OUTCOME_TEST_RE.findall(body)
    outcome = outcomes[-1] if outcomes else "white_circle"

    return BuildStatus(
        pr_number=pr_number,
        preset=preset,
        run_number=run_number,
        workflow_run_id=run_id,
        path_suffix=path_suffix,
        outcome=outcome,
        try_numbers=try_nums,
        report_json_urls=report_json_urls,
    )


def tests_passed(build_status: BuildStatus) -> bool:
    """True if this build ended with tests successful (green)."""
    return build_status.outcome == "green_circle"


def collect_latest_by_preset(
    comments: List[Tuple[int, str]],  # (comment_id, body)
) -> dict:
    """
    From a list of (comment_id, body) for a PR, return the latest status per preset.
    comments should be in chronological order (oldest first).
    Returns: { "relwithdebinfo": BuildStatus or None, "relwithdebinfo" or preset name: ... }
    """
    by_preset: dict = {}
    for _cid, body in comments:
        st = parse_status_comment(body)
        if st is None:
            continue
        # Normalize preset for key (e.g. linux-x86_64-relwithdebinfo -> relwithdebinfo)
        key = "relwithdebinfo" if "relwithdebinfo" in (st.preset or "").lower() else st.preset
        if "asan" in (st.preset or "").lower():
            key = "asan"
        by_preset[key] = st
    return by_preset


def all_builds_passed(by_preset: dict) -> bool:
    """True if we have at least one build and all of them passed (green)."""
    if not by_preset:
        return False
    return all(tests_passed(st) for st in by_preset.values())


def format_report_json_links_for_copilot(by_preset: dict) -> str:
    """
    Build a markdown block with report.json links for all presets and tries,
    to paste into a comment for Copilot.
    """
    lines = []
    for preset_key, st in sorted(by_preset.items()):
        if not st.report_json_urls:
            continue
        lines.append(f"**{preset_key}** (run_id={st.workflow_run_id}):")
        for url in st.report_json_urls:
            lines.append(f"- [report.json]({url})")
        lines.append("")
    return "\n".join(lines).strip()


def make_autodebug_stage_header(
    stage: str, pr_number: int, preset: str, workflow_run_id: int, repost_no_reply: bool = False
) -> str:
    """Build the hidden header so our script can detect which stage a comment is for."""
    if stage not in AUTODEBUG_STAGES:
        raise ValueError(f"stage must be one of {AUTODEBUG_STAGES}")
    suffix = " repost_no_reply=1" if repost_no_reply else ""
    return f"<!-- autodebug-stage stage={stage} pr={pr_number} preset={preset} run_id={workflow_run_id}{suffix} -->"


def parse_autodebug_stage_comment(body: str) -> Optional[Tuple[str, int, str, int, int]]:
    """
    If body is our autodebug-stage comment, return (stage, pr_number, preset, run_id, repost_no_reply).
    repost_no_reply is 1 if header has repost_no_reply=1 else 0.
    Otherwise None.
    """
    if not body or "autodebug-stage" not in body:
        return None
    m = AUTODEBUG_STAGE_HEADER_RE.search(body)
    if not m:
        return None
    repost = 1 if "repost_no_reply=1" in (body or "") else 0
    return (m.group(1).lower(), int(m.group(2)), m.group(3), int(m.group(4)), repost)


# Checklist block: <!-- autodebug-checklist --> key: value lines <!-- /autodebug-checklist -->
AUTODEBUG_CHECKLIST_BLOCK_RE = re.compile(
    r"<!--\s*autodebug-checklist\s*-->\s*(.*?)\s*<!--\s*/autodebug-checklist\s*-->",
    re.DOTALL | re.IGNORECASE,
)


# Placeholder values from our checklist_request template — do not treat as filled checklist
CHECKLIST_TEMPLATE_PLACEHOLDERS = ("area/<component>", "One-line summary")


def parse_checklist_from_body(body: str) -> Optional[dict]:
    """
    Extract checklist key-value pairs from body (e.g. area, resolution, summary).
    Returns dict with stripped keys/values or None if block missing, empty, or is our own template.
    """
    if not body or "autodebug-checklist" not in body:
        return None
    m = AUTODEBUG_CHECKLIST_BLOCK_RE.search(body)
    if not m:
        return None
    content = m.group(1).strip()
    if not content:
        return None
    result = {}
    for line in content.splitlines():
        line = line.strip()
        if ":" in line:
            k, v = line.split(":", 1)
            result[k.strip().lower()] = v.strip()
    if not result:
        return None
    # Ignore our own template (the example block in checklist_request comment)
    if result.get("area") in CHECKLIST_TEMPLATE_PLACEHOLDERS or result.get("summary") in CHECKLIST_TEMPLATE_PLACEHOLDERS:
        return None
    return result
