#!/usr/bin/env python3
"""
Create GitHub issues for PR-check blocked failures grouped by owner and suite.

Reads from the data mart test_results/analytics/pr_blocked_by_failed_tests_rich_with_pr_and_mute,
groups rows by (owner_today, suite_folder), and creates one issue per group. Inside each issue,
failures are listed by test: for each test we show which branches are affected (e.g. test_1 on main,
test_2 on stable-25-4 and stable-25-4-1) with run/stderr/PR links and error excerpts per branch.

Usage:
  # Filter by owner (one command for one team: dry-run or create):
  python3 ... create_issues_pr_blocked_by_suite.py --owner "TEAM:@ydb-platform/engineering" --dry-run
  python3 ... create_issues_pr_blocked_by_suite.py --owner "TEAM:@ydb-platform/engineering" --execute
  # Preview then create in one run:
  python3 ... create_issues_pr_blocked_by_suite.py --dry-run --execute [--owner OWNER]
  # Assign Copilot (SWE agent) to an existing issue via agent_assignment API:
  python3 ... create_issues_pr_blocked_by_suite.py --assign-issue 34730 [--base-branch main]
  # Add label 'ok-to-test' to PRs linked to autodebug issues (prefer Copilot's PR):
  python3 ... create_issues_pr_blocked_by_suite.py --add-ok-to-test [--dry-run]
  # Check issues: assign Copilot if missing; for linked PRs: add autodebug when PR found, ok-to-test after finish, verify/closure/checklist comments, mark Ready after checklist:
  python3 .github/scripts/automation/create_issues_pr_blocked_by_suite.py --check-issues [--dry-run]
  # Check a single issue only:
  python3 .github/scripts/automation/create_issues_pr_blocked_by_suite.py --check-issues --issue 34744 [--dry-run]

Requires: CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS (for YDB). GITHUB_TOKEN when --execute or when creating without --dry-run.
"""

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

# Copilot SWE agent bot login for assignees API with agent_assignment.
# See: gh api .../issues/ISSUE_NUMBER/assignees with agent_assignment body.
COPILOT_SWE_AGENT_BOT = "copilot-swe-agent[bot]"

# Script lives in .github/scripts/automation/; ydb_wrapper is in sibling analytics/
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_SCRIPT_DIR, "..", "analytics"))
from ydb_wrapper import YDBWrapper
sys.path.insert(0, _SCRIPT_DIR)
from pr_check_status_parser import (
    all_builds_passed,
    collect_latest_by_preset,
    format_report_json_links_for_copilot,
    get_last_ok_to_test_head_sha,
    make_autodebug_stage_header,
    parse_autodebug_stage_comment,
    parse_checklist_from_body,
    parse_ok_to_test_added_runs,
    parse_status_comment,
)

# Repo root
REPO_ROOT = os.path.normpath(os.path.join(_SCRIPT_DIR, "..", "..", ".."))
OWNER_AREA_MAPPING_PATH = os.path.join(REPO_ROOT, ".github", "config", "owner_area_mapping.json")
STABLE_TESTS_BRANCHES_PATH = os.path.join(REPO_ROOT, ".github", "config", "stable_tests_branches.json")
AUTOMATION_PROMPTS_DIR = os.path.join(_SCRIPT_DIR, "prompts")

# Mart table path (same as in collect_analytics_fast.yml)
MART_TABLE = "test_results/analytics/pr_blocked_by_failed_tests_rich_with_pr_and_mute"
GITHUB_MAX_BODY_LENGTH = 65000
OWNER = "ydb-platform"
REPO = "ydb"
DEFAULT_LABELS = ["copilot-investigate", "pr-blocked", "autodebug"]
AUTODEBUG_LABEL = "autodebug"  # used for --check-issues / --add-ok-to-test (filter by label) and for dedupe when creating
AUTODEBUG_OWNER_LABEL_PREFIX = "autodebug-owner-"  # dedup: issue is about this owner (slug after prefix)
AUTODEBUG_SUITE_LABEL_PREFIX = "autodebug-suite-"  # dedup: issue is about this suite (slug after prefix)
GITHUB_LABEL_MAX_LEN = 50
DEFAULT_ASSIGNEE = "copilot"  # assign created issues to Copilot by default
OK_TO_TEST_LABEL = "ok-to-test"  # label to add to Copilot PRs in --add-ok-to-test / --check-issues
# After a call to Copilot: if no work started in this many minutes, delete our comment and re-post
COPILOT_WAIT_MINUTES = 15
# If Copilot finished but didn't reply, we re-ask once; if still no reply after that, set this label
AUTODEBUG_BROKEN_LABEL = "autodebug_broken"
# Max concurrent autodebug issues/PRs: don't create new issues if this many open; only work in this many PRs at a time in --check-issues
MAX_CONCURRENT_AUTODEBUG = 15
# Comment author logins that count as "Copilot replied" (PR comments)
COPILOT_COMMENT_AUTHORS = frozenset({"copilot-swe-agent[bot]", "github-copilot[bot]", "Copilot"})


def _github_request(
    method: str,
    url: str,
    token: str,
    data: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
) -> tuple[int, str]:
    """Perform HTTP request to GitHub API. Returns (status_code, body)."""
    h = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if headers:
        h.update(headers)
    if data is not None:
        h.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.fp.read().decode("utf-8", errors="replace") if e.fp else ""
        return e.code, body


def get_issue_timeline(owner: str, repo_name: str, issue_number: int, token: str) -> List[Dict[str, Any]]:
    """Get timeline events for an issue. Returns list of event dicts."""
    all_events: List[Dict[str, Any]] = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo_name}/issues/{issue_number}/timeline?per_page=100&page={page}"
        status, body = _github_request("GET", url, token)
        if status != 200:
            return all_events
        try:
            events = json.loads(body)
        except json.JSONDecodeError:
            return all_events
        if not events:
            break
        all_events.extend(events)
        if len(events) < 100:
            break
        page += 1
    return all_events


def _parse_iso_dt(s: Optional[str]):
    """Parse ISO timestamp to timezone-aware datetime (UTC). Returns None if invalid."""
    if not s:
        return None
    try:
        # GitHub API returns e.g. 2026-02-23T14:54:00Z
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _comment_dt(c) -> Optional[datetime]:
    """Get comment created_at as timezone-aware datetime."""
    created = getattr(c, "created_at", None)
    if created is None:
        return None
    if getattr(created, "tzinfo", None) is None:
        created = created.replace(tzinfo=timezone.utc)
    return created


def get_our_stage_comments(
    comments_sorted: List[Any], pr_number: int
) -> Dict[str, List[Tuple[int, datetime, int]]]:
    """
    Return our autodebug-stage comments per stage: stage -> [(comment_id, created_at, repost_no_reply), ...].
    List is ordered oldest first (latest is last). Only comments for this PR are included.
    """
    out: Dict[str, List[Tuple[int, datetime, int]]] = {}
    for c in comments_sorted:
        parsed = parse_autodebug_stage_comment(c.body or "")
        if not parsed:
            continue
        stage, pr, _preset, _run_id, repost_no_reply = parsed
        if pr != pr_number:
            continue
        dt = _comment_dt(c)
        if dt is None:
            continue
        out.setdefault(stage, []).append((c.id, dt, repost_no_reply))
    for stage in out:
        out[stage].sort(key=lambda x: x[1])
    return out


def get_copilot_finished_after(timeline: List[Dict[str, Any]], after_dt: datetime) -> bool:
    """True if there is a copilot_work_finished (or copilot_work_started) event after after_dt."""
    for e in timeline:
        if (e.get("event") or "") not in ("copilot_work_finished", "copilot_work_started"):
            continue
        ev_dt = _parse_iso_dt(e.get("created_at"))
        if ev_dt is not None and ev_dt > after_dt:
            return True
    return False


def copilot_replied_after(comments_sorted: List[Any], after_dt: datetime) -> bool:
    """True if there is a comment from a Copilot bot after after_dt."""
    for c in comments_sorted:
        created = _comment_dt(c)
        if created is None or created <= after_dt:
            continue
        try:
            login = getattr(getattr(c, "user", None), "login", None)
        except Exception:
            login = None
        if login and str(login) in COPILOT_COMMENT_AUTHORS:
            return True
    return False


def copilot_replied_with_checklist_after(comments_sorted: List[Any], after_dt: datetime) -> bool:
    """
    True if there is a comment from a Copilot bot after after_dt that contains a valid filled checklist.
    Use for checklist_request stage: error messages like "Unfortunately I hit an unexpected error..."
    do not count as a reply; we re-post until we get a real checklist block.
    """
    for c in comments_sorted:
        created = _comment_dt(c)
        if created is None or created <= after_dt:
            continue
        try:
            login = getattr(getattr(c, "user", None), "login", None)
        except Exception:
            login = None
        if not login or str(login) not in COPILOT_COMMENT_AUTHORS:
            continue
        if parse_checklist_from_body(c.body or ""):
            return True
    return False


def delete_issue_comment(owner: str, repo_name: str, comment_id: int, token: str) -> bool:
    """Delete an issue/PR comment. Returns True on success."""
    url = f"https://api.github.com/repos/{owner}/{repo_name}/issues/comments/{comment_id}"
    status, _ = _github_request("DELETE", url, token)
    return status == 204


def get_linked_pr_numbers_via_graphql(owner: str, repo_name: str, issue_number: int, token: str) -> List[int]:
    """
    Get PR numbers linked to this issue (that will close it) via GraphQL closedByPullRequestsReferences.
    This matches the "linked pull request" shown in the issue's Development section (including Copilot-linked PRs).
    """
    query = """
    query($owner: String!, $repo: String!, $issueNum: Int!) {
      repository(owner: $owner, name: $repo) {
        issue(number: $issueNum) {
          closedByPullRequestsReferences(first: 10) {
            nodes { number }
          }
        }
      }
    }
    """
    payload = {
        "query": query,
        "variables": {"owner": owner, "repo": repo_name, "issueNum": issue_number},
    }
    data = json.dumps(payload).encode("utf-8")
    status, body = _github_request("POST", "https://api.github.com/graphql", token, data=data)
    if status != 200:
        return []
    try:
        out = json.loads(body)
        errs = out.get("errors")
        if errs:
            return []
        repo = (out.get("data") or {}).get("repository") or {}
        issue = repo.get("issue")
        if not issue:
            return []
        refs = (issue.get("closedByPullRequestsReferences") or {}).get("nodes") or []
        return [int(n["number"]) for n in refs if isinstance(n, dict) and n.get("number") is not None]
    except (json.JSONDecodeError, TypeError, ValueError):
        return []


def get_pull_request_head_ref(owner: str, repo_name: str, pr_number: int, token: str) -> str:
    """Get head ref (branch name) of a PR. Returns empty string on error."""
    url = f"https://api.github.com/repos/{owner}/{repo_name}/pulls/{pr_number}"
    status, body = _github_request("GET", url, token)
    if status != 200:
        return ""
    try:
        data = json.loads(body)
        head = data.get("head") or {}
        return (head.get("ref") or "").strip()
    except (json.JSONDecodeError, TypeError):
        return ""


def add_labels_to_issue_or_pr(owner: str, repo_name: str, number: int, labels: List[str], token: str) -> bool:
    """Add labels to an issue or PR. Returns True on success."""
    url = f"https://api.github.com/repos/{owner}/{repo_name}/issues/{number}/labels"
    data = json.dumps({"labels": labels}).encode("utf-8")
    status, body = _github_request("POST", url, token, data=data)
    if status not in (200, 201):
        print(f"Failed to add labels to #{number}: {status} {body[:400]}", file=sys.stderr)
        return False
    return True


def get_issue_body(owner: str, repo_name: str, issue_number: int, token: str) -> str:
    """Get issue body. Returns empty string on error."""
    url = f"https://api.github.com/repos/{owner}/{repo_name}/issues/{issue_number}"
    status, body = _github_request("GET", url, token)
    if status != 200:
        return ""
    try:
        data = json.loads(body)
        return (data.get("body") or "") or ""
    except json.JSONDecodeError:
        return ""


def get_pr_details(owner: str, repo_name: str, pr_number: int, token: str) -> Optional[Dict[str, Any]]:
    """Get PR details (draft, changed_files, etc.). Returns None on error."""
    url = f"https://api.github.com/repos/{owner}/{repo_name}/pulls/{pr_number}"
    status, body = _github_request("GET", url, token)
    if status != 200:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


def get_pr_labels(owner: str, repo_name: str, pr_number: int, token: str) -> List[str]:
    """Get label names for a PR (issues endpoint). Returns empty list on error."""
    url = f"https://api.github.com/repos/{owner}/{repo_name}/issues/{pr_number}"
    status, body = _github_request("GET", url, token)
    if status != 200:
        return []
    try:
        data = json.loads(body)
        labels = data.get("labels") or []
        return [lb.get("name") or "" for lb in labels if isinstance(lb, dict) and lb.get("name")]
    except (json.JSONDecodeError, TypeError):
        return []


def mark_pr_ready_for_review_graphql(owner: str, repo_name: str, pr_number: int, token: str) -> bool:
    """Mark draft PR as ready for review via GraphQL mutation. Returns True on success."""
    # Get PR node id
    query_id = """
    query($owner: String!, $repo: String!, $prNum: Int!) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $prNum) { id }
      }
    }
    """
    payload = {
        "query": query_id,
        "variables": {"owner": owner, "repo": repo_name, "prNum": pr_number},
    }
    data = json.dumps(payload).encode("utf-8")
    status, body = _github_request("POST", "https://api.github.com/graphql", token, data=data)
    if status != 200:
        return False
    try:
        out = json.loads(body)
        if out.get("errors"):
            return False
        repo = (out.get("data") or {}).get("repository")
        pr = (repo or {}).get("pullRequest")
        pr_id = (pr or {}).get("id")
        if not pr_id:
            return False
    except (json.JSONDecodeError, TypeError, KeyError):
        return False
    mutation = """
    mutation($prId: ID!) {
      markPullRequestReadyForReview(input: { pullRequestId: $prId }) {
        pullRequest { isDraft }
      }
    }
    """
    payload = {"query": mutation, "variables": {"prId": pr_id}}
    data = json.dumps(payload).encode("utf-8")
    status, body = _github_request("POST", "https://api.github.com/graphql", token, data=data)
    if status != 200:
        return False
    try:
        out = json.loads(body)
        return not out.get("errors") and bool((out.get("data") or {}).get("markPullRequestReadyForReview"))
    except (json.JSONDecodeError, TypeError):
        return False


def request_pr_reviewers(owner: str, repo_name: str, pr_number: int, reviewers: List[str], token: str) -> bool:
    """Request reviewers on a PR. Returns True on success."""
    url = f"https://api.github.com/repos/{owner}/{repo_name}/pulls/{pr_number}/requested_reviewers"
    data = json.dumps({"reviewers": reviewers}).encode("utf-8")
    status, body = _github_request("POST", url, token, data=data)
    if status not in (200, 201):
        print(f"Failed to request reviewers on PR #{pr_number}: {status} {body[:400]}", file=sys.stderr)
        return False
    return True


def add_copilot_assignee_via_agent_assignment(
    owner: str,
    repo_name: str,
    issue_number: int,
    token: str,
    base_branch: str = "main",
) -> bool:
    """
    Assign Copilot (SWE agent) to an issue via POST .../issues/N/assignees
    with body: assignees + agent_assignment (target_repo, base_branch, etc.).
    Returns True on success.
    """
    target_repo = f"{owner}/{repo_name}"
    payload = {
        "assignees": [COPILOT_SWE_AGENT_BOT],
        "agent_assignment": {
            "target_repo": target_repo,
            "base_branch": base_branch,
            "custom_instructions": "",
            "custom_agent": "",
            "model": "",
        },
    }
    url = f"https://api.github.com/repos/{owner}/{repo_name}/issues/{issue_number}/assignees"
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
    }
    req = urllib.request.Request(url, data=data, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req) as resp:
            return 200 <= resp.status < 300
    except urllib.error.HTTPError as e:
        print(f"Failed to assign Copilot to issue #{issue_number}: {e.code} {e.reason}", file=sys.stderr)
        if e.fp:
            try:
                print(e.fp.read().decode("utf-8", errors="replace")[:800], file=sys.stderr)
            except Exception:
                pass
        return False


def _row_to_dict(row: Any, column_names: List[str]) -> Dict[str, Any]:
    """Convert YDB row to plain dict using column names (order matches SELECT)."""
    if hasattr(row, "items") and callable(getattr(row, "items")):
        return {k: v for k, v in row.items() if k in column_names}
    # Row is often indexable by position or name
    try:
        return {name: row[name] for name in column_names}
    except (TypeError, KeyError):
        pass
    try:
        return dict(zip(column_names, row))
    except Exception:
        return dict(zip(column_names, [getattr(row, name, None) for name in column_names]))


def _parse_stable_version(branch: str) -> Optional[tuple]:
    """Parse stable-X-Y[-Z...] into tuple of ints, e.g. stable-25-4-1 -> (25, 4, 1). Returns None if not stable*."""
    if not branch or not branch.startswith("stable-"):
        return None
    parts = branch.split("-")[1:]
    try:
        return tuple(int(p) for p in parts)
    except ValueError:
        return None


def _base_branch_from_branches(branches: List[str]) -> str:
    """Pick base_branch for agent_assignment: main if present; else senior stable (base over suffix, newer over older); else main."""
    branches = [b.strip() for b in branches if b and b.strip()]
    if "main" in branches:
        return "main"
    stable = [b for b in branches if b.startswith("stable")]
    if not stable:
        return next(iter(branches), "main") or "main"
    # Senior stable: prefer suffix over base (stable-25-4-1 over stable-25-4), newer over older (25-4 over 25-3).
    # Sort key: (-len(tuple), -major, -minor, ...) so longer tuple first, then version descending.
    def key(b: str):
        t = _parse_stable_version(b)
        if t is None:
            return (-999, (0,))
        return (-len(t), tuple(-x for x in t))
    stable_sorted = sorted(stable, key=key)
    return stable_sorted[0]


def _base_branch_from_group_rows(rows: List[Dict[str, Any]]) -> str:
    """Pick base_branch for agent_assignment from mart rows (uses _base_branch_from_branches)."""
    branches = [_str(r.get("branch")).strip() for r in rows if r.get("branch")]
    return _base_branch_from_branches(branches)


def _parse_branches_from_issue_body(body: str) -> List[str]:
    """Extract branch names from issue body: find 'Branches affected:' and backtick-wrapped names. Returns [] if not found."""
    if not body:
        return []
    # Match "Branches affected:" then capture all `...` tokens on the same line or following
    idx = body.find("Branches affected:")
    if idx < 0:
        return []
    snippet = body[idx : idx + 500]
    return re.findall(r"`([^`]+)`", snippet)


def fetch_mart_rows(ydb_wrapper: YDBWrapper, lookback_days: int) -> List[Dict[str, Any]]:
    """Query the mart table for the last lookback_days. Returns list of dicts."""
    query = f"""
    SELECT
        full_name,
        suite_folder,
        test_name,
        pr_number,
        job_id,
        run_url,
        last_run_timestamp,
        branch,
        build_type,
        status_description,
        stderr,
        attempt_number,
        pr_target_branch,
        pr_status,
        pr_title,
        pr_url,
        is_muted_today,
        is_muted_in_run_day,
        owner_today
    FROM `{MART_TABLE}`
    WHERE last_run_timestamp > CurrentUtcDate() - {lookback_days} * Interval("P1D")
        AND pr_state = 'MERGED'
        AND pr_status = 'merged'
        and attempt_number >1
    ORDER BY branch, suite_folder, last_run_timestamp DESC
    """
    raw_rows, column_types = ydb_wrapper.execute_scan_query_with_metadata(query, query_name="pr_blocked_with_pr_and_mute")
    column_names = [c[0] for c in column_types] if column_types else []
    if not column_names:
        return []
    return [_row_to_dict(r, column_names) for r in raw_rows]


def _str(val: Any, max_len: int = 0) -> str:
    """Convert value to string; decode bytes so we don't get b'...' in output."""
    if val is None:
        return ""
    if isinstance(val, bytes):
        s = val.decode("utf-8", errors="replace").strip()
    else:
        s = str(val).strip()
    if max_len and len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s


_owner_area_mapping_cache: Optional[Dict[str, str]] = None


def load_owner_area_mapping() -> Dict[str, str]:
    """Load .github/config/owner_area_mapping.json: owner key -> area label (e.g. area/schemeshard)."""
    global _owner_area_mapping_cache
    if _owner_area_mapping_cache is not None:
        return _owner_area_mapping_cache
    if not os.path.isfile(OWNER_AREA_MAPPING_PATH):
        _owner_area_mapping_cache = {}
        return {}
    try:
        with open(OWNER_AREA_MAPPING_PATH, "r", encoding="utf-8") as f:
            _owner_area_mapping_cache = json.load(f)
        return _owner_area_mapping_cache
    except (json.JSONDecodeError, OSError):
        _owner_area_mapping_cache = {}
        return {}


def suggested_area_for_owner(owner: str, mapping: Dict[str, str]) -> Optional[str]:
    """Return area label for owner (e.g. schemeshard -> area/schemeshard). Tries exact, then lowercase, then strip org prefix."""
    if not owner or not mapping:
        return None
    owner = owner.strip()
    # Exact match
    if owner in mapping:
        return mapping[owner]
    # Owner might be "ydb-platform/schemeshard" or "org/team"
    if "/" in owner:
        suffix = owner.split("/", 1)[-1]
        if suffix in mapping:
            return mapping[suffix]
        if suffix.lower() in mapping:
            return mapping[suffix.lower()]
    if owner.lower() in mapping:
        return mapping[owner.lower()]
    return None


_stable_tests_branches_cache: Optional[List[str]] = None


def load_stable_tests_branches() -> List[str]:
    """Load .github/config/stable_tests_branches.json: list of branch names to check for similar failures."""
    global _stable_tests_branches_cache
    if _stable_tests_branches_cache is not None:
        return _stable_tests_branches_cache
    if not os.path.isfile(STABLE_TESTS_BRANCHES_PATH):
        _stable_tests_branches_cache = []
        return []
    try:
        with open(STABLE_TESTS_BRANCHES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            _stable_tests_branches_cache = []
        else:
            _stable_tests_branches_cache = [str(b).strip() for b in data if b]
        return _stable_tests_branches_cache
    except (json.JSONDecodeError, OSError, TypeError):
        _stable_tests_branches_cache = []
        return []


def _format_stable_branches_for_instructions() -> str:
    """Return a short list of stable branches for the issue body (from stable_tests_branches.json)."""
    branches = load_stable_tests_branches()
    if not branches:
        return "see `.github/config/stable_tests_branches.json`"
    return ", ".join(f"`{b}`" for b in branches[:15]) + (" ..." if len(branches) > 15 else "")


def build_issue_title(owner: str, suite_folder: str, count: int) -> str:
    return f"Autodebug: owner `{owner}`, suite `{suite_folder}` ({count} failure(s))"


def _slug_for_label(s: str, max_len: int) -> str:
    """Normalize string for use in a GitHub label: lowercase, [a-z0-9_-], truncate."""
    if not s:
        return "unknown"
    s = (s or "").strip().lower()
    out: List[str] = []
    for c in s:
        if c.isalnum() or c in "-_":
            out.append(c)
        elif out and out[-1] != "_":
            out.append("_")
    slug = "".join(out).strip("_") or "unknown"
    return slug[:max_len] if len(slug) > max_len else slug


def owner_dedup_label(owner: str) -> str:
    """Label for dedup: autodebug-owner-<slug>. Same owner = same label."""
    max_slug = GITHUB_LABEL_MAX_LEN - len(AUTODEBUG_OWNER_LABEL_PREFIX)
    return AUTODEBUG_OWNER_LABEL_PREFIX + _slug_for_label(owner, max_slug)


def suite_dedup_label(suite: str) -> str:
    """Label for dedup: autodebug-suite-<slug>. Same suite = same label."""
    max_slug = GITHUB_LABEL_MAX_LEN - len(AUTODEBUG_SUITE_LABEL_PREFIX)
    return AUTODEBUG_SUITE_LABEL_PREFIX + _slug_for_label(suite, max_slug)


def _build_failures_section(rows: List[Dict[str, Any]]) -> str:
    """Build the Failures section (by test; branch and links). Used in build_issue_body."""
    by_full_name: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_full_name[_str(r.get("full_name"), 200) or "unknown"].append(r)
    lines: List[str] = []
    idx = 0
    for full_name in sorted(by_full_name.keys()):
        group_rows = by_full_name[full_name]
        idx += 1
        branches_for_test = sorted({_str(r.get("branch")) or "unknown" for r in group_rows})
        lines.append(f"### {idx}. `{full_name}`")
        lines.append("")
        lines.append(f"**Branches:** " + ", ".join(f"`{b}`" for b in branches_for_test))
        lines.append("")
        for r in group_rows:
            branch = _str(r.get("branch")) or "unknown"
            run_url = _str(r.get("run_url"))
            stderr = _str(r.get("stderr"))
            pr_url = _str(r.get("pr_url"))
            pr_title = _str(r.get("pr_title"), 120)
            status_desc = _str(r.get("status_description"), 800)
            pr_number = _str(r.get("pr_number"))
            parts = [f"**{branch}:**"]
            if run_url:
                parts.append(f"[Run]({run_url})")
            if stderr:
                parts.append(f"[Stderr]({stderr})")
            if pr_url:
                parts.append(f"[PR]({pr_url})" + (f" ({pr_title})" if pr_title else f" (#{pr_number})"))
            lines.append("- " + " | ".join(parts))
            if status_desc:
                excerpt = status_desc.replace("```", "` ` `")[:600]
                if len(status_desc) > 600:
                    excerpt += "..."
                lines.append("  **Error excerpt:**")
                lines.append("  ```")
                for line in excerpt.split("\n"):
                    lines.append("  " + line)
                lines.append("  ```")
        lines.append("")
    return "\n".join(lines)


def build_issue_body(owner: str, suite_folder: str, rows: List[Dict[str, Any]]) -> str:
    """Build markdown body from prompt template: Instructions, Context, Failures (no checklist — requested later in PR comment)."""
    mapping = load_owner_area_mapping()
    suggested_area = suggested_area_for_owner(owner, mapping) if owner else None
    branches_affected = sorted({_str(r.get("branch")) or "unknown" for r in rows})
    suggested_area_line = (
        f"- **Suggested area (from owner_area_mapping.json):** `{suggested_area}`\n"
        if suggested_area
        else ""
    )
    failures_section = _build_failures_section(rows)
    body = _load_prompt_template(
        "issue_body",
        OWNER=owner or "",
        SUITE=suite_folder or "",
        FAILURES_COUNT=str(len(rows)),
        BRANCHES_AFFECTED=", ".join(f"`{b}`" for b in branches_affected),
        SUGGESTED_AREA_LINE=suggested_area_line,
        FAILURES_SECTION=failures_section,
    )
    if not body:
        # Fallback if template missing: minimal body with failures only
        body = "## Context\n\n- **Owner:** " + (owner or "") + "\n- **Suite:** " + (suite_folder or "") + "\n\n## Failures\n\n" + failures_section
    if len(body) > GITHUB_MAX_BODY_LENGTH:
        body = body[: GITHUB_MAX_BODY_LENGTH - 80] + "\n\n... [truncated due to length] ..."
    return body


def group_by_owner_suite(rows: List[Dict[str, Any]]) -> Dict[tuple, List[Dict[str, Any]]]:
    """Group rows by (owner_today, suite_folder). One issue per (owner, suite)."""
    groups = defaultdict(list)
    for r in rows:
        owner = _str(r.get("owner_today")) or "unknown"
        suite = _str(r.get("suite_folder")) or "unknown"
        groups[(owner, suite)].append(r)
    return dict(groups)


def _pr_stage_summary(
    n_finish: int,
    tests_passed: bool,
    stages_posted: set,
    checklist_data: Any,
    is_draft: bool,
) -> str:
    """Short label for PR autodebug stage (for console output)."""
    if checklist_data:
        return "checklist_filled" + (" (will mark ready)" if is_draft else " (ready)")
    if "checklist_request" in stages_posted:
        return "waiting_for_checklist"
    if "closure_request" in stages_posted:
        return "closure_request_posted"
    if "verify" in stages_posted and n_finish >= 2:
        return "verify_done_waiting_closure"
    if "verify" in stages_posted:
        return "verify_posted"
    if tests_passed and n_finish >= 1:
        return "tests_passed_will_post_verify"
    if n_finish >= 1:
        return "after_first_run_waiting_tests"
    return "waiting_first_run"


def ensure_label_exists(repo: Any, label_name: str, color: str = "ededed", description: str = "") -> None:
    """Create the label in the repo if it does not exist (for dedup labels)."""
    if len(label_name) > GITHUB_LABEL_MAX_LEN:
        return
    try:
        existing = repo.get_label(label_name)
        if existing:
            return
    except Exception:
        pass
    try:
        repo.create_label(label_name, color, description or label_name)
    except Exception:
        pass


def find_open_issue_with_owner_suite(repo: Any, owner: str, suite: str) -> Any:
    """Return an open issue (not a PR) with labels autodebug + autodebug-owner-* + autodebug-suite-* for this owner/suite, or None."""
    olabel = owner_dedup_label(owner)
    slabel = suite_dedup_label(suite)
    try:
        # GitHub API: multiple labels = OR; we need AND, so filter by one then check the rest
        for issue in repo.get_issues(state="open", labels=[olabel]):
            if getattr(issue, "pull_request", None) is not None:
                continue
            names = [lb.name for lb in (issue.labels or [])]
            if AUTODEBUG_LABEL in names and slabel in names:
                return issue
    except Exception:
        pass
    return None


def _load_prompt_template(name: str, **substitutions: str) -> str:
    """Load prompt from automation/prompts/<name>.md and substitute {{KEY}} with substitutions."""
    path = os.path.join(AUTOMATION_PROMPTS_DIR, name + ".md")
    try:
        with open(path, "r", encoding="utf-8") as f:
            body = f.read()
    except OSError:
        return ""
    for k, v in substitutions.items():
        body = body.replace("{{" + k + "}}", v or "")
    return body


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create GitHub issues for PR-check blocked failures by branch and suite (for Copilot investigation)."
    )
    parser.add_argument(
        "--lookback_days",
        type=int,
        default=1,
        help="Consider mart rows from the last N days (default: 1).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data and print issue titles/bodies only; do not create issues.",
    )
    parser.add_argument(
        "--assignee",
        type=str,
        default=DEFAULT_ASSIGNEE,
        help=f"GitHub username to assign issues to (default: {DEFAULT_ASSIGNEE}).",
    )
    parser.add_argument(
        "--no-assignee",
        action="store_true",
        help="Do not assign issues to anyone (overrides --assignee).",
    )
    parser.add_argument(
        "--labels",
        type=str,
        default=",".join(DEFAULT_LABELS),
        help=f"Comma-separated issue labels (default: {','.join(DEFAULT_LABELS)}).",
    )
    parser.add_argument(
        "--dry-run-output",
        type=str,
        default="",
        metavar="DIR",
        help="With --dry-run: write each issue body to DIR/<owner>_<suite_slug>.md for inspection.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually create issues. Use with --dry-run to preview first then create in one run.",
    )
    parser.add_argument(
        "--owner",
        type=str,
        default="",
        metavar="OWNER",
        help="Only process this owner (e.g. TEAM:@ydb-platform/engineering). One command: --owner X --dry-run or --owner X --execute.",
    )
    parser.add_argument(
        "--assign-issue",
        type=int,
        default=0,
        metavar="NUMBER",
        help=f"Assign Copilot (SWE agent) to existing issue NUMBER via agent_assignment API; no mart fetch. Example: --assign-issue 34730.",
    )
    parser.add_argument(
        "--base-branch",
        type=str,
        default="main",
        metavar="BRANCH",
        help="Base branch for Copilot agent_assignment (default: main). Used with --assign-issue as fallback when issue body has no 'Branches affected:'.",
    )
    parser.add_argument(
        "--add-ok-to-test",
        action="store_true",
        help="For each open issue with label 'autodebug', find linked PR (prefer Copilot's), add label 'ok-to-test'. Use --dry-run to only print.",
    )
    parser.add_argument(
        "--check-issues",
        action="store_true",
        help="For each open autodebug issue: assign Copilot if missing; for linked PR: staged flow (ok-to-test after finish, verify/closure/checklist comments, mark Ready only after checklist filled).",
    )
    parser.add_argument(
        "--issue",
        type=int,
        default=0,
        metavar="N",
        help="With --check-issues: only process this issue number (e.g. --check-issues --issue 34744).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="With --add-ok-to-test or --check-issues: print per-issue/PR details.",
    )
    args = parser.parse_args()
    labels = [x.strip() for x in args.labels.split(",") if x.strip()]

    # Mode: check issues — assign Copilot if missing; for linked PR: staged flow (no undraft until checklist filled).
    if args.check_issues:
        if not os.environ.get("GITHUB_TOKEN"):
            print("Error: GITHUB_TOKEN is required for --check-issues.", file=sys.stderr)
            return 1
        try:
            from github import Github
        except ImportError:
            print("Error: PyGithub is required. Install with: pip install PyGithub", file=sys.stderr)
            return 1
        gh = Github(os.environ["GITHUB_TOKEN"])
        repo = gh.get_repo(f"{OWNER}/{REPO}")
        ensure_label_exists(repo, AUTODEBUG_BROKEN_LABEL, description="Autodebug: Copilot did not reply after re-ask")
        token = os.environ["GITHUB_TOKEN"]
        done_issues = 0
        done_prs = 0
        if args.issue:
            issues_to_process = [repo.get_issue(args.issue)]
        else:
            issues_to_process = [i for i in repo.get_issues(state="open", labels=[AUTODEBUG_LABEL])]
        prs_worked = 0  # only run full PR flow (labels, comments) for up to MAX_CONCURRENT_AUTODEBUG PRs
        for issue in issues_to_process:
            if getattr(issue, "pull_request", None) is not None:
                if args.issue:
                    print(f"Issue #{issue.number}: is a PR, not an issue", file=sys.stderr)
                continue
            assignee_logins = [a.login for a in (issue.assignees or [])]
            has_copilot = any(
                (login or "").lower() in ("copilot", "copilot-swe-agent[bot]")
                for login in assignee_logins
            )
            # 1) If no assignee → assign Copilot (base_branch from issue body: main if present, else senior stable)
            if not assignee_logins or not has_copilot:
                body_branches = _parse_branches_from_issue_body(issue.body or "")
                base_branch = _base_branch_from_branches(body_branches) if body_branches else "main"
                if not args.dry_run:
                    if add_copilot_assignee_via_agent_assignment(
                        OWNER, REPO, issue.number, token, base_branch=base_branch
                    ):
                        print(f"Issue #{issue.number}: assigned Copilot")
                        done_issues += 1
                    else:
                        print(f"Issue #{issue.number}: failed to assign Copilot", file=sys.stderr)
                else:
                    print(f"Would assign Copilot to issue #{issue.number} (base_branch={base_branch})")
                    done_issues += 1
            # 2) Linked PR: staged flow — timeline + comments to decide next action (only for first MAX_CONCURRENT_AUTODEBUG PRs)
            pr_numbers = get_linked_pr_numbers_via_graphql(OWNER, REPO, issue.number, token)
            if not pr_numbers:
                print(f"Issue #{issue.number}: no linked PR, skip")
                continue
            if not args.issue and prs_worked >= MAX_CONCURRENT_AUTODEBUG:
                print(f"Issue #{issue.number}: skip PR flow (max concurrent PRs={MAX_CONCURRENT_AUTODEBUG})")
                continue
            chosen_pr = pr_numbers[0]
            pr_details = get_pr_details(OWNER, REPO, chosen_pr, token)
            if not pr_details:
                print(f"Issue #{issue.number}: could not get PR #{chosen_pr} details, skip")
                continue
            prs_worked += 1  # count this PR as in progress (so limit applies even when we continue later)
            changed_files = pr_details.get("changed_files") or 0
            if changed_files == 0:
                if args.verbose:
                    print(f"Issue #{issue.number}: PR #{chosen_pr} has no changed files (investigation-only); will still run flow for verify/checklist.")
            is_draft = pr_details.get("draft") is True
            pr_labels = get_pr_labels(OWNER, REPO, chosen_pr, token)
            has_autodebug = AUTODEBUG_LABEL in pr_labels
            # Add autodebug to PR as soon as we found the linked PR (so Copilot sees it early)
            if not has_autodebug:
                if args.dry_run:
                    print(f"  Would add label autodebug to PR #{chosen_pr} (found linked PR)")
                elif add_labels_to_issue_or_pr(OWNER, REPO, chosen_pr, [AUTODEBUG_LABEL], token):
                    print(f"PR #{chosen_pr}: added label autodebug")
                    done_prs += 1
                    pr_labels = get_pr_labels(OWNER, REPO, chosen_pr, token)
                    has_autodebug = True
            # Timeline: count copilot_work_finished
            timeline = get_issue_timeline(OWNER, REPO, chosen_pr, token)
            n_finish = sum(1 for e in timeline if (e.get("event") or "") == "copilot_work_finished")
            # PR comments (chronological)
            try:
                gh_issue = repo.get_issue(chosen_pr)
                comments = list(gh_issue.get_comments())
                comments_sorted = sorted(comments, key=lambda c: c.created_at or "")
            except Exception:
                comments_sorted = []
            # Status comments from github-actions → by_preset, tests_passed
            status_bodies = [(c.id, c.body or "") for c in comments_sorted if "<!-- status" in (c.body or "")]
            by_preset = collect_latest_by_preset(status_bodies)
            tests_passed = all_builds_passed(by_preset) if by_preset else False
            # Our autodebug-stage comments
            stages_posted = set()
            for c in comments_sorted:
                if "autodebug-stage" not in (c.body or ""):
                    continue
                parsed = parse_autodebug_stage_comment(c.body or "")
                if parsed:
                    stages_posted.add(parsed[0])
            # Runs for which we already posted ok-to-test-added (label is removed by CI after tests)
            comment_bodies = [c.body or "" for c in comments_sorted]
            ok_to_test_runs_posted = parse_ok_to_test_added_runs(comment_bodies, chosen_pr)
            # Head SHA: skip adding ok-to-test if no new commits since last run (avoid re-running same code)
            current_head_sha = (pr_details.get("head") or {}).get("sha") or ""
            last_ok_to_test_sha = get_last_ok_to_test_head_sha(comment_bodies, chosen_pr)
            no_new_commits_since_last_run = bool(
                last_ok_to_test_sha and current_head_sha and current_head_sha == last_ok_to_test_sha
            )
            # Checklist filled (any comment with parsed block)
            checklist_data = None
            for c in comments_sorted:
                data = parse_checklist_from_body(c.body or "")
                if data:
                    checklist_data = data
            # Our stage comments (id, created_at, repost_no_reply) per stage for 15-min retry / autodebug_broken logic
            our_stage_comments = get_our_stage_comments(comments_sorted, chosen_pr)
            # Human-readable stage for output
            stage = _pr_stage_summary(n_finish, tests_passed, stages_posted, checklist_data, is_draft)
            print(f"PR #{chosen_pr}: stage={stage} (n_finish={n_finish}, tests_ok={tests_passed}, stages={sorted(stages_posted) or 'none'})")
            # State machine: next action
            if args.verbose:
                print(f"  Issue #{issue.number} PR #{chosen_pr}: checklist={bool(checklist_data)} draft={is_draft}")
            if args.dry_run:
                now_utc = datetime.now(timezone.utc)
                wait_threshold = now_utc - timedelta(minutes=COPILOT_WAIT_MINUTES)
                dry_run_our = get_our_stage_comments(comments_sorted, chosen_pr)
                if "verify" in stages_posted and dry_run_our.get("verify"):
                    v = dry_run_our["verify"][-1]
                    if v[1] < wait_threshold and not get_copilot_finished_after(timeline, v[1]):
                        print(f"  Would re-post verify (no Copilot work in {COPILOT_WAIT_MINUTES} min)")
                if "closure_request" in stages_posted and dry_run_our.get("closure_request"):
                    c = dry_run_our["closure_request"][-1]
                    if c[1] < wait_threshold and not get_copilot_finished_after(timeline, c[1]):
                        print(f"  Would re-post closure_request (no Copilot work in {COPILOT_WAIT_MINUTES} min)")
                    elif c[1] < wait_threshold and get_copilot_finished_after(timeline, c[1]) and not copilot_replied_after(comments_sorted, c[1]):
                        print(f"  Would re-post closure_request with repost_no_reply=1 or set {AUTODEBUG_BROKEN_LABEL}")
                if "checklist_request" in stages_posted and dry_run_our.get("checklist_request"):
                    ch = dry_run_our["checklist_request"][-1]
                    if ch[1] < wait_threshold and not get_copilot_finished_after(timeline, ch[1]):
                        print(f"  Would re-post checklist_request (no Copilot work in {COPILOT_WAIT_MINUTES} min)")
                    elif ch[1] < wait_threshold and get_copilot_finished_after(timeline, ch[1]) and not copilot_replied_with_checklist_after(comments_sorted, ch[1]):
                        print(f"  Would re-post checklist_request with repost_no_reply=1 or set {AUTODEBUG_BROKEN_LABEL} (no filled checklist)")
                if n_finish >= 1:
                    print(f"  Would ensure labels ok-to-test + autodebug on PR #{chosen_pr}")
                if tests_passed and n_finish >= 1 and "verify" not in stages_posted:
                    print(f"  Would post verify comment with report.json links")
                if "verify" in stages_posted and n_finish >= 2 and "closure_request" not in stages_posted:
                    verify_list = dry_run_our.get("verify", [])
                    if verify_list and get_copilot_finished_after(timeline, verify_list[-1][1]):
                        print(f"  Would post closure_request comment")
                    else:
                        print(f"  Would wait for Copilot to finish after verify before posting closure_request")
                if "closure_request" in stages_posted and n_finish >= 3:
                    print(f"  Would ensure ok-to-test (run 2)")
                if "closure_request" in stages_posted and n_finish >= 3 and tests_passed and "checklist_request" not in stages_posted:
                    print(f"  Would post checklist_request comment")
                if checklist_data and is_draft:
                    print(f"  Would mark PR #{chosen_pr} ready for review")
                done_prs += 1
                continue
            # Copilot call checks: 15 min no start → delete our comment and re-post same stage;
            # finished but no reply → re-post once with repost_no_reply=1; second time → autodebug_broken
            now_utc = datetime.now(timezone.utc)
            wait_threshold = now_utc - timedelta(minutes=COPILOT_WAIT_MINUTES)
            did_copilot_retry_or_broken = False
            # 1) Verify: our comment > 15 min old and Copilot never finished after it → delete and re-post verify
            if "verify" in stages_posted:
                verify_list = our_stage_comments.get("verify", [])
                if verify_list:
                    cid, after_dt, _ = verify_list[-1]
                    if after_dt < wait_threshold and not get_copilot_finished_after(timeline, after_dt):
                        if delete_issue_comment(OWNER, REPO, cid, token):
                            report_links = format_report_json_links_for_copilot(by_preset) if by_preset else "(no status comments parsed)"
                            body = _load_prompt_template("verify_test_results", REPORT_LINKS=report_links)
                            if body:
                                header = make_autodebug_stage_header("verify", chosen_pr, "all", 0)
                                try:
                                    repo.get_issue(chosen_pr).create_comment(header + "\n\n" + body)
                                    print(f"PR #{chosen_pr}: re-posted verify (no Copilot work started in {COPILOT_WAIT_MINUTES} min)")
                                    done_prs += 1
                                    did_copilot_retry_or_broken = True
                                except Exception as e:
                                    print(f"PR #{chosen_pr}: failed to re-post verify: {e}", file=sys.stderr)
            # 2) Closure_request: 15 min no start → delete and re-post; or finished but no reply → re-post with repost_no_reply=1 or set autodebug_broken
            if not did_copilot_retry_or_broken and "closure_request" in stages_posted:
                closure_list = our_stage_comments.get("closure_request", [])
                if closure_list:
                    cid, after_dt, repost_no_reply = closure_list[-1]
                    if after_dt < wait_threshold and not get_copilot_finished_after(timeline, after_dt):
                        # No work started in 15 min → delete and re-post same stage
                        if delete_issue_comment(OWNER, REPO, cid, token):
                            body = _load_prompt_template("closure_request")
                            if body:
                                header = make_autodebug_stage_header("closure_request", chosen_pr, "all", 0)
                                try:
                                    repo.get_issue(chosen_pr).create_comment(header + "\n\n" + body)
                                    print(f"PR #{chosen_pr}: re-posted closure_request (no Copilot work in {COPILOT_WAIT_MINUTES} min)")
                                    done_prs += 1
                                    did_copilot_retry_or_broken = True
                                except Exception as e:
                                    print(f"PR #{chosen_pr}: failed to re-post closure_request: {e}", file=sys.stderr)
                    elif (
                        get_copilot_finished_after(timeline, after_dt)
                        and not copilot_replied_after(comments_sorted, after_dt)
                        and after_dt < wait_threshold
                    ):
                        if repost_no_reply:
                            if add_labels_to_issue_or_pr(OWNER, REPO, chosen_pr, [AUTODEBUG_BROKEN_LABEL], token):
                                print(f"PR #{chosen_pr}: set label {AUTODEBUG_BROKEN_LABEL} (Copilot finished but no reply after re-ask)")
                                done_prs += 1
                            did_copilot_retry_or_broken = True
                        else:
                            if delete_issue_comment(OWNER, REPO, cid, token):
                                body = _load_prompt_template("closure_request")
                                if body:
                                    header = make_autodebug_stage_header("closure_request", chosen_pr, "all", 0, repost_no_reply=True)
                                    try:
                                        repo.get_issue(chosen_pr).create_comment(header + "\n\n" + body)
                                        print(f"PR #{chosen_pr}: re-posted closure_request (Copilot finished but no reply in {COPILOT_WAIT_MINUTES} min)")
                                        done_prs += 1
                                        did_copilot_retry_or_broken = True
                                    except Exception as e:
                                        print(f"PR #{chosen_pr}: failed to re-post closure_request: {e}", file=sys.stderr)
            # 3) Checklist_request: 15 min no start → delete and re-post; or finished but no reply → re-post or autodebug_broken
            if not did_copilot_retry_or_broken and "checklist_request" in stages_posted:
                checklist_list = our_stage_comments.get("checklist_request", [])
                if checklist_list:
                    cid, after_dt, repost_no_reply = checklist_list[-1]
                    if after_dt < wait_threshold and not get_copilot_finished_after(timeline, after_dt):
                        if delete_issue_comment(OWNER, REPO, cid, token):
                            body = _load_prompt_template("checklist_request")
                            if body:
                                header = make_autodebug_stage_header("checklist_request", chosen_pr, "all", 0)
                                try:
                                    repo.get_issue(chosen_pr).create_comment(header + "\n\n" + body)
                                    print(f"PR #{chosen_pr}: re-posted checklist_request (no Copilot work in {COPILOT_WAIT_MINUTES} min)")
                                    done_prs += 1
                                    did_copilot_retry_or_broken = True
                                except Exception as e:
                                    print(f"PR #{chosen_pr}: failed to re-post checklist_request: {e}", file=sys.stderr)
                    elif (
                        get_copilot_finished_after(timeline, after_dt)
                        and not copilot_replied_with_checklist_after(comments_sorted, after_dt)
                        and after_dt < wait_threshold
                    ):
                        if repost_no_reply:
                            if add_labels_to_issue_or_pr(OWNER, REPO, chosen_pr, [AUTODEBUG_BROKEN_LABEL], token):
                                print(f"PR #{chosen_pr}: set label {AUTODEBUG_BROKEN_LABEL} (checklist: no filled checklist after re-ask)")
                                done_prs += 1
                            did_copilot_retry_or_broken = True
                        else:
                            if delete_issue_comment(OWNER, REPO, cid, token):
                                body = _load_prompt_template("checklist_request")
                                if body:
                                    header = make_autodebug_stage_header("checklist_request", chosen_pr, "all", 0, repost_no_reply=True)
                                    try:
                                        repo.get_issue(chosen_pr).create_comment(header + "\n\n" + body)
                                        print(f"PR #{chosen_pr}: re-posted checklist_request (no filled checklist in {COPILOT_WAIT_MINUTES} min)")
                                        done_prs += 1
                                        did_copilot_retry_or_broken = True
                                    except Exception as e:
                                        print(f"PR #{chosen_pr}: failed to re-post checklist_request: {e}", file=sys.stderr)
            if did_copilot_retry_or_broken:
                continue
            # Add labels after first finish; ok-to-test only when there are file changes and there are new commits since last run
            if n_finish >= 1:
                labels_to_add = []
                if AUTODEBUG_LABEL not in pr_labels:
                    labels_to_add.append(AUTODEBUG_LABEL)
                need_ok_to_test_run1 = (
                    changed_files > 0
                    and 1 not in ok_to_test_runs_posted
                    and OK_TO_TEST_LABEL not in pr_labels
                    and not no_new_commits_since_last_run
                )
                if (
                    changed_files > 0
                    and 1 not in ok_to_test_runs_posted
                    and OK_TO_TEST_LABEL not in pr_labels
                    and no_new_commits_since_last_run
                ):
                    print(f"PR #{chosen_pr}: no new commits since last ok-to-test run, skip adding label")
                if need_ok_to_test_run1:
                    labels_to_add.append(OK_TO_TEST_LABEL)
                if labels_to_add and add_labels_to_issue_or_pr(OWNER, REPO, chosen_pr, labels_to_add, token):
                    print(f"PR #{chosen_pr}: added labels {labels_to_add}")
                    done_prs += 1
                    if need_ok_to_test_run1:
                        try:
                            sha_part = f" sha={current_head_sha}" if current_head_sha else ""
                            repo.get_issue(chosen_pr).create_comment(
                                f"<!-- autodebug-ok-to-test-added pr={chosen_pr} run=1{sha_part} -->\n\nLabel `ok-to-test` added for first run."
                            )
                        except Exception:
                            pass
            # Post verify when tests passed (first time) after ≥1 finish
            if tests_passed and n_finish >= 1 and "verify" not in stages_posted:
                report_links = format_report_json_links_for_copilot(by_preset) if by_preset else "(no status comments parsed)"
                body = _load_prompt_template("verify_test_results", REPORT_LINKS=report_links)
                if body:
                    header = make_autodebug_stage_header("verify", chosen_pr, "all", 0)
                    full_body = header + "\n\n" + body
                    try:
                        repo.get_issue(chosen_pr).create_comment(full_body)
                        print(f"PR #{chosen_pr}: posted verify comment")
                        done_prs += 1
                    except Exception as e:
                        print(f"PR #{chosen_pr}: failed to post verify comment: {e}", file=sys.stderr)
            # Post closure_request only after Copilot has finished work *after* our verify comment
            elif "verify" in stages_posted and n_finish >= 2 and "closure_request" not in stages_posted:
                verify_list = our_stage_comments.get("verify", [])
                finish_after_verify = bool(verify_list and get_copilot_finished_after(timeline, verify_list[-1][1]))
                if finish_after_verify:
                    body = _load_prompt_template("closure_request")
                    if body:
                        header = make_autodebug_stage_header("closure_request", chosen_pr, "all", 0)
                        full_body = header + "\n\n" + body
                        try:
                            repo.get_issue(chosen_pr).create_comment(full_body)
                            print(f"PR #{chosen_pr}: posted closure_request comment")
                            done_prs += 1
                        except Exception as e:
                            print(f"PR #{chosen_pr}: failed to post closure_request: {e}", file=sys.stderr)
                elif args.verbose and verify_list:
                    print(f"PR #{chosen_pr}: waiting for Copilot to finish after verify before posting closure_request")
            # After ≥3 finishes: ensure ok-to-test for run 2 only if we haven't recorded it yet, there are file changes, and there are new commits
            if "closure_request" in stages_posted and n_finish >= 3:
                if (
                    changed_files > 0
                    and 2 not in ok_to_test_runs_posted
                    and OK_TO_TEST_LABEL not in pr_labels
                    and no_new_commits_since_last_run
                ):
                    print(f"PR #{chosen_pr}: no new commits since last ok-to-test run, skip adding label (run 2)")
                need_ok_to_test_run2 = (
                    changed_files > 0
                    and 2 not in ok_to_test_runs_posted
                    and OK_TO_TEST_LABEL not in pr_labels
                    and not no_new_commits_since_last_run
                )
                if need_ok_to_test_run2:
                    if add_labels_to_issue_or_pr(OWNER, REPO, chosen_pr, [OK_TO_TEST_LABEL], token):
                        print(f"PR #{chosen_pr}: added ok-to-test (run 2)")
                        done_prs += 1
                        try:
                            sha_part = f" sha={current_head_sha}" if current_head_sha else ""
                            repo.get_issue(chosen_pr).create_comment(
                                f"<!-- autodebug-ok-to-test-added pr={chosen_pr} run=2{sha_part} -->\n\nLabel `ok-to-test` added for second run."
                            )
                        except Exception:
                            pass
                if tests_passed and "checklist_request" not in stages_posted:
                    body = _load_prompt_template("checklist_request")
                    if body:
                        header = make_autodebug_stage_header("checklist_request", chosen_pr, "all", 0)
                        full_body = header + "\n\n" + body
                        try:
                            repo.get_issue(chosen_pr).create_comment(full_body)
                            print(f"PR #{chosen_pr}: posted checklist_request comment")
                            done_prs += 1
                        except Exception as e:
                            print(f"PR #{chosen_pr}: failed to post checklist_request: {e}", file=sys.stderr)
            # Ask for checklist when verify posted but closure not yet — only when we're NOT posting closure in this run (n_finish < 2)
            # Otherwise we'd post both closure_request and checklist_request in one run (stages_posted is from start of run)
            if (
                "verify" in stages_posted
                and "closure_request" not in stages_posted
                and "checklist_request" not in stages_posted
                and n_finish >= 1
                and n_finish < 2
            ):
                body = _load_prompt_template("checklist_request")
                if body:
                    header = make_autodebug_stage_header("checklist_request", chosen_pr, "all", 0)
                    full_body = header + "\n\n" + body
                    try:
                        repo.get_issue(chosen_pr).create_comment(full_body)
                        print(f"PR #{chosen_pr}: posted checklist_request comment (verify done, no closure)")
                        done_prs += 1
                    except Exception as e:
                        print(f"PR #{chosen_pr}: failed to post checklist_request: {e}", file=sys.stderr)
            # No file changes (investigation-only): don't run tests, just ask for checklist → then we apply labels
            if (
                changed_files == 0
                and n_finish >= 1
                and "checklist_request" not in stages_posted
                and not ("verify" in stages_posted and "closure_request" not in stages_posted)
            ):
                body = _load_prompt_template("checklist_request")
                if body:
                    header = make_autodebug_stage_header("checklist_request", chosen_pr, "all", 0)
                    full_body = header + "\n\n" + body
                    try:
                        repo.get_issue(chosen_pr).create_comment(full_body)
                        print(f"PR #{chosen_pr}: posted checklist_request (no file changes; fill checklist so we set labels)")
                        done_prs += 1
                    except Exception as e:
                        print(f"PR #{chosen_pr}: failed to post checklist_request: {e}", file=sys.stderr)
            # When checklist filled: apply area and resolution labels to PR and to the linked issue
            if checklist_data:
                labels_from_checklist = []
                area = (checklist_data.get("area") or "").strip()
                resolution = (checklist_data.get("resolution") or "").strip()
                if area and area.startswith("area/"):
                    labels_from_checklist.append(area)
                if resolution in ("copilot-test-issue", "copilot-ydb-issue"):
                    labels_from_checklist.append(resolution)
                if labels_from_checklist:
                    pr_labels_current = get_pr_labels(OWNER, REPO, chosen_pr, token)
                    to_add_pr = [l for l in labels_from_checklist if l not in pr_labels_current]
                    if to_add_pr and add_labels_to_issue_or_pr(OWNER, REPO, chosen_pr, to_add_pr, token):
                        print(f"PR #{chosen_pr}: added labels from checklist {to_add_pr}")
                        done_prs += 1
                    try:
                        issue_obj = repo.get_issue(issue.number)
                        issue_label_names = [lb.name for lb in (issue_obj.labels or [])]
                        to_add_issue = [l for l in labels_from_checklist if l not in issue_label_names]
                        if to_add_issue and add_labels_to_issue_or_pr(OWNER, REPO, issue.number, to_add_issue, token):
                            print(f"Issue #{issue.number}: added labels from checklist {to_add_issue}")
                            done_prs += 1
                    except Exception as e:
                        print(f"Issue #{issue.number}: failed to add labels from checklist: {e}", file=sys.stderr)
            # Mark ready only when checklist filled
            if checklist_data and is_draft:
                if mark_pr_ready_for_review_graphql(OWNER, REPO, chosen_pr, token):
                    print(f"PR #{chosen_pr}: marked ready for review (checklist filled)")
                    done_prs += 1
                else:
                    print(f"PR #{chosen_pr}: failed to mark ready for review", file=sys.stderr)
        print(f"Done: {done_issues} issue(s) assigned, {done_prs} PR action(s) (labels/comments/ready).")
        return 0

    # Mode: add label 'ok-to-test' to PRs linked to autodebug issues (prefer Copilot's PR).
    if args.add_ok_to_test:
        if not os.environ.get("GITHUB_TOKEN"):
            print("Error: GITHUB_TOKEN is required for --add-ok-to-test.", file=sys.stderr)
            return 1
        try:
            from github import Github
        except ImportError:
            print("Error: PyGithub is required. Install with: pip install PyGithub", file=sys.stderr)
            return 1
        gh = Github(os.environ["GITHUB_TOKEN"])
        repo = gh.get_repo(f"{OWNER}/{REPO}")
        token = os.environ["GITHUB_TOKEN"]
        done = 0
        skipped = 0
        for issue in repo.get_issues(state="open", labels=[AUTODEBUG_LABEL]):
            if getattr(issue, "pull_request", None) is not None:
                continue
            # Only process issues that have Copilot assigned (linked PR + assigned Copilot).
            assignee_logins = [a.login for a in (issue.assignees or [])]
            if not any(
                (login or "").lower() in ("copilot", "copilot-swe-agent[bot]")
                for login in assignee_logins
            ):
                print(f"Issue #{issue.number}: Copilot not assigned (assignees: {assignee_logins}), skip")
                skipped += 1
                continue
            pr_numbers = get_linked_pr_numbers_via_graphql(OWNER, REPO, issue.number, token)
            if args.verbose:
                print(f"Issue #{issue.number}: linked PRs (closedByPullRequestsReferences)={pr_numbers}")
            if not pr_numbers:
                print(f"Issue #{issue.number}: no linked PR (link PR in Development section or use 'Fixes #{issue.number}' in PR), skip")
                skipped += 1
                continue
            # Prefer PR whose branch starts with "copilot/"
            chosen_pr = None
            for pr_num in pr_numbers:
                ref = get_pull_request_head_ref(OWNER, REPO, pr_num, token)
                if args.verbose:
                    print(f"  PR #{pr_num} head ref: {ref!r}")
                if ref.startswith("copilot/"):
                    chosen_pr = pr_num
                    break
            if chosen_pr is None:
                chosen_pr = pr_numbers[0]
            pr_details = get_pr_details(OWNER, REPO, chosen_pr, token)
            if pr_details and (pr_details.get("changed_files") or 0) == 0:
                if args.verbose:
                    print(f"PR #{chosen_pr}: no changed files (investigation-only), skip ok-to-test")
                skipped += 1
                continue
            if args.dry_run:
                print(f"Would add label '{OK_TO_TEST_LABEL}' to PR #{chosen_pr} (linked to issue #{issue.number})")
                done += 1
                continue
            # Skip if we already recorded ok-to-test for this PR (label is removed by CI after tests)
            try:
                pr_comments = [c.body or "" for c in repo.get_issue(chosen_pr).get_comments()]
                ok_runs = parse_ok_to_test_added_runs(pr_comments, chosen_pr)
                if ok_runs and OK_TO_TEST_LABEL not in get_pr_labels(OWNER, REPO, chosen_pr, token):
                    if args.verbose:
                        print(f"PR #{chosen_pr}: ok-to-test already added (runs {sorted(ok_runs)}), label removed by CI, skip re-add")
                    skipped += 1
                    continue
            except Exception:
                pass
            if add_labels_to_issue_or_pr(OWNER, REPO, chosen_pr, [OK_TO_TEST_LABEL], token):
                print(f"Added label '{OK_TO_TEST_LABEL}' to PR #{chosen_pr} (issue #{issue.number})")
                done += 1
                try:
                    repo.get_issue(chosen_pr).create_comment(
                        f"<!-- autodebug-ok-to-test-added pr={chosen_pr} run=1 -->\n\nLabel `ok-to-test` added (--add-ok-to-test mode)."
                    )
                except Exception:
                    pass
            else:
                print(f"Issue #{issue.number}: failed to add label to PR #{chosen_pr}, skip")
                skipped += 1
        print(f"Done: {done} PR(s) labeled, {skipped} skipped or failed.")
        return 0

    # Mode: assign Copilot (SWE agent) to an existing issue via agent_assignment API.
    if args.assign_issue:
        if not os.environ.get("GITHUB_TOKEN"):
            print("Error: GITHUB_TOKEN is required for --assign-issue.", file=sys.stderr)
            return 1
        token = os.environ["GITHUB_TOKEN"]
        body = get_issue_body(OWNER, REPO, args.assign_issue, token)
        body_branches = _parse_branches_from_issue_body(body)
        base_branch = _base_branch_from_branches(body_branches) if body_branches else args.base_branch
        ok = add_copilot_assignee_via_agent_assignment(
            OWNER,
            REPO,
            args.assign_issue,
            token,
            base_branch=base_branch,
        )
        if ok:
            print(f"Assigned Copilot (SWE agent) to issue #{args.assign_issue} (base_branch={base_branch})")
            print(f"  https://github.com/{OWNER}/{REPO}/issues/{args.assign_issue}")
        return 0 if ok else 1

    need_create = args.execute or (not args.dry_run)
    if need_create and not os.environ.get("GITHUB_TOKEN"):
        print("Error: GITHUB_TOKEN is required when creating issues (use --execute or run without --dry-run).", file=sys.stderr)
        return 1

    with YDBWrapper(silent=True) as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("Error: YDB credentials not available (CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS).", file=sys.stderr)
            return 1
        rows = fetch_mart_rows(ydb_wrapper, args.lookback_days)

    if not rows:
        print("No mart rows in the lookback window; nothing to do.")
        return 0

    groups = group_by_owner_suite(rows)
    if args.owner:
        owner_filter = args.owner.strip()
        groups = {(o, s): v for (o, s), v in groups.items() if o == owner_filter}
        if not groups:
            print(f"No groups left for owner '{owner_filter}'. Available owners in data: {sorted({o for o, _ in group_by_owner_suite(rows)})}")
            return 0
        print(f"Filtered to owner '{owner_filter}': {len(groups)} group(s), {sum(len(v) for v in groups.values())} row(s).")
    else:
        print(f"Found {len(rows)} row(s) in {len(groups)} group(s) (owner, suite).")

    if args.dry_run:
        out_dir = (args.dry_run_output or "").strip()
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        for (owner, suite), group_rows in sorted(groups.items()):
            title = build_issue_title(owner, suite, len(group_rows))
            body = build_issue_body(owner, suite, group_rows)
            print("\n" + "=" * 60)
            print("TITLE:", title)
            print("-" * 60)
            print(body[: 2000] + ("..." if len(body) > 2000 else ""))
            if out_dir:
                slug = f"{owner}_{suite}".replace("/", "_").replace("`", "").replace("\\", "_")[:120]
                path = os.path.join(out_dir, f"{slug}.md")
                with open(path, "w", encoding="utf-8") as f:
                    f.write("# " + title + "\n\n")
                    f.write(body)
                print(f"(saved body to {path})")
        print(f"\n[DRY-RUN] Would create {len(groups)} issue(s).")
        if not args.execute:
            print("Run with --execute to create, or use --dry-run --execute to preview then create in one go.")
            return 0
        print("\n--- Creating issues ---\n")

    if args.execute or not args.dry_run:
        try:
            from github import Github
        except ImportError:
            print("Error: PyGithub is required. Install with: pip install PyGithub", file=sys.stderr)
            return 1
        gh = Github(os.environ["GITHUB_TOKEN"])
        repo = gh.get_repo(f"{OWNER}/{REPO}")

    created = []
    skipped = []
    open_autodebug_count = 0
    if args.execute:
        try:
            open_autodebug_count = sum(1 for _ in repo.get_issues(state="open", labels=[AUTODEBUG_LABEL]))
        except Exception:
            pass
    for (owner, suite), group_rows in sorted(groups.items()):
        if args.execute and open_autodebug_count >= MAX_CONCURRENT_AUTODEBUG:
            skipped.append((owner, suite, f"max concurrent autodebug issues ({MAX_CONCURRENT_AUTODEBUG})"))
            print(f"Skipped (max {MAX_CONCURRENT_AUTODEBUG} open autodebug issues): owner {owner!r}, suite {suite!r}")
            continue
        existing = find_open_issue_with_owner_suite(repo, owner, suite)
        if existing:
            skipped.append((owner, suite, existing.html_url))
            print(f"Skipped (open issue exists): {existing.html_url} — owner {owner!r}, suite {suite!r}")
            continue
        title = build_issue_title(owner, suite, len(group_rows))
        body = build_issue_body(owner, suite, group_rows)
        olabel = owner_dedup_label(owner)
        slabel = suite_dedup_label(suite)
        issue_labels = list(labels) + [olabel, slabel]
        ensure_label_exists(repo, olabel)
        ensure_label_exists(repo, slabel)
        create_kw = {"title": title, "body": body, "labels": issue_labels}
        assignee = "" if args.no_assignee else (args.assignee or "").strip()
        # Assigning to Copilot requires header GraphQL-Features: issues_copilot_assignment_api_support;
        # PyGithub cannot send it, so create without assignee then add via REST.
        if assignee and assignee.lower() == "copilot":
            # create without assignees; add assignee after with Copilot header
            pass
        elif assignee:
            create_kw["assignees"] = [assignee]
        issue = repo.create_issue(**create_kw)
        if assignee and assignee.lower() == "copilot":
            base_branch = _base_branch_from_group_rows(group_rows)
            if add_copilot_assignee_via_agent_assignment(
                OWNER, REPO, issue.number, os.environ["GITHUB_TOKEN"], base_branch=base_branch
            ):
                print(f"  Assigned Copilot (SWE agent), base_branch={base_branch}")
            else:
                print(f"Warning: failed to assign Copilot to {issue.html_url}", file=sys.stderr)
        created.append((title, issue.html_url))
        print(f"Created: {issue.html_url} — {title}")
        if args.execute:
            open_autodebug_count += 1

    if created:
        print(f"\nCreated {len(created)} issue(s):")
        for _title, url in created:
            print(f"  {url}")
    if skipped:
        print(f"Skipped {len(skipped)} group(s) (open issue already exists or max concurrent limit).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
