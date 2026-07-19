#!/usr/bin/env python3
"""
Post-merge health checks for CI analytics after PR-check / postcommit unification (#45533).

Detects:
  - postcommit runs written with job_name=PR-check (regression from #44879)
  - missing recent Postcommit_* / PR-check rows on monitored branches
  - push build stats stored under github_workflow=PR-check

Scheduled via telegram_scheduled_notifications.yml; also runnable locally:
  python3 .github/scripts/analytics/ci_analytics_health_check.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from ydb_wrapper import YDBWrapper

# Branches to watch after merging #45533 into main / stable / prestable.
DEFAULT_BRANCHES = ("main", "prestable-26-3")

POSTCOMMIT_JOB_NAMES = ("Postcommit_relwithdebinfo", "Postcommit_asan")
POSTCOMMIT_BUILD_TYPES = ("relwithdebinfo", "release-asan")

# Max age before alerting (hours, UTC). prestable has PR-check only (no postcommit push).
POSTCOMMIT_MAX_AGE_HOURS = {
    "main": 20,
    "default_stable": 30,
}
PR_CHECK_MAX_AGE_HOURS = {
    "main": 8,
    "prestable-26-3": 36,
    "default_stable": 48,
    "default": 36,
}


@dataclass
class Violation:
    severity: str  # critical | warning
    check: str
    message: str
    details: Optional[Dict[str, Any]] = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_ts(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, (int, float)):
        # YDB Timestamp: microseconds since epoch when native types are off.
        value = datetime.fromtimestamp(float(value) / 1_000_000, tz=timezone.utc)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return str(value)


def _decode_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, bytes):
            out[key] = value.decode("utf-8")
        else:
            out[key] = value
    return out


def _hours_since(ts: Any, now: datetime) -> Optional[float]:
    if ts is None:
        return None
    if isinstance(ts, bytes):
        ts = datetime.fromisoformat(ts.decode("utf-8"))
    if isinstance(ts, (int, float)):
        ts = datetime.fromtimestamp(float(ts) / 1_000_000, tz=timezone.utc)
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (now - ts.astimezone(timezone.utc)).total_seconds() / 3600.0
    return None


def _branch_kind(branch: str) -> str:
    if branch == "main":
        return "main"
    if branch.startswith("prestable-"):
        return "prestable"
    if branch.startswith("stable-"):
        return "stable"
    return "other"


def _postcommit_max_age(branch: str) -> float:
    if branch == "main":
        return float(POSTCOMMIT_MAX_AGE_HOURS["main"])
    if branch.startswith("stable-"):
        return float(POSTCOMMIT_MAX_AGE_HOURS["default_stable"])
    return float(POSTCOMMIT_MAX_AGE_HOURS["default_stable"])


def _pr_check_max_age(branch: str) -> float:
    if branch in PR_CHECK_MAX_AGE_HOURS:
        return float(PR_CHECK_MAX_AGE_HOURS[branch])
    if branch.startswith("stable-"):
        return float(PR_CHECK_MAX_AGE_HOURS["default_stable"])
    return float(PR_CHECK_MAX_AGE_HOURS["default"])


def check_misnamed_postcommit(
    wrapper: YDBWrapper, test_runs_table: str, lookback_hours: int
) -> List[Violation]:
    query = f"""
        SELECT
            branch,
            build_type,
            job_name,
            job_id,
            pull,
            DateTime::FromMicroseconds(CAST(run_timestamp AS Int64)) AS run_ts
        FROM `{test_runs_table}`
        WHERE run_timestamp >= CurrentUtcTimestamp() - Interval("PT{lookback_hours}H")
          AND String::Contains(COALESCE(pull, ''), '_POST')
          AND job_name = 'PR-check'
        ORDER BY run_timestamp DESC
        LIMIT 50
    """
    rows = wrapper.execute_scan_query(query, query_name="ci_health_misnamed_postcommit")
    violations: List[Violation] = []
    if not rows:
        return violations

    samples = [_decode_row(r) for r in rows[:5]]
    violations.append(
        Violation(
            severity="critical",
            check="misnamed_postcommit",
            message=(
                f"Found {len(rows)} postcommit run(s) with job_name=PR-check in the last "
                f"{lookback_hours}h (expected Postcommit_*). This breaks mute rules and BI."
            ),
            details={"samples": samples, "count": len(rows)},
        )
    )
    return violations


def check_postcommit_freshness(
    wrapper: YDBWrapper,
    test_runs_table: str,
    branches: Sequence[str],
    lookback_days: int,
    now: datetime,
) -> List[Violation]:
    branch_list = ", ".join(f"'{b}'" for b in branches)
    query = f"""
        SELECT
            branch,
            build_type,
            job_name,
            MAX(run_timestamp) AS last_run,
            COUNT(*) AS runs
        FROM `{test_runs_table}`
        WHERE run_timestamp >= CurrentUtcTimestamp() - Interval("P{lookback_days}D")
          AND job_name IN ('Postcommit_relwithdebinfo', 'Postcommit_asan')
          AND (
                branch IN ({branch_list})
                OR branch LIKE 'stable-%'
              )
        GROUP BY branch, build_type, job_name
    """
    rows = wrapper.execute_scan_query(query, query_name="ci_health_postcommit_freshness")
    present = {
        (r["branch"].decode() if isinstance(r["branch"], bytes) else r["branch"],
         r["build_type"].decode() if isinstance(r["build_type"], bytes) else r["build_type"],
         r["job_name"].decode() if isinstance(r["job_name"], bytes) else r["job_name"]): r
        for r in rows
    }

    violations: List[Violation] = []
    for branch in branches:
        if _branch_kind(branch) == "prestable":
            continue
        for build_type, job_name in zip(
            POSTCOMMIT_BUILD_TYPES, POSTCOMMIT_JOB_NAMES, strict=True
        ):
            key = (branch, build_type, job_name)
            row = present.get(key)
            max_age = _postcommit_max_age(branch)
            if row is None:
                violations.append(
                    Violation(
                        severity="warning",
                        check="missing_postcommit_series",
                        message=(
                            f"No {job_name} rows for branch={branch}, build_type={build_type} "
                            f"in the last {lookback_days}d."
                        ),
                    )
                )
                continue
            age_h = _hours_since(row.get("last_run"), now)
            if age_h is not None and age_h > max_age:
                violations.append(
                    Violation(
                        severity="warning",
                        check="stale_postcommit",
                        message=(
                            f"{job_name} on {branch} ({build_type}) is stale: last run "
                            f"{_format_ts(row.get('last_run'))} ({age_h:.1f}h ago, limit {max_age}h)."
                        ),
                        details=_decode_row(row),
                    )
                )

    # stable branches seen in data but not explicitly listed
    for (branch, build_type, job_name), row in present.items():
        if not branch.startswith("stable-"):
            continue
        age_h = _hours_since(row.get("last_run"), now)
        max_age = _postcommit_max_age(branch)
        if age_h is not None and age_h > max_age:
            violations.append(
                Violation(
                    severity="warning",
                    check="stale_postcommit",
                    message=(
                        f"{job_name} on {branch} ({build_type}) is stale: last run "
                        f"{_format_ts(row.get('last_run'))} ({age_h:.1f}h ago, limit {max_age}h)."
                    ),
                    details=_decode_row(row),
                )
            )
    return violations


def check_pr_check_freshness(
    wrapper: YDBWrapper,
    test_runs_table: str,
    branches: Sequence[str],
    lookback_days: int,
    now: datetime,
) -> List[Violation]:
    branch_list = ", ".join(f"'{b}'" for b in branches)
    query = f"""
        SELECT
            branch,
            build_type,
            MAX(run_timestamp) AS last_run,
            COUNT(*) AS runs
        FROM `{test_runs_table}`
        WHERE run_timestamp >= CurrentUtcTimestamp() - Interval("P{lookback_days}D")
          AND job_name = 'PR-check'
          AND String::Contains(COALESCE(pull, ''), '_PR_')
          AND (
                branch IN ({branch_list})
                OR branch LIKE 'stable-%'
              )
        GROUP BY branch, build_type
    """
    rows = wrapper.execute_scan_query(query, query_name="ci_health_pr_check_freshness")
    violations: List[Violation] = []
    seen_branches = set()

    for row in rows:
        decoded = _decode_row(row)
        branch = decoded["branch"]
        seen_branches.add(branch)
        max_age = _pr_check_max_age(branch)
        age_h = _hours_since(decoded.get("last_run"), now)
        if age_h is not None and age_h > max_age:
            violations.append(
                Violation(
                    severity="warning",
                    check="stale_pr_check",
                    message=(
                        f"PR-check on {branch} ({decoded['build_type']}) is stale: last run "
                        f"{_format_ts(decoded.get('last_run'))} ({age_h:.1f}h ago, limit {max_age}h)."
                    ),
                    details=decoded,
                )
            )

    for branch in branches:
        if branch in seen_branches or branch.startswith("stable-"):
            continue
        violations.append(
            Violation(
                severity="warning",
                check="missing_pr_check",
                message=(
                    f"No PR-check rows for branch={branch} in the last {lookback_days}d "
                    f"(analytics upload or credentials may be broken)."
                ),
            )
        )
    return violations


def check_build_stats_workflow_names(
    wrapper: YDBWrapper, binary_size_table: str, lookback_days: int
) -> List[Violation]:
    query = f"""
        SELECT
            github_ref_name,
            build_preset,
            github_workflow,
            github_event_name,
            git_commit_time
        FROM `{binary_size_table}`
        WHERE git_commit_time >= CurrentUtcDatetime() - Interval("P{lookback_days}D")
          AND github_event_name = 'push'
          AND github_workflow = 'PR-check'
        ORDER BY git_commit_time DESC
        LIMIT 20
    """
    try:
        rows = wrapper.execute_scan_query(
            query, query_name="ci_health_build_stats_workflow"
        )
    except Exception as exc:
        return [
            Violation(
                severity="warning",
                check="build_stats_query_failed",
                message=f"Could not query binary_size table: {exc}",
            )
        ]

    if not rows:
        return []

    samples = [_decode_row(r) for r in rows[:5]]
    return [
        Violation(
            severity="critical",
            check="misnamed_build_stats",
            message=(
                f"Found {len(rows)} push build_stats row(s) with github_workflow=PR-check "
                f"(expected Postcommit_* after #45533)."
            ),
            details={"samples": samples, "count": len(rows)},
        )
    ]


def run_checks(
    branches: Sequence[str],
    lookback_hours: int,
    lookback_days: int,
) -> List[Violation]:
    now = _utc_now()
    violations: List[Violation] = []

    with YDBWrapper(script_name="ci_analytics_health_check.py") as wrapper:
        if not wrapper.check_credentials():
            raise RuntimeError("CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is not configured")

        test_runs_table = wrapper.get_table_path("test_results")
        binary_size_table = wrapper.get_table_path("binary_size")

        violations.extend(
            check_misnamed_postcommit(wrapper, test_runs_table, lookback_hours)
        )
        violations.extend(
            check_postcommit_freshness(
                wrapper, test_runs_table, branches, lookback_days, now
            )
        )
        violations.extend(
            check_pr_check_freshness(
                wrapper, test_runs_table, branches, lookback_days, now
            )
        )
        violations.extend(
            check_build_stats_workflow_names(
                wrapper, binary_size_table, lookback_days
            )
        )

    return violations


def format_telegram_message(violations: Sequence[Violation]) -> str:
    call = os.getenv("GH_ALERTS_TG_LOGINS", "").strip()
    lines = ["*CI analytics health check* (#45533 post-merge)"]
    if call:
        lines[0] = f"{call}\n\n" + lines[0]

    critical = [v for v in violations if v.severity == "critical"]
    warnings = [v for v in violations if v.severity == "warning"]

    if critical:
        lines.append(f"\n*Critical ({len(critical)}):*")
        for v in critical:
            lines.append(f"• [{v.check}] {v.message}")

    if warnings:
        lines.append(f"\n*Warnings ({len(warnings)}):*")
        for v in warnings[:10]:
            lines.append(f"• [{v.check}] {v.message}")
        if len(warnings) > 10:
            lines.append(f"… and {len(warnings) - 10} more")

    lines.append(
        "\nRunbook: merge #45533 → verify PR-check + postcommit on main → "
        "cherry-pick/sync to prestable-26-3 and stable-*."
    )
    return "\n".join(lines)


def send_telegram(message: str, chat_id: str, dry_run: bool) -> bool:
    if dry_run:
        print("DRY-RUN Telegram message:\n")
        print(message)
        return True

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        print("TELEGRAM_BOT_TOKEN is not set, skipping Telegram notification", file=sys.stderr)
        return False

    script_dir = os.path.join(os.path.dirname(__file__), "..", "telegram")
    send_script = os.path.join(script_dir, "send_telegram_message.py")
    if not os.path.isfile(send_script):
        print(f"send_telegram_message.py not found at {send_script}", file=sys.stderr)
        return False

    import subprocess

    result = subprocess.run(
        [
            sys.executable,
            send_script,
            "--bot-token",
            token,
            "--chat-id",
            chat_id,
            "--message",
            message,
            "--parse-mode",
            "Markdown",
        ],
        env={**os.environ, "TELEGRAM_BOT_TOKEN": token},
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stdout, file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        return False
    print(result.stdout)
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CI analytics health checks for #45533")
    parser.add_argument(
        "--branches",
        default=",".join(DEFAULT_BRANCHES),
        help="Comma-separated branches to monitor (stable-* auto-discovered for postcommit)",
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=48,
        help="Window for misnamed postcommit detection",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=3,
        help="Window for freshness checks",
    )
    parser.add_argument(
        "--chat-id",
        default="3017506311",
        help="Telegram chat id (same as alert_queued_jobs)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print results without sending Telegram",
    )
    parser.add_argument(
        "--send-telegram",
        action="store_true",
        help="Send Telegram alert when violations are found",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Exit 1 on warnings too (default: only critical)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    branches = tuple(b.strip() for b in args.branches.split(",") if b.strip())

    print(f"CI analytics health check @ {_utc_now().isoformat()}")
    print(f"Branches: {', '.join(branches)}")

    try:
        violations = run_checks(branches, args.lookback_hours, args.lookback_days)
    except Exception as exc:
        print(f"Health check failed: {exc}", file=sys.stderr)
        if args.send_telegram and not args.dry_run:
            send_telegram(
                f"CI analytics health check *ERROR*: {exc}",
                args.chat_id,
                dry_run=False,
            )
        return 1

    if not violations:
        print("OK: no violations detected")
        return 0

    for v in violations:
        prefix = "CRITICAL" if v.severity == "critical" else "WARNING"
        print(f"[{prefix}] {v.check}: {v.message}")
        if v.details:
            print(f"  details: {v.details}")

    critical = [v for v in violations if v.severity == "critical"]
    if args.send_telegram and violations:
        send_telegram(format_telegram_message(violations), args.chat_id, args.dry_run)

    if critical or (args.fail_on_warning and violations):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
