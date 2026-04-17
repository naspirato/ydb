#!/usr/bin/env python3
from __future__ import annotations

import datetime
import os
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from github_issue_utils import DEFAULT_BUILD_TYPE, parse_body
from mute_policy_rules import DEFAULT_THRESHOLDS, passes_default_unmute


def empty_quarantine_actions() -> Dict[str, object]:
    return {
        "hide": set(),
        "restore": set(),
        "stable": set(),
        "hide_debug": [],
        "restore_debug": [],
        "stable_debug": [],
        "stats": {},
    }


def normalize_utc_datetime(value) -> Optional[datetime.datetime]:
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc)
    if isinstance(value, int):
        # YDB scan values can be seconds or microseconds.
        if value > 10_000_000_000:
            value = value / 1_000_000
        return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)
    return None


def fallback_mute_string_from_full_name(full_name: str) -> Optional[str]:
    if not full_name or "/" not in full_name:
        return None
    testsuite, testcase = full_name.rsplit("/", 1)
    if not testsuite or not testcase:
        return None
    return f"{testsuite} {testcase}"


def build_full_name_to_mute_strings(rows: List[dict]) -> Dict[str, Set[str]]:
    result: Dict[str, Set[str]] = defaultdict(set)
    for row in rows:
        full_name = row.get("full_name")
        testsuite = row.get("suite_folder")
        testcase = row.get("test_name")
        if full_name and testsuite and testcase:
            result[str(full_name)].add(f"{testsuite} {testcase}")
    return result


def extract_latest_user_closed_tests(
    rows: List[dict],
    branch: str,
    build_type: str,
) -> Tuple[Dict[str, datetime.datetime], Dict[str, int]]:
    latest_close_by_test: Dict[str, datetime.datetime] = {}
    stats = {
        "rows_total": 0,
        "rows_user_closed": 0,
        "rows_missing_closed_by_type": 0,
        "rows_state_reason_rejected": 0,
        "rows_without_body": 0,
        "rows_without_closed_at": 0,
        "rows_parse_error": 0,
        "rows_branch_or_build_mismatch": 0,
        "rows_without_tests": 0,
        "linked_tests": 0,
    }

    for row in rows:
        stats["rows_total"] += 1
        closed_by_type = row.get("closed_by_type")
        if not closed_by_type:
            stats["rows_missing_closed_by_type"] += 1
            continue
        if str(closed_by_type) != "User":
            continue
        stats["rows_user_closed"] += 1

        # Treat only "completed" user closures as "user fixed" signal.
        # Other closure reasons (for example not-planned / duplicate flows)
        # should not start quarantine for muted tests.
        state_reason = str(row.get("state_reason") or "").upper()
        if state_reason and state_reason != "COMPLETED":
            stats["rows_state_reason_rejected"] += 1
            continue

        body = str(row.get("body") or "")
        if not body:
            stats["rows_without_body"] += 1
            continue

        closed_at = normalize_utc_datetime(row.get("closed_at"))
        if closed_at is None:
            stats["rows_without_closed_at"] += 1
            continue

        try:
            parsed = parse_body(body)
        except Exception:
            stats["rows_parse_error"] += 1
            continue

        issue_build_type = parsed.build_type or DEFAULT_BUILD_TYPE
        issue_branches = parsed.branches or ["main"]
        if issue_build_type != build_type or branch not in issue_branches:
            stats["rows_branch_or_build_mismatch"] += 1
            continue

        tests = [name for name in parsed.tests if name]
        if not tests:
            stats["rows_without_tests"] += 1
            continue

        for full_name in tests:
            stats["linked_tests"] += 1
            prev_closed_at = latest_close_by_test.get(full_name)
            if prev_closed_at is None or closed_at > prev_closed_at:
                latest_close_by_test[full_name] = closed_at

    return latest_close_by_test, stats


def compute_user_fixed_quarantine_actions(
    latest_close_by_test: Dict[str, datetime.datetime],
    full_name_to_mute_strings: Dict[str, Set[str]],
    unmute_candidates: Set[str],
    quarantine_days: int,
    now_utc: Optional[datetime.datetime] = None,
) -> Dict[str, object]:
    now_utc = now_utc or datetime.datetime.now(datetime.timezone.utc)
    hide: Set[str] = set()
    restore: Set[str] = set()
    stable: Set[str] = set()
    hide_debug: List[str] = []
    restore_debug: List[str] = []
    stable_debug: List[str] = []

    for full_name, closed_at in latest_close_by_test.items():
        mute_strings = set(full_name_to_mute_strings.get(full_name) or set())
        if not mute_strings:
            fallback = fallback_mute_string_from_full_name(full_name)
            if fallback:
                mute_strings.add(fallback)
        if not mute_strings:
            continue

        age_days = max(0, (now_utc.date() - closed_at.date()).days)
        if age_days < quarantine_days:
            hide.update(mute_strings)
            for mute_str in sorted(mute_strings):
                hide_debug.append(
                    f"{mute_str} # quarantine_user_fixed active: closed {age_days}d ago, window={quarantine_days}d"
                )
        elif full_name in unmute_candidates:
            stable.update(mute_strings)
            for mute_str in sorted(mute_strings):
                stable_debug.append(
                    f"{mute_str} # quarantine_user_fixed passed: window ended and default unmute rule passed"
                )
        else:
            restore.update(mute_strings)
            for mute_str in sorted(mute_strings):
                restore_debug.append(
                    f"{mute_str} # quarantine_user_fixed expired without unmute conditions, restoring to muted"
                )

    return {
        "hide": hide,
        "restore": restore,
        "stable": stable,
        "hide_debug": sorted(hide_debug),
        "restore_debug": sorted(restore_debug),
        "stable_debug": sorted(stable_debug),
        "stats": {"linked_tests": len(latest_close_by_test)},
    }


def build_final_muted_ya_list(
    base_muted_ya: List[str],
    to_mute: List[str],
    to_unmute: Set[str],
    to_delete: Set[str],
    quarantine_hide: Set[str],
    quarantine_restore: Set[str],
) -> List[str]:
    overlap = quarantine_hide & quarantine_restore
    if overlap:
        raise ValueError(
            f"Invalid quarantine state: hide/restore overlap for {len(overlap)} tests"
        )

    base_minus = [
        name
        for name in base_muted_ya
        if name not in to_delete and name not in to_unmute and name not in quarantine_hide
    ]
    result = list(base_minus)
    current = set(base_minus)

    for name in to_mute:
        if name not in current and name not in quarantine_hide:
            result.append(name)
            current.add(name)

    for name in sorted(quarantine_restore):
        if name not in current:
            result.append(name)
            current.add(name)

    hidden_intersection = quarantine_hide & set(result)
    if hidden_intersection:
        raise ValueError(
            f"Invalid final muted_ya: hidden tests leaked into output ({len(hidden_intersection)})"
        )

    return result


def _build_unmute_candidates(
    aggregated_for_unmute: List[dict],
    thresholds: Dict[str, int],
) -> Set[str]:
    candidates: Set[str] = set()
    for test in aggregated_for_unmute:
        full_name = test.get("full_name")
        if not full_name:
            continue
        pass_count = int(test.get("pass_count", 0) or 0)
        fail_count = int(test.get("fail_count", 0) or 0)
        mute_count = int(test.get("mute_count", 0) or 0)
        if passes_default_unmute(pass_count, fail_count, mute_count, thresholds):
            candidates.add(str(full_name))
    return candidates


def resolve_user_fixed_quarantine_actions(
    ydb_wrapper,
    branch: str,
    build_type: str,
    all_data: List[dict],
    aggregated_for_unmute: List[dict],
    thresholds: Dict[str, int],
    now_utc: Optional[datetime.datetime] = None,
) -> Dict[str, object]:
    merged_thresholds = dict(DEFAULT_THRESHOLDS)
    merged_thresholds.update({k: int(v) for k, v in dict(thresholds or {}).items()})
    quarantine_days = int(merged_thresholds["quarantine_user_fixed_window_days"])
    lookback_days = max(quarantine_days * 3, 30)
    unmute_candidates = _build_unmute_candidates(aggregated_for_unmute, merged_thresholds)

    try:
        issues_table = ydb_wrapper.get_table_path("issues")
    except Exception:
        out = empty_quarantine_actions()
        out["stats"] = {"error": "issues_table_path_unavailable"}
        return out

    query = f"""
    SELECT issue_number, body, closed_at, closed_by_type, state_reason
    FROM `{issues_table}`
    WHERE state = 'CLOSED'
      AND closed_at IS NOT NULL
      AND closed_at >= CurrentUtcTimestamp() - {lookback_days} * Interval("P1D")
    """
    try:
        rows = ydb_wrapper.execute_scan_query(
            query,
            query_name=f"user_fixed_quarantine_candidates_{branch}_{build_type}",
        )
    except Exception:
        out = empty_quarantine_actions()
        out["stats"] = {"error": "issues_query_failed"}
        return out

    latest_close_by_test, extract_stats = extract_latest_user_closed_tests(
        rows=rows,
        branch=branch,
        build_type=build_type,
    )
    full_name_to_mute_strings = build_full_name_to_mute_strings(all_data)
    actions = compute_user_fixed_quarantine_actions(
        latest_close_by_test=latest_close_by_test,
        full_name_to_mute_strings=full_name_to_mute_strings,
        unmute_candidates=unmute_candidates,
        quarantine_days=quarantine_days,
        now_utc=now_utc,
    )
    actions["stats"] = {
        **extract_stats,
        "actions_hide": len(actions["hide"]),
        "actions_restore": len(actions["restore"]),
        "actions_stable": len(actions["stable"]),
    }
    return actions


def _extract_test_name_from_debug_line(debug_line: str) -> str:
    return str(debug_line).split(" #", 1)[0]


def _filter_debug_lines_by_tests(debug_lines: List[str], allowed_tests: Set[str]) -> List[str]:
    return [line for line in debug_lines if _extract_test_name_from_debug_line(line) in allowed_tests]


def _build_quarantine_debug_map(quarantine_actions: Dict[str, object]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for debug_line in (
        list(quarantine_actions.get("hide_debug") or [])
        + list(quarantine_actions.get("restore_debug") or [])
        + list(quarantine_actions.get("stable_debug") or [])
    ):
        test_name = _extract_test_name_from_debug_line(str(debug_line))
        result[test_name] = str(debug_line)
    return result


def apply_quarantine_actions(
    to_unmute: List[str],
    to_delete: List[str],
    to_unmute_debug: List[str],
    to_delete_debug: List[str],
    quarantine_actions: Optional[Dict[str, object]],
) -> Dict[str, object]:
    quarantine_actions = quarantine_actions or empty_quarantine_actions()
    quarantine_hide_set = set(quarantine_actions.get("hide") or set())
    quarantine_restore_set = set(quarantine_actions.get("restore") or set())
    quarantine_stable_set = set(quarantine_actions.get("stable") or set())
    quarantine_debug_map = _build_quarantine_debug_map(quarantine_actions)

    to_unmute_set = set(to_unmute) - quarantine_hide_set
    to_delete_set = set(to_delete) - quarantine_hide_set
    filtered_unmute_debug = _filter_debug_lines_by_tests(to_unmute_debug, to_unmute_set)
    filtered_delete_debug = _filter_debug_lines_by_tests(to_delete_debug, to_delete_set)

    return {
        "to_unmute": sorted(to_unmute_set),
        "to_delete": sorted(to_delete_set),
        "to_unmute_debug": filtered_unmute_debug,
        "to_delete_debug": filtered_delete_debug,
        "quarantine_hide_set": quarantine_hide_set,
        "quarantine_restore_set": quarantine_restore_set,
        "quarantine_stable_set": quarantine_stable_set,
        "quarantine_debug_map": quarantine_debug_map,
    }


def finalize_new_muted_ya(
    all_muted_ya: List[str],
    to_delete: List[str],
    to_unmute: List[str],
    to_mute: List[str],
    quarantine_hide_set: Set[str],
    quarantine_restore_set: Set[str],
) -> List[str]:
    return build_final_muted_ya_list(
        base_muted_ya=all_muted_ya,
        to_mute=to_mute,
        to_unmute=set(to_unmute),
        to_delete=set(to_delete),
        quarantine_hide=quarantine_hide_set,
        quarantine_restore=quarantine_restore_set,
    )


# Backward-compatible aliases for callers that imported the old names during
# intermediate refactors.
def latest_user_closed_at_by_test(
    rows: List[dict],
    branch: str,
    build_type: str,
    parse_body_fn=parse_body,
    default_build_type: str = DEFAULT_BUILD_TYPE,
    normalize_utc_datetime_fn=normalize_utc_datetime,
) -> Dict[str, datetime.datetime]:
    _ = parse_body_fn
    _ = default_build_type
    _ = normalize_utc_datetime_fn
    latest, _stats = extract_latest_user_closed_tests(rows, branch, build_type)
    return latest


def classify_quarantine_actions_for_closed_tests(
    latest_close_by_test: Dict[str, datetime.datetime],
    unmute_candidates: Set[str],
    full_name_to_mute_strings: Dict[str, Set[str]],
    quarantine_days: int,
    fallback_mute_string_fn=fallback_mute_string_from_full_name,
) -> Dict[str, object]:
    _ = fallback_mute_string_fn
    return compute_user_fixed_quarantine_actions(
        latest_close_by_test=latest_close_by_test,
        full_name_to_mute_strings=full_name_to_mute_strings,
        unmute_candidates=unmute_candidates,
        quarantine_days=quarantine_days,
    )
