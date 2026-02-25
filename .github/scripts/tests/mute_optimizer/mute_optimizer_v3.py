#!/usr/bin/env python3
"""
Mute threshold optimizer v3.

Отличия от v1:
1) Локальный кэш — данные скачиваются в JSON, при перезапуске берутся из кэша
2) Источник: test_runs_column (raw runs), а не tests_monitor
3) Измерение job_filter: wf_only (только Nightly/Regression/Postcommit) vs wf_and_pr (+ PR-check с коэффициентом)
4) Измерение update_interval: как редко можно обновлять mute без потери эффективности (1,2,3,5,7 дней)
"""
import argparse
import datetime
import hashlib
import json
import logging
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'analytics'))
from ydb_wrapper import YDBWrapper

# WF jobs from flaky_tests_history.py (lines 171-180)
WF_JOB_NAMES = (
    'Nightly-run',
    'Regression-run',
    'Regression-run_Large',
    'Regression-run_Small_and_Medium',
    'Regression-run_compatibility',
    'Regression-whitelist-run',
    'Postcommit_relwithdebinfo',
    'Postcommit_asan',
)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent / "mute_optimizer_cache"

# If a muted test has no runs for this many days, unmute it (test may be removed).
# Match create_new_muted_ya.py DELETE_DAYS=7.
STALE_UNMUTE_DAYS = 7


@dataclass
class ThresholdConfig:
    mute_days: int
    mute_fail_threshold: int
    unmute_days: int
    unmute_min_runs: int
    window_type: str = 'days'        # 'days' or 'runs'
    mute_last_runs: int = 20         # for window_type='runs': look at last N runs for mute
    unmute_last_runs: int = 20       # for window_type='runs': look at last N runs for unmute
    mute_fail_threshold_low_runs: Optional[int] = None  # threshold when total_runs <= low_runs_bound
    low_runs_bound: int = 10         # boundary: if runs <= this, use mute_fail_threshold_low_runs

    def __str__(self):
        if self.window_type == 'runs':
            return (
                f"MUTE(last_runs={self.mute_last_runs}, f>={self.mute_fail_threshold}) "
                f"UNMUTE(last_runs={self.unmute_last_runs}, 0 fails)"
            )
        return (
            f"MUTE(d={self.mute_days}, f>={self.mute_fail_threshold}) "
            f"UNMUTE(d={self.unmute_days}, r>={self.unmute_min_runs})"
        )


@dataclass
class SimulatedState:
    muted: bool
    date: datetime.date


@dataclass
class Metrics:
    volatility: float
    avg_time_to_mute: Optional[float]
    avg_time_to_unmute: Optional[float]
    n_mute_transitions: int
    n_unmute_transitions: int

    def score(self, w_volatility: float = 1.0, w_mute: float = 1.0, w_unmute: float = 1.0) -> float:
        v = self.volatility
        m = self.avg_time_to_mute if self.avg_time_to_mute is not None else 0
        u = self.avg_time_to_unmute if self.avg_time_to_unmute is not None else 0
        return w_volatility * v + w_mute * m + w_unmute * u


def is_chunk_test(full_name: str) -> bool:
    """Match create_new_muted_ya: chunk tests have [N/M] in name (e.g. [1/100])."""
    return bool(re.search(r'\[\d+/\d+\]', full_name or ''))


def get_chunk_wildcard_key(full_name: str) -> Optional[str]:
    """Return wildcard key [*/*] for chunk grouping, or None if not a chunk."""
    if not is_chunk_test(full_name):
        return None
    return re.sub(r'\[\d+/\d+\]', '[*/*]', full_name)


def build_chunk_groups(keys: List[Tuple[str, str]]) -> Dict[str, List[Tuple[str, str]]]:
    """Group (full_name, branch) by wildcard key. Only chunk tests are grouped."""
    groups: Dict[str, List[Tuple[str, str]]] = {}
    for fn, br in keys:
        wk = get_chunk_wildcard_key(fn)
        if wk is not None:
            key = (wk, br)
            if key not in groups:
                groups[key] = []
            groups[key].append((fn, br))
    return {k: v for k, v in groups.items() if len(v) > 1}  # only groups with 2+ chunks


def to_days(d) -> int:
    base = datetime.date(1970, 1, 1)
    if d is None:
        return -1
    if isinstance(d, datetime.date):
        return (d - base).days
    if hasattr(d, 'days'):
        return int(d.days)
    return int(d)


def _cache_path(branch: str, build_type: str, days: int) -> Path:
    key = f"{branch}_{build_type}_{days}"
    h = hashlib.sha256(key.encode()).hexdigest()[:16]
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"mute_opt_{h}.json"


def _cache_path_pr_merged(branch: str, build_type: str, days: int) -> Path:
    key = f"{branch}_{build_type}_{days}_pr_merged"
    h = hashlib.sha256(key.encode()).hexdigest()[:16]
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"mute_opt_pr_merged_{h}.json"


def load_cached_pr_merged(branch: str, build_type: str, days: int) -> Optional[List[Dict]]:
    path = _cache_path_pr_merged(branch, build_type, days)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        if isinstance(data, dict) and 'rows' in data:
            if data.get('branch') != branch or data.get('build_type') != build_type or data.get('days') != days:
                return None
            return data['rows']
        return data if isinstance(data, list) else None
    except (json.JSONDecodeError, IOError):
        return None


def save_cache_pr_merged(branch: str, build_type: str, days: int, rows: List[Dict]) -> None:
    path = _cache_path_pr_merged(branch, build_type, days)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    serializable = [_row_to_json_serializable(r) for r in rows]
    with open(path, 'w') as f:
        json.dump({'rows': serializable, 'branch': branch, 'build_type': build_type, 'days': days}, f, indent=0)
    logger.info(f"Cached PR merged failures {len(rows)} rows to {path}")


def fetch_pr_last_run_merged_failures(
    branch: str, build_type: str, days: int, no_cache: bool = False
) -> List[Dict]:
    """
    Full test results from last PR-check run per merged PR.
    Returns rows: full_name, branch, date_window, pass_count, fail_count, mute_count, skip_count.
    """
    if not no_cache:
        cached = load_cached_pr_merged(branch, build_type, days)
        if cached:
            logger.info(f"Using cached PR merged data ({len(cached)} rows)")
            return cached

    try:
        with YDBWrapper(silent=True) as ydb_wrapper:
            if not ydb_wrapper.check_credentials():
                return []
            tr_table = ydb_wrapper.get_table_path("test_results")
            pr_table = ydb_wrapper.get_table_path("pull_requests")
            query = f"""
            -- Only merged PRs targeting our branch (pr_target_branch = base_ref_name, like pr_with_test_failures.sql)
            $merged = (
                SELECT pr_number FROM (
                    SELECT pr_number, ROW_NUMBER() OVER (PARTITION BY pr_number ORDER BY exported_at DESC) AS rn
                    FROM `{pr_table}`
                    WHERE merged = 1 AND COALESCE(base_ref_name, '') = '{branch}'
                ) WHERE rn = 1
            );
            $runs = (
                SELECT job_id, run_timestamp, suite_folder, test_name, branch, status,
                    ListHead(Unicode::SplitToList(ListHead(ListSkip(Unicode::SplitToList(CAST(pull AS UTF8), 'PR_'), 1)), '_')) AS pr_num
                FROM `{tr_table}`
                WHERE job_name = 'PR-check' AND build_type = '{build_type}'
                  AND run_timestamp >= CurrentUtcDate() - {days} * Interval("P1D")
                  AND pull IS NOT NULL AND pull != '' AND String::Contains(CAST(pull AS UTF8), 'PR_')
            );
            -- Last job per PR (as in pr_blocked_by_failed_tests_rich.sql: MAX_BY(job_id, run_timestamp))
            $all_pr_runs = (
                SELECT pr_num, job_id, MAX(run_timestamp) AS run_ts
                FROM $runs GROUP BY pr_num, job_id
            );
            $last_job_per_pr = (
                SELECT pr_num, MAX_BY(job_id, run_ts) AS last_job_id
                FROM $all_pr_runs GROUP BY pr_num
            );
            $merged_last_job = (
                SELECT lj.last_job_id AS job_id
                FROM $last_job_per_pr AS lj
                INNER JOIN $merged AS m ON CAST(m.pr_number AS UTF8) = lj.pr_num
            );
            -- All attempts from the last job (no run_timestamp filter, unlike pr_with_test_failures which uses attempt 3 only)
            -- Branch = pr_target_branch (base_ref_name), like pr_blocked_by_failed_tests_rich_with_pr_and_mute.sql
            $joined = (
                SELECT
                    r.suite_folder || '/' || r.test_name AS full_name,
                    '{branch}' AS branch,
                    Cast(r.run_timestamp AS Date) AS date_window,
                    r.status AS status
                FROM $runs AS r
                INNER JOIN $merged_last_job AS ml ON r.job_id = ml.job_id
            );
            SELECT
                full_name, branch, date_window,
                countIf(status = 'passed') AS pass_count,
                countIf(status IN ('failure', 'error')) AS fail_count,
                countIf(status = 'mute') AS mute_count,
                countIf(status = 'skipped') AS skip_count
            FROM $joined
            GROUP BY full_name, branch, date_window
            """
            rows = list(ydb_wrapper.execute_scan_query(query, query_name="mute_optimizer_v3_pr_merged"))
    except Exception as e:
        logger.warning(f"PR merged query failed (use wf_only?): {e}")
        return []

    base = datetime.date(1970, 1, 1)
    for r in rows:
        for k in ('full_name', 'branch'):
            v = r.get(k)
            if isinstance(v, bytes):
                r[k] = v.decode('utf-8')
        dw = r.get('date_window')
        if hasattr(dw, 'days'):
            r['date_window'] = base + datetime.timedelta(days=int(dw.days))
        elif isinstance(dw, int):
            r['date_window'] = base + datetime.timedelta(days=dw)

    if rows:
        save_cache_pr_merged(branch, build_type, days, rows)
    return rows


def load_cached(branch: str, build_type: str, days: int) -> Optional[List[Dict]]:
    """Load aggregated rows from cache if exists and matches params."""
    path = _cache_path(branch, build_type, days)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        if isinstance(data, dict) and 'rows' in data:
            if data.get('branch') != branch or data.get('build_type') != build_type or data.get('days') != days:
                return None
            return data['rows']
        return data if isinstance(data, list) else None
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Cache read failed: {e}")
        return None


def _row_to_json_serializable(row: Dict) -> Dict:
    """Convert YDB row values (bytes, date, etc.) to JSON-serializable types."""
    out = {}
    base = datetime.date(1970, 1, 1)
    for k, v in row.items():
        if v is None:
            out[k] = None
        elif isinstance(v, bytes):
            out[k] = v.decode('utf-8', errors='replace')
        elif isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
            out[k] = (v - base).days
        elif isinstance(v, datetime.datetime):
            out[k] = v.isoformat()
        elif hasattr(v, 'days'):
            out[k] = int(v.days)
        elif isinstance(v, (str, int, float, bool)):
            out[k] = v
        else:
            out[k] = v
    return out


def save_cache(branch: str, build_type: str, days: int, rows: List[Dict]) -> None:
    path = _cache_path(branch, build_type, days)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    serializable = [_row_to_json_serializable(r) for r in rows]
    payload = {'rows': serializable, 'branch': branch, 'build_type': build_type, 'days': days}
    with open(path, 'w') as f:
        json.dump(payload, f, indent=0)
    logger.info(f"Cached {len(rows)} rows to {path}")


def fetch_from_test_runs_column(
    branch: str, build_type: str, days: int, no_cache: bool = False
) -> List[Dict]:
    """
    Fetch aggregated pass/fail/mute per (full_name, branch, date_window, job_type).
    job_type: 'wf' = WF jobs, 'pr' = PR-check.
    """
    if not no_cache:
        cached = load_cached(branch, build_type, days)
        if cached:
            logger.info(f"Using cached data ({len(cached)} rows)")
            return cached

    try:
        with YDBWrapper(silent=True) as ydb_wrapper:
            if not ydb_wrapper.check_credentials():
                return []
            table = ydb_wrapper.get_table_path("test_results")
            wf_list = "', '".join(WF_JOB_NAMES)
            query = f"""
            SELECT
                suite_folder || '/' || test_name AS full_name,
                branch,
                Cast(Max(run_timestamp) AS Date) AS date_window,
                CASE
                    WHEN job_name IN ('{wf_list}') THEN 'wf'
                    WHEN job_name = 'PR-check' THEN 'pr'
                    ELSE 'other'
                END AS job_type,
                countIf(status = 'passed') AS pass_count,
                countIf(status IN ('failure', 'error')) AS fail_count,
                countIf(status = 'mute') AS mute_count,
                countIf(status = 'skipped') AS skip_count
            FROM `{table}`
            WHERE run_timestamp >= CurrentUtcDate() - {days} * Interval("P1D")
              AND branch = '{branch}'
              AND build_type = '{build_type}'
              AND test_name NOT IN ('unittest', 'py3test', 'gtest')
              AND (job_name IN ('{wf_list}') OR job_name = 'PR-check')
            GROUP BY suite_folder, test_name, branch, Cast(run_timestamp AS Date), job_name
            """
            rows = list(ydb_wrapper.execute_scan_query(query, query_name="mute_optimizer_v3"))
    except Exception as e:
        logger.error(f"Failed to fetch: {e}")
        return []

    # Normalize: decode bytes from YDB, fix date_window
    base = datetime.date(1970, 1, 1)
    for r in rows:
        for k in ('full_name', 'branch', 'job_type'):
            v = r.get(k)
            if isinstance(v, bytes):
                r[k] = v.decode('utf-8')
        dw = r.get('date_window')
        if hasattr(dw, 'days'):
            r['date_window'] = base + datetime.timedelta(days=int(dw.days))
        elif isinstance(dw, int):
            r['date_window'] = base + datetime.timedelta(days=dw)

    if rows:
        save_cache(branch, build_type, days, rows)

    return rows


@dataclass
class PrecomputedAggregates:
    """Pre-computed per (full_name, branch, date) — WF and PR parts separately."""
    # key = (full_name, branch, days_since_epoch)
    wf: Dict[Tuple[str, str, int], Dict]            # wf_pass, wf_fail, wf_mute, wf_skip
    pr: Dict[Tuple[str, str, int], Dict]             # pr_pass, pr_fail, pr_mute, pr_skip
    pr_merged: Dict[Tuple[str, str, int], Dict]       # {pr_pass, pr_fail, pr_mute, pr_skip} from merged PRs
    date_windows: Dict[Tuple[str, str, int], Any]    # date_window objects for reconstruction


def precompute_base_aggregates(
    rows: List[Dict],
    pr_merged_rows: Optional[List[Dict]] = None,
) -> PrecomputedAggregates:
    """One-time O(N) pass over raw rows. Call once, reuse across all trials."""
    wf: Dict[Tuple[str, str, int], Dict] = {}
    pr: Dict[Tuple[str, str, int], Dict] = {}
    date_windows: Dict[Tuple[str, str, int], Any] = {}

    for r in rows:
        jt = r.get('job_type', 'other')
        if isinstance(jt, bytes):
            jt = jt.decode('utf-8')
        if jt == 'other':
            continue
        fn, br = r['full_name'], r['branch']
        d = to_days(r.get('date_window'))
        k = (fn, br, d)
        date_windows.setdefault(k, r.get('date_window'))
        if jt == 'wf':
            if k not in wf:
                wf[k] = {'wf_pass': 0, 'wf_fail': 0, 'wf_mute': 0, 'wf_skip': 0}
            wf[k]['wf_pass'] += r.get('pass_count') or 0
            wf[k]['wf_fail'] += r.get('fail_count') or 0
            wf[k]['wf_mute'] += r.get('mute_count') or 0
            wf[k]['wf_skip'] += r.get('skip_count') or 0
        elif jt == 'pr':
            if k not in pr:
                pr[k] = {'pr_pass': 0, 'pr_fail': 0, 'pr_mute': 0, 'pr_skip': 0}
            pr[k]['pr_pass'] += r.get('pass_count') or 0
            pr[k]['pr_fail'] += r.get('fail_count') or 0
            pr[k]['pr_mute'] += r.get('mute_count') or 0
            pr[k]['pr_skip'] += r.get('skip_count') or 0

    pr_merged_agg: Dict[Tuple[str, str, int], Dict] = {}
    if pr_merged_rows:
        base_date = datetime.date(1970, 1, 1)
        for r in pr_merged_rows:
            k = (r['full_name'], r['branch'], to_days(r.get('date_window')))
            if k not in pr_merged_agg:
                pr_merged_agg[k] = {'pr_pass': 0, 'pr_fail': 0, 'pr_mute': 0, 'pr_skip': 0}
            pr_merged_agg[k]['pr_pass'] += r.get('pass_count') or r.get('pr_pass') or 0
            pr_merged_agg[k]['pr_fail'] += r.get('fail_count') or r.get('pr_fail_count') or r.get('pr_fail') or 0
            pr_merged_agg[k]['pr_mute'] += r.get('mute_count') or r.get('pr_mute') or 0
            pr_merged_agg[k]['pr_skip'] += r.get('skip_count') or r.get('pr_skip') or 0
            # Ensure date_windows has entries for pr_merged-only keys (BUG-3 fix)
            if k not in date_windows:
                dw = r.get('date_window')
                if dw is None:
                    dw = base_date + datetime.timedelta(days=to_days(r.get('date_window', 0)) or 0)
                elif isinstance(dw, int):
                    dw = base_date + datetime.timedelta(days=dw)
                date_windows[k] = dw

    return PrecomputedAggregates(wf=wf, pr=pr, pr_merged=pr_merged_agg, date_windows=date_windows)


def build_daily_fast(
    pre: PrecomputedAggregates,
    job_filter: str,
    use_pr_merged: bool = False,
) -> Dict[Tuple[str, str], List[Dict]]:
    """Fast merge: O(unique keys) instead of O(9.4M rows). Uses pre-computed aggregates.
    job_filter: 'wf_only' or 'wf_and_pr'. When wf_and_pr and use_pr_merged=True, adds merged PR data 1:1."""
    all_keys = set(pre.wf.keys())
    if job_filter == 'wf_and_pr' and use_pr_merged:
        all_keys |= set(pre.pr_merged.keys())

    by_key_date: Dict[Tuple[str, str, int], Dict] = {}
    for k in all_keys:
        fn, br, d = k
        if k not in by_key_date:
            by_key_date[k] = {
                'full_name': fn, 'branch': br,
                'date_window': pre.date_windows.get(k),
                'pass_count': 0, 'fail_count': 0, 'mute_count': 0, 'skip_count': 0,
                'total_runs': 0, 'total_fails': 0, 'is_muted': 0,
            }
        out = by_key_date[k]
        w = pre.wf.get(k)
        if w:
            out['pass_count'] += w['wf_pass']
            out['fail_count'] += w['wf_fail']
            out['mute_count'] += w['wf_mute']
            out['skip_count'] += w['wf_skip']

        if job_filter == 'wf_and_pr' and use_pr_merged:
            p = pre.pr_merged.get(k)
            if p:
                out['pass_count'] += p['pr_pass']
                out['fail_count'] += p['pr_fail']
                out['mute_count'] += p['pr_mute']
                out['skip_count'] += p['pr_skip']

        out['total_runs'] = out['pass_count'] + out['fail_count'] + out['mute_count']
        out['total_fails'] = out['fail_count'] + out['mute_count']

    daily: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
    for (fn, br, d), row in by_key_date.items():
        daily[(fn, br)].append(row)
    for key in daily:
        daily[key].sort(key=lambda x: to_days(x.get('date_window')))
    return dict(daily)


def compute_pr_blocking_metric(
    simulation: Dict[Tuple[str, str], List[SimulatedState]],
    date_list: List[datetime.date],
    pre: PrecomputedAggregates,
) -> float:
    """
    PR blocking quality: fraction of PR fail events where we were NOT muted (lower = better).
    For each (test, date) where a merged PR's last run had failures, check: was the test
    muted in our simulation? If not, we "missed" — could have blocked PRs.
    Returns 0 when pre.pr_merged is empty (no PR data to evaluate).
    """
    if not pre.pr_merged:
        return 0.0
    date_to_idx = {to_days(d): i for i, d in enumerate(date_list)}
    total = 0
    missed = 0
    for k, agg in pre.pr_merged.items():
        pr_fail = agg.get('pr_fail') or 0
        if pr_fail <= 0:
            continue
        fn, br, d = k
        key = (fn, br)
        if key not in simulation:
            continue
        idx = date_to_idx.get(d)
        if idx is None:
            continue
        history = simulation[key]
        if idx >= len(history):
            continue
        if not history[idx].muted:
            missed += 1
        total += 1
    return missed / total if total > 0 else 0.0


def build_daily_aggregates(
    rows: List[Dict],
    job_filter: str,
    pr_merged_rows: Optional[List[Dict]] = None,
) -> Dict[Tuple[str, str], List[Dict]]:
    """Original interface — uses precompute + fast merge internally.
    job_filter: 'wf_only' or 'wf_and_pr'. When wf_and_pr, uses pr_merged (1:1) if pr_merged_rows provided."""
    pre = precompute_base_aggregates(rows, pr_merged_rows)
    use_pr = job_filter == 'wf_and_pr' and bool(pr_merged_rows)
    return build_daily_fast(pre, job_filter, use_pr_merged=use_pr)


def aggregate_for_window(
    daily: Dict[Tuple[str, str], List[Dict]], full_name: str, branch: str, end_date: datetime.date, n_days: int
) -> Dict[str, int]:
    end_days = to_days(end_date)
    start_days = end_days - n_days + 1
    key = (full_name, branch)
    rows = daily.get(key, [])
    p, f, m, s = 0, 0, 0, 0
    is_muted = 0
    for r in rows:
        d = to_days(r.get('date_window'))
        if start_days <= d <= end_days:
            p += r.get('pass_count') or 0
            f += r.get('fail_count') or 0
            m += r.get('mute_count') or 0
            s += r.get('skip_count') or 0
    total_runs = p + f + m
    total_fails = f + m
    return {
        'pass_count': p, 'fail_count': f, 'mute_count': m, 'skip_count': s,
        'total_runs': total_runs, 'total_fails': total_fails, 'is_muted': is_muted,
    }


def aggregate_for_window_by_runs(
    daily: Dict[Tuple[str, str], List[Dict]], full_name: str, branch: str, end_date: datetime.date, n_runs: int
) -> Dict[str, int]:
    """Aggregate the last n_runs runs (by total_runs per day) up to end_date, regardless of how many days back.
    Assumes rows in daily are already sorted by date ascending."""
    end_days = to_days(end_date)
    key = (full_name, branch)
    rows = daily.get(key, [])
    p, f, m, s = 0, 0, 0, 0
    accumulated_runs = 0
    # Walk backwards (rows sorted ascending, so iterate in reverse)
    for i in range(len(rows) - 1, -1, -1):
        r = rows[i]
        if to_days(r.get('date_window')) > end_days:
            continue
        rp = r.get('pass_count') or 0
        rf = r.get('fail_count') or 0
        rm = r.get('mute_count') or 0
        rs = r.get('skip_count') or 0
        day_runs = rp + rf + rm
        p += rp
        f += rf
        m += rm
        s += rs
        accumulated_runs += day_runs
        if accumulated_runs >= n_runs:
            break
    total_runs = p + f + m
    total_fails = f + m
    return {
        'pass_count': p, 'fail_count': f, 'mute_count': m, 'skip_count': s,
        'total_runs': total_runs, 'total_fails': total_fails, 'is_muted': 0,
    }


def aggregate_for_window_hybrid(
    daily: Dict[Tuple[str, str], List[Dict]], full_name: str, branch: str,
    end_date: datetime.date, n_runs: int, max_days: int,
) -> Dict[str, int]:
    """Hybrid: last n_runs runs, but only within last max_days days.
    Assumes rows sorted ascending by date."""
    end_days = to_days(end_date)
    start_days = end_days - max_days + 1
    key = (full_name, branch)
    rows = daily.get(key, [])
    p, f, m, s = 0, 0, 0, 0
    accumulated_runs = 0
    for i in range(len(rows) - 1, -1, -1):
        r = rows[i]
        d = to_days(r.get('date_window'))
        if d > end_days:
            continue
        if d < start_days:
            break  # sorted ascending, earlier days won't qualify
        rp = r.get('pass_count') or 0
        rf = r.get('fail_count') or 0
        rm = r.get('mute_count') or 0
        rs = r.get('skip_count') or 0
        day_runs = rp + rf + rm
        p += rp
        f += rf
        m += rm
        s += rs
        accumulated_runs += day_runs
        if accumulated_runs >= n_runs:
            break
    total_runs = p + f + m
    total_fails = f + m
    return {
        'pass_count': p, 'fail_count': f, 'mute_count': m, 'skip_count': s,
        'total_runs': total_runs, 'total_fails': total_fails, 'is_muted': 0,
    }


def would_mute(cfg: ThresholdConfig, agg: Dict, currently_muted: bool) -> bool:
    """Match create_new_muted_ya.py: total_runs for runs threshold = pass+fail only (exclude mute_count)."""
    if currently_muted:
        return True
    pass_count = agg.get('pass_count', 0)
    fail_count = agg.get('fail_count', 0)
    total_runs_for_threshold = pass_count + fail_count  # prod uses pass+fail, not pass+fail+mute
    if cfg.mute_fail_threshold_low_runs is not None and total_runs_for_threshold <= cfg.low_runs_bound:
        return fail_count >= cfg.mute_fail_threshold_low_runs
    return fail_count >= cfg.mute_fail_threshold


def would_unmute(cfg: ThresholdConfig, agg: Dict, currently_muted: bool) -> bool:
    if not currently_muted:
        return False
    if cfg.window_type in ('runs', 'hybrid'):
        return agg['total_runs'] > 0 and agg['total_fails'] == 0
    return agg['total_runs'] >= cfg.unmute_min_runs and agg['total_fails'] == 0


def days_since_last_run(
    daily: Dict[Tuple[str, str], List[Dict]], full_name: str, branch: str, end_date: datetime.date
) -> int:
    """
    Days since the last run (date with any activity) up to end_date.
    Match create_new_muted_ya.py is_delete_candidate: total_runs = pass+fail+mute+skip.
    Returns 999 if no runs at all (test removed/deleted).
    """
    end_days = to_days(end_date)
    key = (full_name, branch)
    rows = daily.get(key, [])
    for i in range(len(rows) - 1, -1, -1):
        r = rows[i]
        d = to_days(r.get('date_window'))
        if d > end_days:
            continue
        day_runs = (
            (r.get('pass_count') or 0) + (r.get('fail_count') or 0)
            + (r.get('mute_count') or 0) + (r.get('skip_count') or 0)
        )
        if day_runs > 0:
            return end_days - d
    return 999  # no runs at all


def get_mute_window_agg(
    daily: Dict[Tuple[str, str], List[Dict]], full_name: str, branch: str,
    d: datetime.date, config: ThresholdConfig,
) -> Dict[str, int]:
    """Return agg for mute decision window (last N runs/days) — for display in reports."""
    agg_mute, _ = _get_aggregates(daily, full_name, branch, d, config)
    return agg_mute


def _get_aggregates(
    daily: Dict[Tuple[str, str], List[Dict]], full_name: str, branch: str,
    d: datetime.date, config: ThresholdConfig,
) -> Tuple[Dict, Dict]:
    """Return (agg_mute, agg_unmute) using the right window type."""
    if config.window_type == 'runs':
        agg_mute = aggregate_for_window_by_runs(daily, full_name, branch, d, config.mute_last_runs)
        agg_unmute = aggregate_for_window_by_runs(daily, full_name, branch, d, config.unmute_last_runs)
    elif config.window_type == 'hybrid':
        agg_mute = aggregate_for_window_hybrid(
            daily, full_name, branch, d, config.mute_last_runs, config.mute_days)
        agg_unmute = aggregate_for_window_hybrid(
            daily, full_name, branch, d, config.unmute_last_runs, config.unmute_days)
    else:
        agg_mute = aggregate_for_window(daily, full_name, branch, d, config.mute_days)
        agg_unmute = aggregate_for_window(daily, full_name, branch, d, config.unmute_days)
    return agg_mute, agg_unmute


def _apply_chunk_wildcard_rules(
    result: Dict[Tuple[str, str], List[SimulatedState]],
    keys: List[Tuple[str, str]],
    n_days: int,
) -> None:
    """
    Match create_new_muted_ya: if any chunk mutes → mute [*/*]; unmute only when all pass.
    Overwrites result in place.
    """
    groups = build_chunk_groups(keys)
    if not groups:
        return
    for (wk, br), member_keys in groups.items():
        for day_idx in range(n_days):
            group_muted = any(result.get((fn, br), [])[day_idx].muted for fn, _ in member_keys if (fn, br) in result)
            for fn, b in member_keys:
                key = (fn, b)
                if key in result and day_idx < len(result[key]):
                    result[key][day_idx] = SimulatedState(muted=group_muted, date=result[key][day_idx].date)


def simulate(
    daily: Dict[Tuple[str, str], List[Dict]],
    dates: List[datetime.date],
    config: ThresholdConfig,
    update_interval_days: int = 1,
    show_progress: bool = False,
    chunk_wildcard_rules: bool = False,
) -> Dict[Tuple[str, str], List[SimulatedState]]:
    """
    Simulate mute/unmute. If update_interval_days > 1, decisions are made only on days 0, N, 2N, ...
    Supports window_type='days' (default) and window_type='runs'.
    chunk_wildcard_rules: if True, match create_new_muted_ya — one chunk mutes → mute [*/*]; unmute only when all pass.
    """
    result: Dict[Tuple[str, str], List[SimulatedState]] = defaultdict(list)
    base = to_days(dates[0]) if dates else 0

    keys_iter = list(daily.keys())
    if show_progress and HAS_TQDM:
        keys_iter = tqdm(keys_iter, desc="Simulate", unit="test", leave=False, total=len(daily))

    for key in keys_iter:
        full_name, branch = key
        history: List[SimulatedState] = []
        prev_muted: Optional[bool] = None
        last_decision_muted: Optional[bool] = None

        for d in dates:
            agg_mute, agg_unmute = _get_aggregates(daily, full_name, branch, d, config)
            d_idx = to_days(d) - base

            if prev_muted is None:
                prev_muted = bool(agg_unmute.get('is_muted', 0))
                last_decision_muted = prev_muted

            # Only re-evaluate on update days
            if d_idx % update_interval_days == 0:
                # Stale unmute: if no runs for STALE_UNMUTE_DAYS, unmute (test may be removed)
                if last_decision_muted and days_since_last_run(daily, full_name, branch, d) >= STALE_UNMUTE_DAYS:
                    last_decision_muted = False
                elif would_unmute(config, agg_unmute, last_decision_muted):
                    last_decision_muted = False
                elif would_mute(config, agg_mute, last_decision_muted):
                    last_decision_muted = True

            prev_muted = last_decision_muted
            history.append(SimulatedState(muted=last_decision_muted, date=d))

        result[key] = history

    if chunk_wildcard_rules:
        _apply_chunk_wildcard_rules(result, list(daily.keys()), len(dates))
    return dict(result)


def compute_metrics(
    simulation: Dict[Tuple[str, str], List[SimulatedState]],
    daily: Dict[Tuple[str, str], List[Dict]],
) -> Metrics:
    """
    Measures:
    - volatility: mute<->unmute transitions per test
    - avg_time_to_mute: avg days from first fail (while not muted) to mute decision
    - avg_time_to_unmute: avg days from last fail (while muted) to unmute decision
    """
    transitions = 0
    n_tests = len(simulation)
    reaction_to_mute: List[int] = []
    reaction_to_unmute: List[int] = []

    for key, history in simulation.items():
        days_data = daily.get(key, [])
        days_by_date = {to_days(r.get('date_window')): r for r in days_data}

        prev_muted = None
        failing_since: Optional[int] = None   # day index when unmuted test started failing
        last_fail_while_unmuted: Optional[int] = None  # last day with fail while unmuted
        clean_since: Optional[int] = None      # day index when muted test stopped failing
        consecutive_clean = 0                  # consecutive clean days while unmuted

        for i, s in enumerate(history):
            d = to_days(s.date)
            day_data = days_by_date.get(d, {})
            has_fails = (day_data.get('fail_count') or 0) > 0

            if prev_muted is not None and prev_muted != s.muted:
                transitions += 1
                if s.muted and failing_since is not None:
                    reaction_to_mute.append(i - failing_since)
                    failing_since = None
                    last_fail_while_unmuted = None
                    consecutive_clean = 0
                elif not s.muted and clean_since is not None:
                    reaction_to_unmute.append(i - clean_since)
                    clean_since = None

            # Track when an unmuted test starts/keeps failing
            if not s.muted:
                if has_fails:
                    if failing_since is None:
                        failing_since = i
                    last_fail_while_unmuted = i
                    consecutive_clean = 0
                else:
                    consecutive_clean += 1
                    # Only forget about the failure streak if test has been clean
                    # for enough days that no reasonable mute window would catch it
                    if consecutive_clean > 10:
                        failing_since = None
                        last_fail_while_unmuted = None

            # Track when a muted test stops failing
            if s.muted:
                if has_fails:
                    clean_since = None
                elif clean_since is None:
                    clean_since = i
                # Reset unmuted tracking
                failing_since = None
                last_fail_while_unmuted = None
                consecutive_clean = 0

            prev_muted = s.muted

    volatility = transitions / n_tests if n_tests else 0
    avg_mute = sum(reaction_to_mute) / len(reaction_to_mute) if reaction_to_mute else None
    avg_unmute = sum(reaction_to_unmute) / len(reaction_to_unmute) if reaction_to_unmute else None

    return Metrics(
        volatility=volatility,
        avg_time_to_mute=avg_mute,
        avg_time_to_unmute=avg_unmute,
        n_mute_transitions=len(reaction_to_mute),
        n_unmute_transitions=len(reaction_to_unmute),
    )


def run_optimization(
    branch: str = 'main',
    build_type: str = 'relwithdebinfo',
    days: int = 60,
    sample_ratio: float = 0.05,
    no_cache: bool = False,
    job_filter: str = 'wf_only',
    update_intervals: Optional[List[int]] = None,
) -> List[Tuple[Dict[str, Any], ThresholdConfig, Metrics]]:
    """
    Returns list of (context, config, metrics) where context describes job_filter, update_interval.
    job_filter: 'wf_only' or 'wf_and_pr' (WF + merged PR, 1:1).
    """
    if update_intervals is None:
        update_intervals = [1, 2, 3, 5, 7]

    logger.info(f"Fetching data (branch={branch}, days={days}, cache={'no' if no_cache else 'yes'})...")
    rows = fetch_from_test_runs_column(branch, build_type, days, no_cache=no_cache)
    if not rows:
        logger.warning("No data. Check YDB credentials.")
        return []

    pr_merged = fetch_pr_last_run_merged_failures(branch, build_type, days, no_cache=no_cache) if job_filter == 'wf_and_pr' else None
    daily = build_daily_aggregates(rows, job_filter, pr_merged_rows=pr_merged)
    today = datetime.date.today()
    date_list = [today - datetime.timedelta(days=i) for i in range(days - 1, -1, -1)]

    if sample_ratio < 1.0:
        import random
        keys = list(daily.keys())
        n_sample = max(1000, int(len(keys) * sample_ratio))
        sampled = set(random.sample(keys, min(n_sample, len(keys))))
        daily = {k: daily[k] for k in sampled}
        logger.info(f"Sampled {len(daily)} tests ({sample_ratio*100:.0f}%)")

    # Grid: thresholds + update_interval
    configs = []
    for mute_days in [4, 5, 7]:
        for mute_fail in [2, 3]:
            for unmute_days in [7, 10, 14]:
                for unmute_runs in [5, 10, 20]:
                    for ui in update_intervals:
                        configs.append((
                            ThresholdConfig(mute_days, mute_fail, unmute_days, unmute_runs),
                            ui,
                        ))

    context = {'job_filter': job_filter}
    results: List[Tuple[Dict, ThresholdConfig, Metrics]] = []
    for idx, (cfg, ui) in enumerate(configs, 1):
        if idx % 50 == 0 or idx == len(configs):
            logger.info(f"  [{idx}/{len(configs)}] ...")
        sim = simulate(daily, date_list, cfg, update_interval_days=ui)
        m = compute_metrics(sim, daily)
        results.append(({**context, 'update_interval': ui}, cfg, m))

    def sort_key(item):
        _, _, metrics = item
        return metrics.score(w_volatility=2.0, w_mute=0.5, w_unmute=0.5)

    results.sort(key=sort_key)
    return results


def main():
    ap = argparse.ArgumentParser(description="Mute optimizer v3 (cache, test_runs_column, job filter, update freq)")
    ap.add_argument('--branch', default='main')
    ap.add_argument('--build-type', default='relwithdebinfo')
    ap.add_argument('--days', type=int, default=60)
    ap.add_argument('--sample', type=float, default=0.05)
    ap.add_argument('--no-cache', action='store_true', help='Force re-fetch from YDB')
    ap.add_argument('--job-filter', choices=['wf_only', 'wf_and_pr'], default='wf_only',
        help='wf_only=Nightly/Regression/Postcommit; wf_and_pr=+merged PR (1:1)')
    ap.add_argument('--update-intervals', default='1,2,3,5,7',
        help='Comma-separated days: 1=daily, 7=weekly')
    ap.add_argument('--top', type=int, default=15)
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()

    update_intervals = [int(x.strip()) for x in args.update_intervals.split(',') if x.strip()]

    results = run_optimization(
        branch=args.branch,
        build_type=args.build_type,
        days=args.days,
        sample_ratio=args.sample,
        no_cache=args.no_cache,
        job_filter=args.job_filter,
        update_intervals=update_intervals,
    )

    if not results:
        sys.exit(1)

    if args.json:
        out = []
        for ctx, cfg, m in results[:args.top]:
            out.append({
                'context': ctx,
                'config': {
                    'mute_days': cfg.mute_days,
                    'mute_fail_threshold': cfg.mute_fail_threshold,
                    'unmute_days': cfg.unmute_days,
                    'unmute_min_runs': cfg.unmute_min_runs,
                },
                'metrics': {
                    'volatility': round(m.volatility, 4),
                    'avg_time_to_mute': m.avg_time_to_mute,
                    'avg_time_to_unmute': m.avg_time_to_unmute,
                },
                'score': round(m.score(w_volatility=2.0, w_mute=0.5, w_unmute=0.5), 2),
            })
        print(json.dumps(out, indent=2))
    else:
        print("\n=== MUTE OPTIMIZER v3 ===\n")
        print(f"job_filter={args.job_filter} update_intervals={update_intervals}\n")
        print(f"{'Context':<35} {'Config':<45} {'Vol':<8} {'Mute':<8} {'Unmute':<8} {'Score':<8}")
        print("-" * 120)
        for ctx, cfg, m in results[:args.top]:
            mu = f"{m.avg_time_to_mute:.1f}" if m.avg_time_to_mute is not None else "—"
            um = f"{m.avg_time_to_unmute:.1f}" if m.avg_time_to_unmute is not None else "—"
            sc = m.score(w_volatility=2.0, w_mute=0.5, w_unmute=0.5)
            ctx_str = f"ui={ctx['update_interval']}"
            print(f"{ctx_str:<35} {str(cfg):<45} {m.volatility:<8.4f} {mu:<8} {um:<8} {sc:<8.2f}")
        print("\nUse --job-filter wf_and_pr to include merged PR data. Use --no-cache to refetch.")


if __name__ == "__main__":
    main()
