#!/usr/bin/env python3
"""
ML-оптимизатор mute порогов v3: кэш, test_runs_column, PR только last run merged, подбор коэффициента, визуализация.

- Данные: локальный кэш, test_runs_column; PR-check = только последний запуск по merged PR (как pr_with_test_failures.sql).
- Optuna подбирает: пороги, update_interval. use_pr задаётся OPTIMIZATION_MODE ('wf_and_pr' / 'wf_only').
- В конце: HTML с сравнением baseline (wf_only) vs best и объяснением, почему лучший вариант лучше.
Требует: pip install optuna
"""
import argparse
import datetime
import json
import logging
import os
import random
import sys
import webbrowser
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

try:
    import optuna
    from optuna.samplers import TPESampler
    HAS_OPTUNA = True
except ImportError:
    HAS_OPTUNA = False

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

sys.path.insert(0, os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'analytics'))

from mute_optimizer_v3 import (
    ThresholdConfig,
    Metrics,
    PrecomputedAggregates,
    fetch_from_test_runs_column,
    fetch_pr_last_run_merged_failures,
    precompute_base_aggregates,
    build_daily_fast,
    build_daily_aggregates,
    simulate,
    compute_metrics,
    to_days,
    get_mute_window_agg,
    days_since_last_run,
    STALE_UNMUTE_DAYS,
)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Optimization mode: 'wf_and_pr' = WF+merged PR (default), 'wf_only' = WF only
OPTIMIZATION_MODE = 'wf_and_pr'

# Normalization bounds for score (priority: mute > unmute > volatility)
TIME_NORM_DAYS = 14   # max acceptable days for time metrics
VOL_NORM = 0.10       # max acceptable volatility
TARGET_MUTE_DAYS = 1  # target for time_to_mute; score penalizes deviation from this
MUTE_DEVIATION_SCALE = 7  # max acceptable deviation (days) for mute term norm

# Descriptions for Parameter Importance chart (all params that can appear in Optuna)
PARAM_DESCRIPTIONS = {
    'window_type': 'Тип окна: days (по дням), runs (по запускам), hybrid',
    'use_pr': 'Использовать PR-check (WF+PR) или только WF',
    'mute_fail_threshold': 'Мин. кол-во failures в окне для mute',
    'mute_days': 'Дней в окне для mute (days/hybrid)',
    'unmute_days': 'Дней в окне для unmute (days/hybrid)',
    'unmute_min_runs': 'Мин. runs в окне для unmute (days)',
    'mute_last_runs': 'Последние N runs для mute (runs/hybrid)',
    'unmute_last_runs': 'Последние N runs для unmute (runs/hybrid)',
    'chunk_wildcard_rules': 'Правила chunk [*/*]: 1 падающий chunk → mute всего паттерна; unmute только когда все проходят',
}


def compute_normalized_score(
    m: Metrics,
    w_mute: float,
    w_unmute: float,
    w_vol: float,
) -> float:
    """
    Normalized score: priority 1=mute, 2=unmute, 3=volatility.
    Mute: penalizes deviation from TARGET_MUTE_DAYS (1 day).
    Unmute: norm(t) = min(1, t/14).
    Vol: norm(vol) = min(1, vol/0.10).
    """
    ttm = m.avg_time_to_mute or 0
    tm = min(1.0, abs(ttm - TARGET_MUTE_DAYS) / MUTE_DEVIATION_SCALE)
    tu = min(1.0, (m.avg_time_to_unmute or 0) / TIME_NORM_DAYS)
    vol = min(1.0, m.volatility / VOL_NORM)
    return w_mute * tm + w_unmute * tu + w_vol * vol


def prepare_data(
    branch: str,
    build_type: str,
    days: int,
    sample_ratio: float,
    no_cache: bool,
) -> Tuple[Optional[List], Optional[List], Optional[List]]:
    """Возвращает (rows_wf_pr, pr_merged_rows, date_list)."""
    rows = fetch_from_test_runs_column(branch, build_type, days, no_cache=no_cache)
    if not rows:
        return None, None, None
    today = datetime.date.today()
    date_list = [today - datetime.timedelta(days=i) for i in range(days - 1, -1, -1)]
    pr_merged = fetch_pr_last_run_merged_failures(branch, build_type, days, no_cache=no_cache)
    return rows, pr_merged if pr_merged else None, date_list


def run_optuna(
    pre: PrecomputedAggregates,
    pr_merged_rows: Optional[List[Dict]],
    date_list: List[datetime.date],
    sample_ratio: float,
    n_trials: int,
    w_vol: float,
    w_mute: float,
    w_unmute: float,
) -> optuna.Study:
    """Optuna TPE: window_type, thresholds. use_pr fixed by OPTIMIZATION_MODE."""
    has_pr_merged = bool(pr_merged_rows)
    use_pr_fixed = (OPTIMIZATION_MODE == 'wf_and_pr') and has_pr_merged
    logger.info(f"  Optimization mode: {OPTIMIZATION_MODE} (use_pr={use_pr_fixed})")

    def objective(trial):
        use_pr = trial.suggest_categorical('use_pr', [use_pr_fixed])
        if use_pr:
            daily = build_daily_fast(pre, 'wf_and_pr', use_pr_merged=True)
        else:
            daily = build_daily_fast(pre, 'wf_only', use_pr_merged=False)

        if sample_ratio < 1.0:
            # Use WF keys as stable base for sampling (same across all PR configs)
            rng = random.Random(42)
            wf_test_keys = sorted(set((fn, br) for fn, br, _ in pre.wf.keys()))
            n = max(1000, int(len(wf_test_keys) * sample_ratio))
            sampled = set(rng.sample(wf_test_keys, min(n, len(wf_test_keys))))
            daily = {k: daily[k] for k in daily if k in sampled}

        window_type = trial.suggest_categorical('window_type', ['days', 'runs', 'hybrid'])
        chunk_wildcard_rules = trial.suggest_categorical('chunk_wildcard_rules', [True, False])
        mute_fail = trial.suggest_int('mute_fail_threshold', 1, 2)

        if window_type == 'days':
            mute_days = trial.suggest_int('mute_days', 3, 8)
            unmute_days = trial.suggest_int('unmute_days', 5, 18)
            unmute_runs = trial.suggest_int('unmute_min_runs', 3, 22)
            cfg = ThresholdConfig(mute_days, mute_fail, unmute_days, unmute_runs, window_type='days')
        elif window_type == 'runs':
            mute_last_runs = trial.suggest_int('mute_last_runs', 5, 50)
            unmute_last_runs = trial.suggest_int('unmute_last_runs', 5, 50)
            cfg = ThresholdConfig(
                mute_days=0, mute_fail_threshold=mute_fail, unmute_days=0, unmute_min_runs=0,
                window_type='runs', mute_last_runs=mute_last_runs, unmute_last_runs=unmute_last_runs,
            )
        else:  # hybrid: last N runs within last M days
            mute_days = trial.suggest_int('mute_days', 3, 14)
            unmute_days = trial.suggest_int('unmute_days', 5, 21)
            mute_last_runs = trial.suggest_int('mute_last_runs', 5, 50)
            unmute_last_runs = trial.suggest_int('unmute_last_runs', 5, 50)
            cfg = ThresholdConfig(
                mute_days=mute_days, mute_fail_threshold=mute_fail,
                unmute_days=unmute_days, unmute_min_runs=0,
                window_type='hybrid',
                mute_last_runs=mute_last_runs, unmute_last_runs=unmute_last_runs,
            )

        sim = simulate(daily, date_list, cfg, update_interval_days=1, chunk_wildcard_rules=chunk_wildcard_rules)
        m = compute_metrics(sim, daily)
        score = compute_normalized_score(m, w_mute, w_unmute, w_vol)

        # Hard constraint: penalize configs where volatility is worse than production
        if m.volatility > VOL_NORM:
            score += 50.0 * (m.volatility - VOL_NORM)

        return score

    sampler = TPESampler(seed=42, n_startup_trials=15)
    study = optuna.create_study(direction='minimize', sampler=sampler)
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
    return study


def _metrics_to_dict(m: Metrics, w_vol: float, w_mute: float, w_unmute: float, n_tests: int = 0, n_days: int = 0) -> Dict:
    d = {
        'volatility': m.volatility,
        'avg_time_to_mute': m.avg_time_to_mute,
        'avg_time_to_unmute': m.avg_time_to_unmute,
        'n_mute': m.n_mute_transitions,
        'n_unmute': m.n_unmute_transitions,
        'score': compute_normalized_score(m, w_mute, w_unmute, w_vol),
        'n_tests': n_tests,
        'n_days': n_days,
    }
    # Practical numbers
    if n_tests > 0 and n_days > 0:
        d['wrong_state_tests_per_day'] = round(m.volatility * n_tests / n_days, 1) if n_days else 0
        d['mute_events_per_day'] = round(m.n_mute_transitions / n_days, 2) if n_days else 0
        d['unmute_events_per_day'] = round(m.n_unmute_transitions / n_days, 2) if n_days else 0
        d['blocked_test_days'] = round((m.avg_time_to_mute or 0) * m.n_mute_transitions, 1)
        d['stale_mute_days'] = round((m.avg_time_to_unmute or 0) * m.n_unmute_transitions, 1)
    return d


def _full_name_to_muted_ya(full_name: str) -> str:
    """Convert 'suite_folder/test_name' to 'suite_folder test_name' (muted_ya.txt format)."""
    idx = full_name.rfind('/')
    if idx == -1:
        return full_name
    return full_name[:idx] + ' ' + full_name[idx + 1:]


def extract_muted_tests(
    sim: Dict,
    daily: Dict,
    cfg: ThresholdConfig,
    date_list: List[datetime.date],
) -> Dict[str, Dict]:
    """From simulation, extract tests that are muted on the last day, with reasons."""
    muted = {}
    last_date = date_list[-1] if date_list else None
    for key, history in sim.items():
        if not history:
            continue
        last_state = history[-1]
        if not last_state.muted:
            continue
        full_name, branch = key
        # Exclude stale tests: if no runs for STALE_UNMUTE_DAYS, treat as removed
        if last_date and days_since_last_run(daily, full_name, branch, last_date) >= STALE_UNMUTE_DAYS:
            continue
        days_data = daily.get(key, [])
        total_fails = sum(r.get('fail_count', 0) for r in days_data)
        total_runs = sum((r.get('pass_count', 0) + r.get('fail_count', 0) + r.get('mute_count', 0)) for r in days_data)

        # When did this test become muted? (first day in history with muted=True)
        muted_since_date = None
        for s in history:
            if s.muted:
                muted_since_date = s.date
                break

        # Decision-window stats (last N runs/days used for the mute decision)
        window_agg = get_mute_window_agg(daily, full_name, branch, last_date, cfg) if last_date else {}
        window_fails = window_agg.get('fail_count', 0)
        window_runs = window_agg.get('total_runs', 0)

        muted[full_name] = {
            'branch': branch,
            'total_fails': total_fails,
            'total_runs': total_runs,
            'fail_rate': round(total_fails / total_runs, 3) if total_runs > 0 else 0,
            'config': str(cfg),
            'muted_since_date': muted_since_date,
            'window_fails': window_fails,
            'window_runs': window_runs,
        }
    return muted


def write_muted_list(muted: Dict[str, Dict], out_path: Path) -> None:
    """Write muted tests in muted_ya.txt format."""
    lines = sorted(_full_name_to_muted_ya(fn) for fn in muted)
    out_path.write_text('\n'.join(lines) + '\n' if lines else '', encoding='utf-8')
    logger.info(f"Wrote {len(lines)} tests to {out_path}")


def compute_mute_diff(
    prod_muted: Dict[str, Dict],
    best_muted: Dict[str, Dict],
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """Returns (added, removed, unchanged).
    added = opt mutes, prod doesn't (new mutes when switching to opt)
    removed = prod mutes, opt doesn't (would be unmuted when switching to opt)
    unchanged = muted in both"""
    prod_set = set(prod_muted.keys())
    best_set = set(best_muted.keys())
    added_set = best_set - prod_set
    removed_set = prod_set - best_set
    unchanged_set = prod_set & best_set

    def _row(fn: str, info: Dict) -> Dict:
        m = info.get('muted_since_date')
        muted_since = m.isoformat() if m else '—'
        wf, wr = info.get('window_fails', 0), info.get('window_runs', 0)
        window_str = f"{wf}/{wr}" if wr else "—"
        return {
            'test': fn,
            'muted_ya': _full_name_to_muted_ya(fn),
            'muted_since_date': muted_since,
            'window_fails': wf,
            'window_runs': wr,
            'window_str': window_str,
            'fail_rate': info.get('fail_rate', 0),
            'total_fails': info.get('total_fails', 0),
            'total_runs': info.get('total_runs', 0),
        }

    added = [_row(fn, best_muted[fn]) for fn in sorted(added_set)]
    removed = [_row(fn, prod_muted[fn]) for fn in sorted(removed_set)]
    unchanged = [_row(fn, prod_muted[fn]) for fn in sorted(unchanged_set)]
    return added, removed, unchanged


# Current production thresholds from create_new_muted_ya.py (WF-only, no PR)
# Mute: fail>=3 (when runs>10) or fail>=2 (when runs<=10) over MUTE_DAYS=4
# Unmute: total_runs>=4 and total_fails==0 over UNMUTE_DAYS=7
PROD_CONFIG = ThresholdConfig(
    mute_days=4,
    mute_fail_threshold=3,
    unmute_days=7,
    unmute_min_runs=4,
    window_type='days',
    mute_fail_threshold_low_runs=2,
    low_runs_bound=10,
)


def build_comparison_data(
    pre: PrecomputedAggregates,
    pr_merged_rows: Optional[List[Dict]],
    date_list: List[datetime.date],
    best_params: Dict,
    sample_ratio: float,
    w_vol: float,
    w_mute: float,
    w_unmute: float,
) -> Tuple[Dict, Dict, Dict, Dict]:
    """Returns (best_metrics, prod_metrics, baseline_metrics, best_params_dict).
    prod = current production config (create_new_muted_ya.py), WF-only data (no PR).
    baseline = same thresholds as best, but wf_only.
    """
    use_pr = best_params.get('use_pr', False)
    daily_best = build_daily_fast(pre, 'wf_and_pr' if use_pr else 'wf_only',
                                   use_pr_merged=use_pr)
    daily_wf = build_daily_fast(pre, 'wf_only', use_pr_merged=False)

    if sample_ratio < 1.0:
        rng = random.Random(42)
        wf_test_keys = sorted(set((fn, br) for fn, br, _ in pre.wf.keys()))
        n = max(1000, int(len(wf_test_keys) * sample_ratio))
        sampled = set(rng.sample(wf_test_keys, min(n, len(wf_test_keys))))
        daily_best = {k: daily_best[k] for k in daily_best if k in sampled}
        daily_wf = {k: daily_wf[k] for k in daily_wf if k in sampled}

    # Best config
    wt = best_params.get('window_type', 'days')
    if wt == 'runs':
        cfg = ThresholdConfig(
            mute_days=0, mute_fail_threshold=best_params['mute_fail_threshold'],
            unmute_days=0, unmute_min_runs=0,
            window_type='runs',
            mute_last_runs=best_params.get('mute_last_runs', 20),
            unmute_last_runs=best_params.get('unmute_last_runs', 20),
        )
    elif wt == 'hybrid':
        cfg = ThresholdConfig(
            mute_days=best_params.get('mute_days', 7),
            mute_fail_threshold=best_params['mute_fail_threshold'],
            unmute_days=best_params.get('unmute_days', 14),
            unmute_min_runs=0,
            window_type='hybrid',
            mute_last_runs=best_params.get('mute_last_runs', 20),
            unmute_last_runs=best_params.get('unmute_last_runs', 20),
        )
    else:
        cfg = ThresholdConfig(
            best_params.get('mute_days', 7),
            best_params['mute_fail_threshold'],
            best_params.get('unmute_days', 10),
            best_params.get('unmute_min_runs', 10),
            window_type='days',
        )
    chunk_rules = best_params.get('chunk_wildcard_rules', False)
    sim_best = simulate(daily_best, date_list, cfg, update_interval_days=1, chunk_wildcard_rules=chunk_rules)
    m_best = compute_metrics(sim_best, daily_best)

    # Production config (create_new_muted_ya.py): WF-only, MUTE_DAYS=4, fail>=3|2, UNMUTE_DAYS=7, UNMUTE_MIN_RUNS=4; uses chunk wildcard rules
    sim_prod = simulate(daily_wf, date_list, PROD_CONFIG, update_interval_days=1, chunk_wildcard_rules=True)
    m_prod = compute_metrics(sim_prod, daily_wf)

    # Baseline = best thresholds but wf_only (for isolating PR effect)
    cfg_base = ThresholdConfig(
        mute_days=cfg.mute_days if wt == 'days' else 7,
        mute_fail_threshold=cfg.mute_fail_threshold,
        unmute_days=cfg.unmute_days if wt == 'days' else 10,
        unmute_min_runs=cfg.unmute_min_runs if wt == 'days' else 10,
        window_type='days',
    )
    sim_base = simulate(daily_wf, date_list, cfg_base, update_interval_days=1, chunk_wildcard_rules=chunk_rules)
    m_base = compute_metrics(sim_base, daily_wf)

    n_tests = len(daily_wf)
    n_days = len(date_list)
    return (
        _metrics_to_dict(m_best, w_vol, w_mute, w_unmute, n_tests, n_days),
        _metrics_to_dict(m_prod, w_vol, w_mute, w_unmute, n_tests, n_days),
        _metrics_to_dict(m_base, w_vol, w_mute, w_unmute, n_tests, n_days),
        dict(best_params),
    )


def generate_html(
    best_metrics: Dict,
    prod_metrics: Dict,
    baseline_metrics: Dict,
    best_params: Dict,
    branch: str,
    days: int,
    out_path: Path,
    study: Optional[optuna.Study] = None,
) -> None:
    """Генерирует HTML: сравнение production vs best, история оптимизации, importance параметров."""
    use_pr = best_params.get('use_pr', False)
    vol_b = best_metrics['volatility']
    vol_p = prod_metrics['volatility']
    tm_b = best_metrics['avg_time_to_mute'] or 0
    tm_p = prod_metrics['avg_time_to_mute'] or 0
    tu_b = best_metrics['avg_time_to_unmute'] or 0
    tu_p = prod_metrics['avg_time_to_unmute'] or 0
    sc_b = best_metrics['score']
    sc_p = prod_metrics['score']

    wt = best_params.get('window_type', 'days')
    reason_vol = f"Volatility {vol_p:.4f} → {vol_b:.4f} (" + ("↓" if vol_b < vol_p else "↑") + f" {abs(vol_b - vol_p):.4f})"
    reason_mute = f"Reaction to mute {tm_p:.1f} → {tm_b:.1f} days"
    reason_unmute = f"Reaction to unmute {tu_p:.1f} → {tu_b:.1f} days"
    reason_score = f"Score {sc_p:.4f} → {sc_b:.4f} (" + ("лучше" if sc_b < sc_p else "хуже") + ")"

    summary_parts = []
    if wt == 'runs':
        summary_parts.append(
            f"Лучший конфиг использует окно по количеству запусков (runs-based): "
            f"mute по последним {best_params.get('mute_last_runs')} запускам, "
            f"unmute по последним {best_params.get('unmute_last_runs')} запускам."
        )
    else:
        summary_parts.append(
            f"Лучший конфиг использует окно по дням (days-based): "
            f"mute_days={best_params.get('mute_days')}, unmute_days={best_params.get('unmute_days')}."
        )
    if use_pr:
        summary_parts.append("Используется PR-check (merged PR, pass+fail 1:1 с WF).")
    else:
        summary_parts.append("PR-check не используется (только WF).")
    if best_params.get('chunk_wildcard_rules', False):
        summary_parts.append("Chunk wildcard rules: да (как prod).")
    else:
        summary_parts.append("Chunk wildcard rules: нет (поштучно).")
    if sc_b < sc_p:
        delta_pct = (1 - sc_b / sc_p) * 100 if sc_p > 0 else 0
        summary_parts.append(f"Score лучше на {delta_pct:.0f}% vs production.")
    else:
        summary_parts.append("Production config оказался не хуже — смотри графики.")
    summary = " ".join(summary_parts)

    pr_info = f"use_pr={use_pr}, chunk_wildcard_rules={best_params.get('chunk_wildcard_rules', False)}"
    if wt == 'runs':
        params_str = (
            f"window_type=runs, {pr_info}, "
            f"mute_last_runs={best_params.get('mute_last_runs')}, mute_fail>={best_params.get('mute_fail_threshold')}, "
            f"unmute_last_runs={best_params.get('unmute_last_runs')}"
        )
    else:
        params_str = (
            f"window_type={wt}, {pr_info}, "
            f"mute_days={best_params.get('mute_days')}, mute_fail>={best_params.get('mute_fail_threshold')}, "
            f"unmute_days={best_params.get('unmute_days')}, unmute_runs>={best_params.get('unmute_min_runs')}"
        )

    # Build optimization history data from study
    history_js = "[]"
    best_so_far_js = "[]"
    days_scores_js = "[]"
    runs_scores_js = "[]"
    param_importance_js = "{}"
    if study:
        completed = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
        trial_nums = [t.number for t in completed]
        trial_vals = [round(t.value, 4) for t in completed]
        best_vals = []
        bv = float('inf')
        for v in trial_vals:
            bv = min(bv, v)
            best_vals.append(round(bv, 4))
        history_js = json.dumps(trial_vals)
        best_so_far_js = json.dumps(best_vals)

        # Scores by window_type
        days_scores = [(t.number, round(t.value, 4)) for t in completed if t.params.get('window_type') == 'days']
        runs_scores = [(t.number, round(t.value, 4)) for t in completed if t.params.get('window_type') == 'runs']
        days_scores_js = json.dumps(days_scores)
        runs_scores_js = json.dumps(runs_scores)

        # Parameter importance: merge Optuna result with full param list + descriptions
        try:
            importance = optuna.importance.get_param_importances(study)
        except Exception:
            importance = {}
        all_params = set(PARAM_DESCRIPTIONS.keys())
        param_importance_full = [
            {'param': p, 'imp': round(importance.get(p, 0), 4), 'desc': PARAM_DESCRIPTIONS.get(p, '')}
            for p in sorted(all_params, key=lambda x: -importance.get(x, 0))
        ]
        param_importance_js = json.dumps(param_importance_full, ensure_ascii=False)
    else:
        param_importance_js = json.dumps(
            [{'param': p, 'imp': 0, 'desc': PARAM_DESCRIPTIONS.get(p, '')} for p in sorted(PARAM_DESCRIPTIONS.keys())],
            ensure_ascii=False
        )

    html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Mute Optimizer v3 ML — Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{ font-family: system-ui, sans-serif; margin: 24px; max-width: 1100px; }}
        h1 {{ color: #222; }}
        h2 {{ color: #444; margin-top: 32px; border-bottom: 1px solid #ddd; padding-bottom: 8px; }}
        .summary {{ background: #f0f7ff; padding: 16px; border-radius: 8px; margin: 16px 0; }}
        .charts-row {{ display: flex; flex-wrap: wrap; gap: 24px; }}
        .chart-container {{ flex: 1; min-width: 400px; max-width: 550px; }}
        canvas {{ max-width: 100%; }}
        table {{ border-collapse: collapse; margin-top: 16px; width: 100%; max-width: 700px; }}
        th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
        th {{ background: #f5f5f5; }}
        .better {{ color: #0a0; font-weight: bold; }}
        .worse {{ color: #c00; }}
    </style>
</head>
<body>
    <h1>Mute Optimizer v3 ML</h1>
    <p>Branch: <strong>{branch}</strong> | Days: <strong>{days}</strong></p>
    <div class="summary" style="margin-top: 12px;">
        <p><strong>Prod:</strong> WF-only, MUTE_DAYS=4, fail&gt;=3|2, UNMUTE_DAYS=7, UNMUTE_MIN_RUNS=4</p>
        <p><strong>Optimized:</strong> {params_str}</p>
    </div>

    <h2>1. Best Config</h2>
    <div class="summary">
        <p>{summary}</p>
        <ul>
            <li>{reason_vol}</li>
            <li>{reason_mute}</li>
            <li>{reason_unmute}</li>
            <li>{reason_score}</li>
        </ul>
        <p><strong>Best params:</strong> {params_str}</p>
    </div>

    <h2>2. Production vs Optimized</h2>
    <p style="color:#666">Production = create_new_muted_ya.py (WF-only, MUTE_DAYS=4, fail&gt;=3|2, UNMUTE_DAYS=7, UNMUTE_MIN_RUNS=4)</p>
    <div class="charts-row">
        <div class="chart-container">
            <canvas id="barChart" height="280"></canvas>
        </div>
    </div>
    <table>
        <thead><tr><th>Metric</th><th>Production (current)</th><th>Best (optimized)</th><th>Δ (best−prod)</th></tr></thead>
        <tbody>
            <tr><td>Volatility</td><td>{vol_p:.4f}</td><td>{vol_b:.4f}</td><td class="{'better' if vol_b < vol_p else 'worse'}">{vol_b - vol_p:+.4f}</td></tr>
            <tr><td>Reaction to mute (d)</td><td>{tm_p:.1f}</td><td>{tm_b:.1f}</td><td class="{'better' if tm_b < tm_p else 'worse'}">{tm_b - tm_p:+.1f}</td></tr>
            <tr><td>Reaction to unmute (d)</td><td>{tu_p:.1f}</td><td>{tu_b:.1f}</td><td class="{'better' if tu_b < tu_p else 'worse'}">{tu_b - tu_p:+.1f}</td></tr>
            <tr><td>Score</td><td>{sc_p:.4f}</td><td>{sc_b:.4f}</td><td class="{'better' if sc_b < sc_p else 'worse'}">{sc_b - sc_p:+.4f}</td></tr>
            <tr><td>Mute transitions</td><td>{prod_metrics.get('n_mute', 0)}</td><td>{best_metrics.get('n_mute', 0)}</td><td>{best_metrics.get('n_mute', 0) - prod_metrics.get('n_mute', 0):+d}</td></tr>
            <tr><td>Unmute transitions</td><td>{prod_metrics.get('n_unmute', 0)}</td><td>{best_metrics.get('n_unmute', 0)}</td><td>{best_metrics.get('n_unmute', 0) - prod_metrics.get('n_unmute', 0):+d}</td></tr>
        </tbody>
    </table>

    <h2>3. Optimization History</h2>
    <div class="charts-row">
        <div class="chart-container">
            <canvas id="historyChart" height="280"></canvas>
        </div>
        <div class="chart-container">
            <canvas id="modelCompareChart" height="280"></canvas>
        </div>
    </div>

    <h2>4. Parameter Importance</h2>
    <p style="color:#666; margin-bottom: 8px;">use_pr может быть 0 — фиксирован через OPTIMIZATION_MODE и не варьируется в trials.</p>
    <div class="charts-row">
        <div class="chart-container">
            <canvas id="importanceChart" height="320"></canvas>
        </div>
    </div>
    <table style="margin-top: 16px; max-width: 700px;">
        <thead><tr><th>Параметр</th><th>За что отвечает</th></tr></thead>
        <tbody id="paramDescTable"></tbody>
    </table>

    <script>
        // --- Production vs Best bar chart ---
        new Chart(document.getElementById('barChart').getContext('2d'), {{
            type: 'bar',
            data: {{
                labels: ['Volatility', 'Reaction to mute (d)', 'Reaction to unmute (d)', 'Score'],
                datasets: [
                    {{ label: 'Production (current)', data: [{vol_p}, {tm_p}, {tu_p}, {sc_p}], backgroundColor: 'rgba(180,80,60,0.7)' }},
                    {{ label: 'Best (optimized)', data: [{vol_b}, {tm_b}, {tu_b}, {sc_b}], backgroundColor: 'rgba(50,120,200,0.7)' }}
                ]
            }},
            options: {{ scales: {{ y: {{ beginAtZero: true }} }}, plugins: {{ legend: {{ position: 'top' }} }} }}
        }});

        // --- Optimization history ---
        const historyData = {history_js};
        const bestSoFar = {best_so_far_js};
        new Chart(document.getElementById('historyChart').getContext('2d'), {{
            type: 'line',
            data: {{
                labels: historyData.map((_, i) => i),
                datasets: [
                    {{ label: 'Trial score', data: historyData, borderColor: 'rgba(100,100,100,0.5)', pointRadius: 3, pointBackgroundColor: historyData.map((v, i) => v === bestSoFar[i] ? 'rgba(50,120,200,1)' : 'rgba(100,100,100,0.5)'), fill: false, tension: 0 }},
                    {{ label: 'Best so far', data: bestSoFar, borderColor: 'rgba(50,120,200,1)', borderWidth: 2, pointRadius: 0, fill: false, tension: 0.1 }}
                ]
            }},
            options: {{
                scales: {{ x: {{ title: {{ display: true, text: 'Trial #' }} }}, y: {{ title: {{ display: true, text: 'Score (lower=better)' }}, beginAtZero: true }} }},
                plugins: {{ title: {{ display: true, text: 'Optimization History' }} }}
            }}
        }});

        // --- Model A (days) vs Model B (runs) scatter ---
        const daysScores = {days_scores_js};
        const runsScores = {runs_scores_js};
        new Chart(document.getElementById('modelCompareChart').getContext('2d'), {{
            type: 'scatter',
            data: {{
                datasets: [
                    {{ label: 'Model A (days)', data: daysScores.map(d => ({{x: d[0], y: d[1]}})), backgroundColor: 'rgba(50,120,200,0.7)', pointRadius: 5 }},
                    {{ label: 'Model B (runs)', data: runsScores.map(d => ({{x: d[0], y: d[1]}})), backgroundColor: 'rgba(220,80,50,0.7)', pointRadius: 5 }}
                ]
            }},
            options: {{
                scales: {{ x: {{ title: {{ display: true, text: 'Trial #' }} }}, y: {{ title: {{ display: true, text: 'Score' }}, beginAtZero: true }} }},
                plugins: {{ title: {{ display: true, text: 'Model A (days) vs Model B (runs)' }} }}
            }}
        }});

        // --- Parameter importance ---
        const paramImp = {param_importance_js};
        const impLabels = paramImp.map(p => p.param);
        const impValues = paramImp.map(p => p.imp);
        new Chart(document.getElementById('importanceChart').getContext('2d'), {{
            type: 'bar',
            data: {{
                labels: impLabels,
                datasets: [{{ label: 'Importance', data: impValues, backgroundColor: impLabels.map((_, i) => `hsl(${{Math.max(0, 40 - i * 15)}}, 65%, 55%)`) }}]
            }},
            options: {{
                indexAxis: 'y',
                scales: {{ x: {{ beginAtZero: true, max: 1.0 }} }},
                plugins: {{ title: {{ display: true, text: 'Parameter Importance (fANOVA)' }}, legend: {{ display: false }} }}
            }}
        }});
        document.getElementById('paramDescTable').innerHTML = paramImp.map(p =>
            `<tr><td><code>${{p.param}}</code></td><td>${{p.desc || '—'}}</td></tr>`
        ).join('');
    </script>
</body>
</html>'''
    out_path.write_text(html, encoding='utf-8')
    logger.info(f"Wrote {out_path}")


def bootstrap_significance(
    pre: PrecomputedAggregates,
    pr_merged_rows: Optional[List[Dict]],
    date_list: List[datetime.date],
    best_params: Dict,
    sample_ratio: float,
    n_boot: int,
    w_vol: float,
    w_mute: float,
    w_unmute: float,
) -> Dict:
    """Bootstrap resampling: best vs production. Returns CI and significance."""
    import random
    random.seed(42)

    use_pr = best_params.get('use_pr', False)
    daily_best = build_daily_fast(pre, 'wf_and_pr' if use_pr else 'wf_only',
                                   use_pr_merged=use_pr)
    daily_wf = build_daily_fast(pre, 'wf_only', use_pr_merged=False)

    wf_test_keys = sorted(set((fn, br) for fn, br, _ in pre.wf.keys()))
    if sample_ratio < 1.0:
        n = max(1000, int(len(wf_test_keys) * sample_ratio))
        all_keys = random.sample(wf_test_keys, min(n, len(wf_test_keys)))
    else:
        all_keys = wf_test_keys

    wt = best_params.get('window_type', 'days')
    if wt == 'runs':
        cfg_best = ThresholdConfig(
            mute_days=0, mute_fail_threshold=best_params['mute_fail_threshold'],
            unmute_days=0, unmute_min_runs=0,
            window_type='runs',
            mute_last_runs=best_params.get('mute_last_runs', 20),
            unmute_last_runs=best_params.get('unmute_last_runs', 20),
        )
    elif wt == 'hybrid':
        cfg_best = ThresholdConfig(
            mute_days=best_params.get('mute_days', 7),
            mute_fail_threshold=best_params['mute_fail_threshold'],
            unmute_days=best_params.get('unmute_days', 14),
            unmute_min_runs=0,
            window_type='hybrid',
            mute_last_runs=best_params.get('mute_last_runs', 20),
            unmute_last_runs=best_params.get('unmute_last_runs', 20),
        )
    else:
        cfg_best = ThresholdConfig(
            best_params.get('mute_days', 7),
            best_params['mute_fail_threshold'],
            best_params.get('unmute_days', 10),
            best_params.get('unmute_min_runs', 10),
            window_type='days',
        )

    diffs = []
    iters = range(n_boot)
    if HAS_TQDM:
        iters = tqdm(iters, desc="Bootstrap", unit="iter", leave=False)
    for i in iters:
        if not HAS_TQDM and (i + 1) % 10 == 0:
            logger.info(f"    Bootstrap {i + 1}/{n_boot}...")
        sample_keys = random.choices(all_keys, k=len(all_keys))
        d_best = {k: daily_best[k] for k in sample_keys if k in daily_best}
        d_wf = {k: daily_wf[k] for k in sample_keys if k in daily_wf}

        chunk_rules = best_params.get('chunk_wildcard_rules', False)
        sim_b = simulate(d_best, date_list, cfg_best, update_interval_days=1, chunk_wildcard_rules=chunk_rules)
        m_b = compute_metrics(sim_b, d_best)
        sim_p = simulate(d_wf, date_list, PROD_CONFIG, update_interval_days=1, chunk_wildcard_rules=True)
        m_p = compute_metrics(sim_p, d_wf)

        score_b = compute_normalized_score(m_b, w_mute, w_unmute, w_vol)
        score_p = compute_normalized_score(m_p, w_mute, w_unmute, w_vol)
        diffs.append(score_b - score_p)

    diffs.sort()
    ci_lo = diffs[max(0, int(n_boot * 0.025))]
    ci_hi = diffs[min(len(diffs) - 1, int(n_boot * 0.975))]
    mean_diff = sum(diffs) / len(diffs)
    significant = ci_hi < 0  # entire CI below 0 means best is significantly better

    return {
        'mean_diff': round(mean_diff, 6),
        'ci_95': (round(ci_lo, 6), round(ci_hi, 6)),
        'significant': significant,
        'n_boot': n_boot,
    }


def run_single_branch(
    branch: str,
    build_type: str,
    days: int,
    sample_ratio: float,
    no_cache: bool,
    n_trials: int,
    n_bootstrap: int,
    w_vol: float,
    w_mute: float,
    w_unmute: float,
    debug: bool = False,
    bootstrap_only: bool = False,
    report_only: bool = False,
    best_params_override: Optional[Dict] = None,
) -> Optional[Dict]:
    """Run full pipeline for one branch. Returns result dict or None."""
    logger.info(f"Loading data for branch={branch}...")
    rows, pr_merged_rows, date_list = prepare_data(branch, build_type, days, sample_ratio, no_cache)
    if not rows or not date_list:
        logger.warning(f"No data for branch={branch}, skipping.")
        return None
    logger.info(f"  [{branch}] {len(rows)} rows, {len(date_list)} days, PR merged: {len(pr_merged_rows or [])}")

    pre = precompute_base_aggregates(rows, pr_merged_rows)
    logger.info(f"  [{branch}] Pre-computed: {len(pre.wf)} WF, {len(pre.pr)} PR, {len(pre.pr_merged)} PR-merged keys")

    if debug:
        daily = build_daily_fast(pre, 'wf_only', use_pr_merged=False)
        if sample_ratio < 1.0:
            rng = random.Random(42)
            wf_test_keys = sorted(set((fn, br) for fn, br, _ in pre.wf.keys()))
            n = max(1000, int(len(wf_test_keys) * sample_ratio))
            sampled = set(rng.sample(wf_test_keys, min(n, len(wf_test_keys))))
            daily = {k: daily[k] for k in daily if k in sampled}
        cfg = ThresholdConfig(7, 3, 7, 6)
        sim = simulate(daily, date_list, cfg, update_interval_days=1, chunk_wildcard_rules=False)
        m = compute_metrics(sim, daily)
        logger.info(
            f"  [{branch}] DEBUG: n_tests={len(daily)}, "
            f"volatility={m.volatility:.4f}, n_mute={m.n_mute_transitions}, n_unmute={m.n_unmute_transitions}, "
            f"score={compute_normalized_score(m, w_mute, w_unmute, w_vol):.4f}"
        )
        return None

    study = None
    if (bootstrap_only or report_only) and best_params_override:
        best_params = best_params_override
        mode = "Report-only" if report_only else "Bootstrap-only"
        logger.info(f"  [{branch}] {mode} mode, using provided params: {best_params}")
    else:
        logger.info(f"  [{branch}] Running Optuna TPE, {n_trials} trials...")
        study = run_optuna(pre, pr_merged_rows, date_list, sample_ratio, n_trials, w_vol, w_mute, w_unmute)
        best = study.best_trial
        if not best:
            logger.warning(f"  [{branch}] No best trial.")
            return None
        best_params = best.params

    best_metrics, prod_metrics, baseline_metrics, _ = build_comparison_data(
        pre, pr_merged_rows, date_list, best_params, sample_ratio, w_vol, w_mute, w_unmute,
    )

    # Early exit when --bootstrap 0 and not report-only: stop after parameter selection
    if n_bootstrap <= 0 and not report_only:
        logger.info(f"  [{branch}] Bootstrap 0: stopping after parameter selection")
        sig = {'mean_diff': 0, 'ci_95': (0, 0), 'significant': False, 'n_boot': 0, 'skipped': True}
        return {
            'branch': branch,
            'study': study,
            'best_params': best_params,
            'best_metrics': best_metrics,
            'prod_metrics': prod_metrics,
            'baseline_metrics': baseline_metrics,
            'significance': sig,
            'best_days_trial': None,
            'best_runs_trial': None,
            'n_tests_wf': len(pre.wf),
            'n_tests_pr': len(pre.pr),
            'prod_muted_count': 0,
            'best_muted_count': 0,
            'diff_added': [],
            'diff_removed': [],
            'early_exit': True,
        }

    # Bootstrap significance (skip in report-only mode)
    if report_only:
        sig = {'mean_diff': 0, 'ci_95': (0, 0), 'significant': False, 'n_boot': 0, 'skipped': True}
        logger.info(f"  [{branch}] Report-only: skipping bootstrap")
    else:
        logger.info(f"  [{branch}] Bootstrap significance ({n_bootstrap} iterations)...")
        sig = bootstrap_significance(
            pre, pr_merged_rows, date_list, best_params,
            sample_ratio, n_bootstrap, w_vol, w_mute, w_unmute,
        )
        logger.info(f"  [{branch}] Bootstrap: mean_diff={sig['mean_diff']:.4f}, CI={sig['ci_95']}, significant={sig['significant']}")

    # Best per window_type (only when we ran Optuna)
    best_days = None
    best_runs = None
    if study is not None:
        for t in study.trials:
            if t.state != optuna.trial.TrialState.COMPLETE:
                continue
            wt = t.params.get('window_type', 'days')
            if wt == 'days' and (best_days is None or t.value < best_days.value):
                best_days = t
            elif wt == 'runs' and (best_runs is None or t.value < best_runs.value):
                best_runs = t

    # Generate mute lists (on full data, not sampled)
    logger.info(f"  [{branch}] Generating mute lists (full data)...")
    daily_wf_full = build_daily_fast(pre, 'wf_only', use_pr_merged=False)
    use_pr = best_params.get('use_pr', False)
    daily_best_full = build_daily_fast(
        pre, 'wf_and_pr' if use_pr else 'wf_only',
        use_pr_merged=use_pr)

    # Build best config (needed before simulate)
    bwt = best_params.get('window_type', 'days')
    if bwt == 'runs':
        cfg_best = ThresholdConfig(
            mute_days=0, mute_fail_threshold=best_params['mute_fail_threshold'],
            unmute_days=0, unmute_min_runs=0, window_type='runs',
            mute_last_runs=best_params.get('mute_last_runs', 20),
            unmute_last_runs=best_params.get('unmute_last_runs', 20),
        )
    elif bwt == 'hybrid':
        cfg_best = ThresholdConfig(
            mute_days=best_params.get('mute_days', 7),
            mute_fail_threshold=best_params['mute_fail_threshold'],
            unmute_days=best_params.get('unmute_days', 14), unmute_min_runs=0,
            window_type='hybrid',
            mute_last_runs=best_params.get('mute_last_runs', 20),
            unmute_last_runs=best_params.get('unmute_last_runs', 20),
        )
    else:
        cfg_best = ThresholdConfig(
            best_params.get('mute_days', 7), best_params['mute_fail_threshold'],
            best_params.get('unmute_days', 10), best_params.get('unmute_min_runs', 10),
            window_type='days',
        )

    chunk_rules = best_params.get('chunk_wildcard_rules', False)
    logger.info(f"  [{branch}]   Simulating prod config ({len(daily_wf_full)} tests, chunk_wildcard_rules=True)...")
    sim_prod_full = simulate(daily_wf_full, date_list, PROD_CONFIG, update_interval_days=1, show_progress=True, chunk_wildcard_rules=True)
    logger.info(f"  [{branch}]   Simulating best config ({len(daily_best_full)} tests, chunk_wildcard_rules={chunk_rules})...")
    sim_best_full = simulate(daily_best_full, date_list, cfg_best, update_interval_days=1, show_progress=True, chunk_wildcard_rules=chunk_rules)

    prod_muted = extract_muted_tests(sim_prod_full, daily_wf_full, PROD_CONFIG, date_list)
    best_muted = extract_muted_tests(sim_best_full, daily_best_full, cfg_best, date_list)

    # Write files
    out_dir = Path(__file__).parent / 'mute_optimizer_output'
    out_dir.mkdir(exist_ok=True)
    write_muted_list(prod_muted, out_dir / f'to_mute_prod_{branch}.txt')
    write_muted_list(best_muted, out_dir / f'to_mute_best_{branch}.txt')

    added, removed, unchanged = compute_mute_diff(prod_muted, best_muted)
    logger.info(f"  [{branch}] Mute diff: +{len(added)} opt adds, -{len(removed)} opt removes, "
                f"{len(unchanged)} unchanged")

    return {
        'branch': branch,
        'study': study,
        'best_params': best_params,
        'best_metrics': best_metrics,
        'prod_metrics': prod_metrics,
        'baseline_metrics': baseline_metrics,
        'significance': sig,
        'best_days_trial': best_days,
        'best_runs_trial': best_runs,
        'n_tests_wf': len(pre.wf),
        'n_tests_pr': len(pre.pr),
        'prod_muted_count': len(prod_muted),
        'best_muted_count': len(best_muted),
        'diff_added': added,
        'diff_removed': removed,
        'diff_unchanged': unchanged,
    }


def _build_verdict_html(bm: Dict, pm: Dict) -> str:
    """Build human-readable verdict: what's good, what's bad, overall assessment."""
    vol_b = bm.get('volatility', 0)
    vol_p = pm.get('volatility', 0)
    tm_b = bm.get('avg_time_to_mute') or 0
    tm_p = pm.get('avg_time_to_mute') or 0
    tu_b = bm.get('avg_time_to_unmute') or 0
    tu_p = pm.get('avg_time_to_unmute') or 0
    sc_b = bm.get('score', 0)
    sc_p = pm.get('score', 1)
    n_mute_b = bm.get('n_mute', 0)
    n_mute_p = pm.get('n_mute', 0)
    wrong_b = bm.get('wrong_state_tests_per_day', 0)
    wrong_p = pm.get('wrong_state_tests_per_day', 0)

    good = []
    bad = []
    # Priority 1: Reaction to mute
    if tm_b < tm_p:
        good.append(f"<strong>Отлично</strong> — Reaction to mute: {tm_p:.1f} → {tm_b:.1f} дн. Быстрее замьючиваем сломанные тесты.")
    elif tm_b > tm_p:
        bad.append(f"<strong>Хуже</strong> — Reaction to mute: {tm_p:.1f} → {tm_b:.1f} дн. Медленнее замьючиваем.")
    # Priority 2: Reaction to unmute
    if tu_b < tu_p:
        good.append(f"<strong>Отлично</strong> — Reaction to unmute: {tu_p:.1f} → {tu_b:.1f} дн. Быстрее размьючиваем починенные.")
    elif tu_b > tu_p:
        bad.append(f"<strong>Хуже</strong> — Reaction to unmute: {tu_p:.1f} → {tu_b:.1f} дн. Медленнее размьючиваем.")
    # Priority 3: Volatility
    if vol_b < vol_p:
        good.append(f"<strong>Хорошо</strong> — Volatility: {vol_p:.4f} → {vol_b:.4f}. Меньше переключений mute↔unmute.")
    elif vol_b > vol_p:
        bad.append(f"<strong>Хуже</strong> — Volatility: {vol_p:.4f} → {vol_b:.4f}. Больше переключений (менее стабильно).")
    # Score
    if sc_b < sc_p:
        pct = (1 - sc_b / sc_p) * 100 if sc_p > 0 else 0
        good.append(f"<strong>Score лучше на {pct:.0f}%</strong> — целевая функция улучшена.")
    # Critical: no new mutes
    if n_mute_p > 0 and n_mute_b == 0:
        bad.append(f"<strong>Критично</strong> — Mute transitions: {n_mute_p} → 0. Оптимизированный конфиг не мьет новые тесты. Система не будет реагировать на новые поломки.")
    elif wrong_b > wrong_p:
        bad.append(f"<strong>Проблема</strong> — Tests in wrong state/day: {wrong_p} → {wrong_b}. Больше ошибочно замьюченных/не замьюченных тестов.")

    # Overall verdict
    if bad and any("Критично" in b for b in bad):
        verdict = "Не готово к продакшену — есть критические недостатки."
        verdict_class = "worse"
    elif len(bad) > len(good):
        verdict = "Смешанный результат — улучшения есть, но и потери значимые. Рекомендуется донастройка."
        verdict_class = "worse"
    elif good and not bad:
        verdict = "Готово к продакшену — все метрики улучшились."
        verdict_class = "better"
    else:
        verdict = "Смешанный результат — часть метрик лучше, часть хуже. Оцените trade-off."
        verdict_class = "worse"

    parts = [
        "<div class=\"verdict-section\">",
        "<h4>Что хорошо:</h4><ul>",
    ]
    if good:
        parts.extend(f"<li>{g}</li>" for g in good)
    else:
        parts.append("<li>Нет заметных улучшений.</li>")
    parts.append("</ul><h4>Что плохо:</h4><ul>")
    if bad:
        parts.extend(f"<li>{b}</li>" for b in bad)
    else:
        parts.append("<li>Нет ухудшений.</li>")
    parts.append(f"</ul><p class=\"verdict-overall {verdict_class}\"><strong>Вердикт:</strong> {verdict}</p></div>")
    return "\n".join(parts)


def _build_diff_html(added: List[Dict], removed: List[Dict], unchanged: List[Dict] = None) -> str:
    """Build HTML for side-by-side prod vs opt mute diff.
    added = opt mutes, prod doesn't (opt adds to muted list)
    removed = prod mutes, opt doesn't (opt removes from muted list)
    unchanged = muted in both
    """
    unchanged = unchanged or []
    if not added and not removed:
        return '<p style="color:#888">No differences — prod and optimized mute the same tests.</p>'

    def _row(r: Dict, prod_muted: bool, opt_muted: bool, change: str) -> str:
        # muted_since: for added = when opt would mute; for removed = — (opt unmutes)
        muted_since = r.get('muted_since_date', '—') if opt_muted else '—'
        window = r.get('window_str', '—')
        return (
            f'<tr class="diff-{change}">'
            f'<td><code>{r["muted_ya"]}</code></td>'
            f'<td>{"✓ muted" if prod_muted else "— unmuted"}</td>'
            f'<td>{"✓ muted" if opt_muted else "— unmuted"}</td>'
            f'<td>{muted_since}</td>'
            f'<td>{window}</td>'
            f'<td>{r.get("total_fails", 0)}/{r.get("total_runs", 0)}</td>'
            f'<td>{r.get("fail_rate", 0):.1%}</td>'
            f'</tr>'
        )

    limit = 100
    added_rows = [_row(a, False, True, 'added') for a in added[:limit]]
    removed_rows = [_row(r, True, False, 'removed') for r in removed[:limit]]
    more = max(0, len(added) + len(removed) - limit)

    parts = [
        '<p><strong>Side-by-side:</strong> какие тесты в muted на сегодня — prod vs opt.</p>',
        '<table class="diff-table">',
        '<thead><tr>'
        '<th>Test</th>'
        '<th>Prod (muted today)</th>'
        '<th>Opt (muted today)</th>'
        '<th>muted_since (opt)</th>'
        '<th>Window (opt) fails/runs</th>'
        '<th>Fails/Runs</th>'
        '<th>Fail rate</th>'
        '</tr></thead>',
        '<tbody>',
    ]
    if added_rows:
        parts.append(f'<tr class="section-row"><td colspan="7" style="background:#e8f5e9;font-weight:bold">'
                    f'+ {len(added)} opt добавит в muted (prod не мьют)</td></tr>')
        parts.extend(added_rows)
    if removed_rows:
        parts.append(f'<tr class="section-row"><td colspan="7" style="background:#ffebee;font-weight:bold">'
                    f'− {len(removed)} opt уберёт из muted (prod мьют, opt размьют)</td></tr>')
        parts.extend(removed_rows)
    parts.append('</tbody></table>')
    if more > 0:
        parts.append(f'<p>... и ещё {more}</p>')

    # Unchanged (collapsible)
    if unchanged:
        u_limit = 50
        u_rows = ''.join(_row(u, True, True, 'unchanged') for u in unchanged[:u_limit])
        u_more = f'<p>... и ещё {len(unchanged) - u_limit}</p>' if len(unchanged) > u_limit else ''
        parts.append(f'''
        <details>
            <summary style="cursor:pointer;color:#666">Без изменений (muted в обоих): {len(unchanged)} тестов</summary>
            <table class="diff-table">
                <thead><tr><th>Test</th><th>Prod</th><th>Opt</th><th>muted_since</th><th>Window</th><th>F/R</th><th>Rate</th></tr></thead>
                <tbody>{u_rows}</tbody>
            </table>{u_more}
        </details>''')

    return '\n'.join(parts)


def generate_multi_branch_html(
    results: List[Dict],
    days: int,
    out_path: Path,
) -> None:
    """Multi-branch HTML report with per-branch sections and cross-branch comparison."""

    branch_sections = []
    summary_rows = []

    for r in results:
        branch = r['branch']
        bm = r['best_metrics']
        pm = r['prod_metrics']
        bp = r['best_params']
        sig = r.get('significance', {})
        study = r.get('study')

        vol_b = bm['volatility']
        vol_p = pm['volatility']
        tm_b = bm.get('avg_time_to_mute') or 0
        tm_p = pm.get('avg_time_to_mute') or 0
        tu_b = bm.get('avg_time_to_unmute') or 0
        tu_p = pm.get('avg_time_to_unmute') or 0
        sc_b = bm['score']
        sc_p = pm['score']
        pct = (1 - sc_b / sc_p) * 100 if sc_p > 0 else 0
        wt = bp.get('window_type', 'days')
        use_pr = bp.get('use_pr', False)

        ci = sig.get('ci_95', (0, 0))
        if sig.get('skipped'):
            sig_str = 'skipped'
        else:
            sig_str = 'SIGNIFICANT' if sig.get('significant') else 'not significant'
        sig_class = 'better' if sig.get('significant') else 'worse'

        chunk_rules = bp.get('chunk_wildcard_rules', False)
        if wt == 'hybrid':
            params_str = (
                f"window_type=hybrid, mute: last {bp.get('mute_last_runs')} runs within {bp.get('mute_days')}d, "
                f"unmute: last {bp.get('unmute_last_runs')} runs within {bp.get('unmute_days')}d, "
                f"mute_fail>={bp.get('mute_fail_threshold')}, use_pr={use_pr}, chunk_wildcard_rules={chunk_rules}"
            )
        elif wt == 'runs':
            params_str = (
                f"window_type=runs, mute_last_runs={bp.get('mute_last_runs')}, "
                f"mute_fail>={bp.get('mute_fail_threshold')}, unmute_last_runs={bp.get('unmute_last_runs')}, "
                f"use_pr={use_pr}, chunk_wildcard_rules={chunk_rules}"
            )
        else:
            params_str = (
                f"window_type=days, mute_days={bp.get('mute_days')}, "
                f"mute_fail>={bp.get('mute_fail_threshold')}, unmute_days={bp.get('unmute_days')}, "
                f"unmute_runs>={bp.get('unmute_min_runs')}, use_pr={use_pr}, chunk_wildcard_rules={chunk_rules}"
            )

        # Optuna history data
        history_js = "[]"
        best_so_far_js = "[]"
        days_scores_js = "[]"
        runs_scores_js = "[]"
        hybrid_scores_js = "[]"
        param_importance_js = "{}"
        if study:
            completed = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
            trial_vals = [round(t.value, 4) for t in completed]
            bv = float('inf')
            best_vals = []
            for v in trial_vals:
                bv = min(bv, v)
                best_vals.append(round(bv, 4))
            history_js = json.dumps(trial_vals)
            best_so_far_js = json.dumps(best_vals)
            ds = [(t.number, round(t.value, 4)) for t in completed if t.params.get('window_type') == 'days']
            rs = [(t.number, round(t.value, 4)) for t in completed if t.params.get('window_type') == 'runs']
            hs = [(t.number, round(t.value, 4)) for t in completed if t.params.get('window_type') == 'hybrid']
            days_scores_js = json.dumps(ds)
            runs_scores_js = json.dumps(rs)
            hybrid_scores_js = json.dumps(hs)
            try:
                importance = optuna.importance.get_param_importances(study)
            except Exception:
                importance = {}
            all_params = set(PARAM_DESCRIPTIONS.keys())
            param_importance_full = [
                {'param': p, 'imp': round(importance.get(p, 0), 4), 'desc': PARAM_DESCRIPTIONS.get(p, '')}
                for p in sorted(all_params, key=lambda x: -importance.get(x, 0))
            ]
            param_importance_js = json.dumps(param_importance_full, ensure_ascii=False)
        else:
            param_importance_js = json.dumps(
                [{'param': p, 'imp': 0, 'desc': PARAM_DESCRIPTIONS.get(p, '')} for p in sorted(PARAM_DESCRIPTIONS.keys())],
                ensure_ascii=False
            )

        safe_branch = branch.replace('-', '_').replace('.', '_')

        section = f'''
    <div class="branch-section" id="branch-{safe_branch}">
        <h2>{branch}</h2>
        <p>WF tests: {r.get('n_tests_wf', '?')} | PR tests: {r.get('n_tests_pr', '?')}</p>

        <h3>Best Config</h3>
        <p><strong>{params_str}</strong></p>

        <h3>Production vs Optimized</h3>
        <table>
            <thead><tr><th>Metric</th><th>Production</th><th>Optimized</th><th>\u0394</th></tr></thead>
            <tbody>
                <tr><td>Volatility</td><td>{vol_p:.4f}</td><td>{vol_b:.4f}</td><td class="{'better' if vol_b < vol_p else 'worse'}">{vol_b-vol_p:+.4f}</td></tr>
                <tr><td>Reaction to mute (d)</td><td>{tm_p:.1f}</td><td>{tm_b:.1f}</td><td class="{'better' if tm_b < tm_p else 'worse'}">{tm_b-tm_p:+.1f}</td></tr>
                <tr><td>Reaction to unmute (d)</td><td>{tu_p:.1f}</td><td>{tu_b:.1f}</td><td class="{'better' if tu_b < tu_p else 'worse'}">{tu_b-tu_p:+.1f}</td></tr>
                <tr><td>Score</td><td>{sc_p:.4f}</td><td>{sc_b:.4f}</td><td class="{'better' if sc_b < sc_p else 'worse'}">{sc_b-sc_p:+.4f} ({pct:+.1f}%)</td></tr>
                <tr><td>Mute transitions</td><td>{pm.get('n_mute',0)}</td><td>{bm.get('n_mute',0)}</td><td>{bm.get('n_mute',0)-pm.get('n_mute',0):+d}</td></tr>
                <tr><td>Unmute transitions</td><td>{pm.get('n_unmute',0)}</td><td>{bm.get('n_unmute',0)}</td><td>{bm.get('n_unmute',0)-pm.get('n_unmute',0):+d}</td></tr>
            </tbody>
        </table>

        <h3>What changes in practice?</h3>
        <table>
            <thead><tr><th>Metric</th><th>Production</th><th>Optimized</th><th>Meaning</th></tr></thead>
            <tbody>
                <tr>
                    <td>Tests in wrong state / day</td>
                    <td>{pm.get('wrong_state_tests_per_day', 0)}</td>
                    <td>{bm.get('wrong_state_tests_per_day', 0)}</td>
                    <td>{'Fewer tests misclassified daily' if bm.get('wrong_state_tests_per_day',0) < pm.get('wrong_state_tests_per_day',0) else 'More tests misclassified daily \u2014 worse'}</td>
                </tr>
                <tr>
                    <td>Total blocked test-days</td>
                    <td>{pm.get('blocked_test_days', 0)}</td>
                    <td>{bm.get('blocked_test_days', 0)}</td>
                    <td>Days devs see broken unmuted tests before mute kicks in</td>
                </tr>
                <tr>
                    <td>Total stale mute test-days</td>
                    <td>{pm.get('stale_mute_days', 0)}</td>
                    <td>{bm.get('stale_mute_days', 0)}</td>
                    <td>Days fixed tests sit muted before unmute kicks in</td>
                </tr>
                <tr>
                    <td>Mute events / day</td>
                    <td>{pm.get('mute_events_per_day', 0)}</td>
                    <td>{bm.get('mute_events_per_day', 0)}</td>
                    <td>How often the system mutes tests</td>
                </tr>
                <tr>
                    <td>Unmute events / day</td>
                    <td>{pm.get('unmute_events_per_day', 0)}</td>
                    <td>{bm.get('unmute_events_per_day', 0)}</td>
                    <td>How often the system unmutes tests</td>
                </tr>
            </tbody>
        </table>

        <h3>Вердикт: хорошо или плохо?</h3>
        {_build_verdict_html(bm, pm)}

        <h3>Statistical Significance (Bootstrap{sig.get('n_boot', 0) and f", {sig.get('n_boot', 0)} iter" or ", skipped"})</h3>
        {"<p>Bootstrap was skipped (--bootstrap 0).</p>" if sig.get('skipped') else f"""<p>Score diff (best−prod): mean={sig.get('mean_diff', 0):.4f}, 95% CI=[{ci[0]:+.4f}, {ci[1]:+.4f}]</p>
        <p class="{sig_class}"><strong>{sig_str}</strong>
           {"— the entire 95% CI is below 0, improvement is real" if sig.get('significant') else "— CI crosses 0, improvement may be noise"}</p>"""}

        <h3>Mute List Diff (prod \u2192 optimized)</h3>
        <p>Production muted: <strong>{r.get('prod_muted_count', '?')}</strong> tests |
           Optimized muted: <strong>{r.get('best_muted_count', '?')}</strong> tests |
           Files: <code>mute_optimizer_output/to_mute_prod_{branch}.txt</code>, <code>to_mute_best_{branch}.txt</code></p>
        {_build_diff_html(r.get('diff_added', []), r.get('diff_removed', []), r.get('diff_unchanged', []))}

        <h3>Optimization History</h3>
        <div class="charts-row">
            <div class="chart-container"><canvas id="history_{safe_branch}" height="250"></canvas></div>
            <div class="chart-container"><canvas id="model_{safe_branch}" height="250"></canvas></div>
        </div>
        <h3>Parameter Importance</h3>
        <p style="color:#666; font-size: 0.9em;">use_pr=0 — фиксирован через OPTIMIZATION_MODE.</p>
        <div class="charts-row">
            <div class="chart-container"><canvas id="imp_{safe_branch}" height="280"></canvas></div>
        </div>
        <table style="margin-top: 12px; max-width: 600px; font-size: 0.9em;">
            <thead><tr><th>Параметр</th><th>За что отвечает</th></tr></thead>
            <tbody id="paramDesc_{safe_branch}"></tbody>
        </table>
    </div>
    <script>
    (function() {{
        const hData = {history_js};
        const bsf = {best_so_far_js};
        new Chart(document.getElementById('history_{safe_branch}').getContext('2d'), {{
            type: 'line',
            data: {{
                labels: hData.map((_,i) => i),
                datasets: [
                    {{ label: 'Trial score', data: hData, borderColor: 'rgba(100,100,100,0.5)', pointRadius: 3, fill: false, tension: 0 }},
                    {{ label: 'Best so far', data: bsf, borderColor: 'rgba(50,120,200,1)', borderWidth: 2, pointRadius: 0, fill: false, tension: 0.1 }}
                ]
            }},
            options: {{ scales: {{ y: {{ beginAtZero: true, title: {{ display: true, text: 'Score' }} }} }}, plugins: {{ title: {{ display: true, text: '{branch} — Optimization History' }} }} }}
        }});

        const ds = {days_scores_js};
        const rs = {runs_scores_js};
        const hy = {hybrid_scores_js};
        new Chart(document.getElementById('model_{safe_branch}').getContext('2d'), {{
            type: 'scatter',
            data: {{
                datasets: [
                    {{ label: 'days', data: ds.map(d => ({{x:d[0],y:d[1]}})), backgroundColor: 'rgba(50,120,200,0.7)', pointRadius: 5 }},
                    {{ label: 'runs', data: rs.map(d => ({{x:d[0],y:d[1]}})), backgroundColor: 'rgba(220,80,50,0.7)', pointRadius: 5 }},
                    {{ label: 'hybrid', data: hy.map(d => ({{x:d[0],y:d[1]}})), backgroundColor: 'rgba(50,180,80,0.7)', pointRadius: 5 }}
                ]
            }},
            options: {{ scales: {{ y: {{ beginAtZero: true, title: {{ display: true, text: 'Score' }} }} }}, plugins: {{ title: {{ display: true, text: '{branch} — days vs runs vs hybrid' }} }} }}
        }});

        const paramImp = {param_importance_js};
        const impLabels = paramImp.map(p => p.param);
        const impValues = paramImp.map(p => p.imp);
        new Chart(document.getElementById('imp_{safe_branch}').getContext('2d'), {{
            type: 'bar',
            data: {{
                labels: impLabels,
                datasets: [{{ label: 'Importance', data: impValues, backgroundColor: impLabels.map((_,i) => `hsl(${{Math.max(0, 40-i*15)}},65%,55%)`) }}]
            }},
            options: {{ indexAxis: 'y', scales: {{ x: {{ beginAtZero: true, max: 1.0 }} }}, plugins: {{ title: {{ display: true, text: '{branch} — Parameter Importance' }}, legend: {{ display: false }} }} }}
        }});
        document.getElementById('paramDesc_{safe_branch}').innerHTML = paramImp.map(p =>
            `<tr><td><code>${{p.param}}</code></td><td>${{p.desc || '—'}}</td></tr>`
        ).join('');
    }})();
    </script>
'''
        branch_sections.append(section)

        # Summary row for cross-branch table
        summary_rows.append(f'''
            <tr>
                <td><a href="#branch-{safe_branch}">{branch}</a></td>
                <td>{wt}</td>
                <td>{sc_p:.4f}</td>
                <td>{sc_b:.4f}</td>
                <td class="{'better' if pct > 0 else 'worse'}">{pct:+.1f}%</td>
                <td>{tm_b:.1f}</td>
                <td>{tu_b:.1f}</td>
                <td>[{ci[0]:+.4f}, {ci[1]:+.4f}]</td>
                <td class="{sig_class}">{sig_str}</td>
            </tr>''')

    html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Mute Optimizer v3 ML — Multi-Branch Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{ font-family: system-ui, sans-serif; margin: 24px; max-width: 1200px; }}
        h1 {{ color: #222; }}
        h2 {{ color: #333; margin-top: 40px; border-bottom: 2px solid #4a90d9; padding-bottom: 8px; }}
        h3 {{ color: #555; margin-top: 20px; }}
        .summary-table {{ margin: 20px 0; }}
        .charts-row {{ display: flex; flex-wrap: wrap; gap: 24px; margin: 12px 0; }}
        .chart-container {{ flex: 1; min-width: 380px; max-width: 550px; }}
        canvas {{ max-width: 100%; }}
        table {{ border-collapse: collapse; margin-top: 12px; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: left; }}
        th {{ background: #f5f5f5; }}
        .better {{ color: #0a0; font-weight: bold; }}
        .worse {{ color: #c00; }}
        .diff-added {{ background: #e8f5e9; }}
        .diff-removed {{ background: #ffebee; }}
        .diff-unchanged {{ background: #fafafa; }}
        .branch-section {{ border: 1px solid #e0e0e0; border-radius: 8px; padding: 16px 24px; margin-top: 24px; background: #fafafa; }}
        .verdict-section {{ background: #f8f9fa; padding: 16px; border-radius: 8px; margin: 16px 0; border-left: 4px solid #6c757d; }}
        .verdict-section ul {{ margin: 8px 0; padding-left: 20px; }}
        .verdict-overall {{ font-size: 1.1em; margin-top: 12px; padding: 10px; border-radius: 4px; }}
        .verdict-overall.better {{ background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }}
        .verdict-overall.worse {{ background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }}
        .nav {{ display: flex; gap: 12px; margin: 16px 0; flex-wrap: wrap; }}
        .nav a {{ padding: 6px 16px; background: #4a90d9; color: #fff; border-radius: 4px; text-decoration: none; }}
        .nav a:hover {{ background: #357abd; }}
    </style>
</head>
<body>
    <h1>Mute Optimizer v3 ML — Multi-Branch</h1>
    <p>Branches: <strong>{', '.join(r['branch'] for r in results)}</strong> | Days: <strong>{days}</strong></p>
    <div class="summary" style="margin: 12px 0; padding: 12px; background: #f0f7ff; border-radius: 8px;">
        <p><strong>Prod:</strong> WF-only, MUTE_DAYS=4, fail&gt;=3|2, UNMUTE_DAYS=7, UNMUTE_MIN_RUNS=4</p>
        <p><strong>Optimized:</strong> see per-branch sections below (window_type, thresholds, use_pr)</p>
    </div>

    <div class="nav">
        {''.join(f'<a href="#branch-{r["branch"].replace("-","_").replace(".","_")}">{r["branch"]}</a>' for r in results)}
    </div>

    <h2>Cross-Branch Summary</h2>
    <table class="summary-table">
        <thead>
            <tr>
                <th>Branch</th><th>Best Window</th><th>Prod Score</th><th>Best Score</th>
                <th>Improvement</th><th>Mute React (d)</th><th>Unmute React (d)</th>
                <th>95% CI (diff)</th><th>Significant?</th>
            </tr>
        </thead>
        <tbody>
            {''.join(summary_rows)}
        </tbody>
    </table>

    {''.join(branch_sections)}
</body>
</html>'''
    out_path.write_text(html, encoding='utf-8')
    logger.info(f"Wrote {out_path}")


def main():
    if not HAS_OPTUNA:
        logger.error("Optuna not installed. Run: pip install optuna")
        sys.exit(1)

    ap = argparse.ArgumentParser(description="Mute optimizer v3 ML (cache, PR last merged, Optuna, viz)")
    ap.add_argument('--branches', default='main',
                     help='Comma-separated branches (e.g. main,stable-25-4-1,stable-25-3)')
    ap.add_argument('--build-type', default='relwithdebinfo')
    ap.add_argument('--days', type=int, default=60)
    ap.add_argument('--sample', type=float, default=0.05)
    ap.add_argument('--no-cache', action='store_true')
    ap.add_argument('--trials', type=int, default=40)
    ap.add_argument('--bootstrap', type=int, default=100, help='Bootstrap iterations for significance test (0 to skip)')
    ap.add_argument('--bootstrap-only', action='store_true',
                    help='Skip Optuna, run bootstrap+report with --best-params')
    ap.add_argument('--report-only', action='store_true',
                    help='Skip Optuna and bootstrap, generate mute lists + HTML with --best-params')
    ap.add_argument('--best-params', type=str, default=None,
                    help='JSON params for --bootstrap-only/--report-only')
    ap.add_argument('--w-vol', type=float, default=1.0, help='Weight for volatility (priority 3)')
    ap.add_argument('--w-mute', type=float, default=5.0, help='Weight for avg_time_to_mute (priority 1)')
    ap.add_argument('--w-unmute', type=float, default=2.0, help='Weight for avg_time_to_unmute (priority 2)')
    ap.add_argument('--out', '-o', default='mute_optimizer_v3_ml_report.html', help='Output HTML path')
    ap.add_argument('--no-open', action='store_true', help='Do not open browser')
    ap.add_argument('--debug', action='store_true', help='Quick debug run per branch')
    args = ap.parse_args()

    branches = [b.strip() for b in args.branches.split(',') if b.strip()]
    all_branch_results: List[Dict] = []

    best_params_override = None
    if args.bootstrap_only or args.report_only:
        if not args.best_params:
            logger.error("--bootstrap-only and --report-only require --best-params (JSON)")
            sys.exit(1)
        try:
            best_params_override = json.loads(args.best_params)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid --best-params JSON: {e}")
            sys.exit(1)

    for branch in branches:
        logger.info(f"\n{'='*60}\nBranch: {branch}\n{'='*60}")
        result = run_single_branch(
            branch, args.build_type, args.days, args.sample, args.no_cache,
            args.trials, args.bootstrap, args.w_vol, args.w_mute, args.w_unmute,
            debug=args.debug,
            bootstrap_only=args.bootstrap_only,
            report_only=args.report_only,
            best_params_override=best_params_override,
        )
        if result:
            all_branch_results.append(result)

    if not all_branch_results:
        logger.error("No results for any branch.")
        sys.exit(1)

    early_exit = any(r.get('early_exit') for r in all_branch_results)

    # CLI summary
    print(f"\n{'='*60}")
    print("SUMMARY: all branches")
    print(f"{'='*60}")
    for r in all_branch_results:
        br = r['branch']
        bm = r['best_metrics']
        pm = r['prod_metrics']
        bp = r['best_params']
        sig = r.get('significance', {})
        wt = bp.get('window_type', 'days')
        pct = (1 - bm['score'] / pm['score']) * 100 if pm['score'] > 0 else 0
        sig_str = ""
        if sig and not sig.get('skipped'):
            ci = sig.get('ci_95', (0, 0))
            sig_str = f" | 95%CI=[{ci[0]:+.4f},{ci[1]:+.4f}] {'SIGNIFICANT' if sig.get('significant') else 'not significant'}"
        elif sig and sig.get('skipped'):
            sig_str = " | bootstrap skipped"
        print(f"  {br}: prod={pm['score']:.4f} best={bm['score']:.4f} ({pct:+.1f}%) window={wt}{sig_str}")

    if early_exit:
        print("\nStopped after parameter selection (--bootstrap 0). No HTML report.")
    else:
        out_path = Path(args.out)
        if not out_path.is_absolute():
            out_path = Path(__file__).parent / out_path
        generate_multi_branch_html(all_branch_results, args.days, out_path)
        print(f"\nReport: {out_path}")
        if not args.no_open:
            webbrowser.open(f"file://{out_path.resolve()}")


if __name__ == "__main__":
    main()
