#!/usr/bin/env python3
"""Per-PR push→result cycle analysis for charts and team reports."""

from __future__ import annotations

import argparse
import json
import statistics
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pr_check_model import (
    PrCheckRun,
    build_pr_check_runs,
    estimate_shard_count,
    parse_ts,
    sharded_rwdi_timeline,
)

import sys

_sharding_dir = Path(__file__).resolve().parent / "vendor" / "sharding"
if str(_sharding_dir) not in sys.path:
    sys.path.insert(0, str(_sharding_dir))
from choose_shard_count import is_peak_hour_utc  # noqa: E402

ROOT = Path(__file__).resolve().parent
DEFAULT_DATA = ROOT / "data" / "jobs_14d.json"

# Presentation scenario buckets (Russian labels for charts).
SCENARIO_ORDER = [
    ("light_single", "Лёгкий PR\n(classifier → single)"),
    ("medium_n1", "Средний PR\n(heavy path, N=1)"),
    ("shard_n4", "Длинный PR\n(оценка ≥60 мин → N=4)"),
    ("shard_n8", "Тяжёлый PR\n(оценка ≥120 мин → N=8)"),
    ("shard_n12", "Очень тяжёлый\n(оценка ≥200 мин → N=12)"),
    ("other", "Прочие"),
]


@dataclass
class CycleRecord:
    run_id: int
    pr_number: int | None
    classifier_mode: str
    shard_count: int
    estimated_d_min: float
    is_peak_utc: bool
    mono_rwdi_min: float
    mono_asan_min: float
    baseline_cycle_min: float
    parallel_rwdi_min: float
    parallel_cycle_min: float
    cycle_saved_min: float
    cycle_speedup_pct: float
    parallel_runner_min: float
    baseline_runner_min: float
    extra_runner_min: float
    scenario_key: str
    scenario_label: str
    prepare_min: float
    shard_phase_min: float


def cycle_wall_sec(
    rwdi: dict[str, Any] | None,
    asan: dict[str, Any] | None,
) -> float:
    """Push → last job done (relwithdebinfo + release-asan in parallel)."""
    starts: list[float] = []
    ends: list[float] = []
    for job in (rwdi, asan):
        if not job:
            continue
        starts.append(parse_ts(job["started_at"]).timestamp())
        ends.append(parse_ts(job["completed_at"]).timestamp())
    if not starts:
        return 0.0
    return max(ends) - min(starts)


def scenario_for(run: PrCheckRun, shard_count: int, classifier_mode: str) -> tuple[str, str]:
    if classifier_mode == "single":
        key = "light_single"
    elif shard_count <= 1:
        key = "medium_n1"
    elif shard_count == 4:
        key = "shard_n4"
    elif shard_count == 8:
        key = "shard_n8"
    elif shard_count >= 12:
        key = "shard_n12"
    else:
        key = "other"
    label = dict(SCENARIO_ORDER).get(key, key)
    return key, label


def parallel_rwdi_minutes(run: PrCheckRun) -> tuple[float, int, float, float, float]:
    mono = float(run.rwdi_job["duration_sec"])
    started = parse_ts(run.rwdi_job["started_at"])
    if run.mode == "single":
        return mono / 60.0, 1, 0.0, mono / 60.0, mono / 60.0

    wall, shard_count, prepare_sec, shard_sec = sharded_rwdi_timeline(
        mono, started_at=started, capacity_cap=12
    )
    if shard_count <= 1:
        return mono / 60.0, 1, 0.0, mono / 60.0, mono / 60.0
    return wall / 60.0, shard_count, prepare_sec / 60.0, shard_sec / 60.0, wall / 60.0


def analyze(jobs: list[dict[str, Any]], pr_runs: list[PrCheckRun]) -> list[CycleRecord]:
    records: list[CycleRecord] = []
    for run in pr_runs:
        mono_rwdi = float(run.rwdi_job["duration_sec"]) / 60.0
        mono_asan = (
            float(run.asan_job["duration_sec"]) / 60.0 if run.asan_job else mono_rwdi * 0.85
        )
        baseline_cycle = cycle_wall_sec(run.rwdi_job, run.asan_job) / 60.0

        par_rwdi, shard_count, prepare_min, shard_min, _ = parallel_rwdi_minutes(run)
        # ASAN unchanged; parallel cycle = max(rwdi_parallel, asan) relative to same push.
        rwdi_start = parse_ts(run.rwdi_job["started_at"]).timestamp()
        asan_start = (
            parse_ts(run.asan_job["started_at"]).timestamp()
            if run.asan_job
            else rwdi_start
        )
        push = min(rwdi_start, asan_start)
        asan_end = (
            parse_ts(run.asan_job["completed_at"]).timestamp()
            if run.asan_job
            else rwdi_start + mono_asan * 60
        )
        par_rwdi_end = rwdi_start + par_rwdi * 60
        parallel_cycle = (max(par_rwdi_end, asan_end) - push) / 60.0

        started = parse_ts(run.rwdi_job["started_at"])
        _, est_d = estimate_shard_count(
            float(run.rwdi_job["duration_sec"]),
            started_at=started,
            capacity_cap=12,
        )
        hour = started.astimezone(__import__("datetime").timezone.utc).hour
        is_peak = is_peak_hour_utc(hour)

        baseline_runner = mono_rwdi + mono_asan
        if run.mode == "single" or shard_count <= 1:
            parallel_runner = mono_rwdi + mono_asan
        else:
            parallel_runner = prepare_min + shard_count * shard_min + mono_asan

        saved = baseline_cycle - parallel_cycle
        speedup = 100.0 * saved / baseline_cycle if baseline_cycle > 0 else 0.0
        scenario_key, scenario_label = scenario_for(run, shard_count, run.mode)

        records.append(
            CycleRecord(
                run_id=run.run_id,
                pr_number=run.pr_number,
                classifier_mode=run.mode,
                shard_count=shard_count,
                estimated_d_min=round(est_d, 1),
                is_peak_utc=is_peak,
                mono_rwdi_min=round(mono_rwdi, 1),
                mono_asan_min=round(mono_asan, 1),
                baseline_cycle_min=round(baseline_cycle, 1),
                parallel_rwdi_min=round(par_rwdi, 1),
                parallel_cycle_min=round(parallel_cycle, 1),
                cycle_saved_min=round(saved, 1),
                cycle_speedup_pct=round(speedup, 1),
                parallel_runner_min=round(parallel_runner, 1),
                baseline_runner_min=round(baseline_runner, 1),
                extra_runner_min=round(parallel_runner - baseline_runner, 1),
                scenario_key=scenario_key,
                scenario_label=scenario_label,
                prepare_min=round(prepare_min, 1),
                shard_phase_min=round(shard_min, 1),
            )
        )
    return records


def aggregate_by_scenario(records: list[CycleRecord]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, label in SCENARIO_ORDER:
        subset = [r for r in records if r.scenario_key == key]
        if not subset:
            continue
        base_cycles = [r.baseline_cycle_min for r in subset]
        par_cycles = [r.parallel_cycle_min for r in subset]
        saved = [r.cycle_saved_min for r in subset]
        out[key] = {
            "label": label.replace("\n", " "),
            "count": len(subset),
            "baseline_cycle_median_min": round(statistics.median(base_cycles), 1),
            "baseline_cycle_p90_min": round(_p90(base_cycles), 1),
            "parallel_cycle_median_min": round(statistics.median(par_cycles), 1),
            "parallel_cycle_p90_min": round(_p90(par_cycles), 1),
            "median_saved_min": round(statistics.median(saved), 1),
            "median_speedup_pct": round(
                statistics.median([r.cycle_speedup_pct for r in subset]), 1
            ),
            "total_extra_runner_hours": round(
                sum(r.extra_runner_min for r in subset) / 60.0, 1
            ),
        }
    return out


def _p90(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round(0.9 * (len(ordered) - 1)))
    return ordered[idx]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "data" / "pr_cycle_details.json",
    )
    parser.add_argument("--skip-classify", action="store_true")
    args = parser.parse_args()

    payload = json.loads(args.data.read_text(encoding="utf-8"))
    pr_runs = build_pr_check_runs(payload["jobs"], classify=not args.skip_classify)
    records = analyze(payload["jobs"], pr_runs)

    result = {
        "records": [asdict(r) for r in records],
        "by_scenario": aggregate_by_scenario(records),
        "totals": {
            "pr_checks": len(records),
            "median_cycle_saved_min": round(
                statistics.median([r.cycle_saved_min for r in records]), 1
            ),
            "median_speedup_pct": round(
                statistics.median([r.cycle_speedup_pct for r in records]), 1
            ),
            "total_extra_runner_hours": round(
                sum(r.extra_runner_min for r in records) / 60.0, 1
            ),
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"wrote {len(records)} cycle records to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
