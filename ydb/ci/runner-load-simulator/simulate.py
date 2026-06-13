#!/usr/bin/env python3
"""Discrete-event replay of the shared runner pool: baseline vs pr_check_parallel."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from pool import PoolConfig, PoolSimulator
from pr_check_model import PrCheckRun, build_pr_check_runs, parse_ts, sharded_rwdi_timeline

ROOT = Path(__file__).resolve().parent
DEFAULT_DATA = ROOT / "data" / "jobs_14d.json"
DEFAULT_CAPACITY = Path(__file__).resolve().parent / "vendor" / "runner_capacity.yml"


@dataclass
class WorkItem:
    start: float
    preset: str
    key: str
    duration_sec: float
    parallel_count: int = 1
    is_pr_rwdi: bool = False
    is_pr_check: bool = False


@dataclass
class AllocEvent:
    requested_at: float
    started_at: float
    ended_at: float
    wait_sec: float
    work_sec: float
    preset: str
    parallel_count: int
    is_pr_rwdi: bool
    is_pr_check: bool
    key: str


@dataclass
class ScenarioResult:
    name: str
    pool: PoolSimulator
    pr_wall_times: list[float]
    pr_queue_waits: list[float]
    pr_sharded: int
    pr_single: int
    pr_runner_seconds: float
    window_start: float
    window_end: float
    alloc_events: list[AllocEvent]

    @property
    def horizon_sec(self) -> float:
        return max(self.window_end - self.window_start, 1.0)


def job_start_epoch(job: dict[str, Any]) -> float:
    return parse_ts(job["started_at"]).timestamp()


def is_pr_check_job(job: dict[str, Any]) -> bool:
    return job["workflow_name"] == "PR-check" and (
        "relwithdebinfo" in job["job_name"] or "asan" in job["job_name"]
    )


def is_pr_check_rwdi(job: dict[str, Any]) -> bool:
    return job["workflow_name"] == "PR-check" and "relwithdebinfo" in job["job_name"]


def build_work_items(
    jobs: list[dict[str, Any]],
    pr_runs: list[PrCheckRun],
    *,
    parallel: bool,
    shard_eligible: Callable[[PrCheckRun], bool] | None = None,
) -> list[WorkItem]:
    sharded_run_ids = {
        run.run_id
        for run in pr_runs
        if parallel
        and run.mode == "sharded"
        and (shard_eligible is None or shard_eligible(run))
    }
    pr_by_run = {run.run_id: run for run in pr_runs}
    items: list[WorkItem] = []

    for job in jobs:
        if parallel and job["run_id"] in sharded_run_ids and is_pr_check_rwdi(job):
            continue
        start = job_start_epoch(job)
        duration = float(job["duration_sec"])
        items.append(
            WorkItem(
                start=start,
                preset=job["preset"],
                key=str(job["job_id"]),
                duration_sec=duration,
                is_pr_rwdi=is_pr_check_rwdi(job),
                is_pr_check=is_pr_check_job(job),
            )
        )

    if parallel:
        for run in pr_runs:
            if run.mode != "sharded":
                continue
            if shard_eligible is not None and not shard_eligible(run):
                continue
            start = job_start_epoch(run.rwdi_job)
            mono = float(run.rwdi_job["duration_sec"])
            wall, shard_count, prepare_sec, shard_sec = sharded_rwdi_timeline(
                mono,
                started_at=parse_ts(run.rwdi_job["started_at"]),
                capacity_cap=12,
            )
            run.shard_count = shard_count
            run.rwdi_wall_sec = wall
            # Adaptive N=1: same cost model as monolith (no prepare/list overhead).
            if shard_count <= 1:
                items.append(
                    WorkItem(
                        start=start,
                        preset=run.rwdi_job["preset"],
                        key=str(run.rwdi_job["job_id"]),
                        duration_sec=mono,
                        is_pr_rwdi=True,
                        is_pr_check=True,
                    )
                )
                continue
            items.append(
                WorkItem(
                    start=start,
                    preset=run.rwdi_job["preset"],
                    key=f"prepare:{run.run_id}",
                    duration_sec=prepare_sec,
                    is_pr_rwdi=True,
                    is_pr_check=True,
                )
            )
            items.append(
                WorkItem(
                    start=start + prepare_sec,
                    preset=run.rwdi_job["preset"],
                    key=f"shards:{run.run_id}",
                    duration_sec=shard_sec,
                    parallel_count=shard_count,
                    is_pr_rwdi=True,
                    is_pr_check=True,
                )
            )

    return sorted(items, key=lambda item: item.start)


def replay(
    items: list[WorkItem],
    config: PoolConfig,
    *,
    name: str,
    pr_runs: list[PrCheckRun],
    parallel: bool,
) -> ScenarioResult:
    pool = PoolSimulator(config=config)
    pr_walls: list[float] = []
    pr_waits: list[float] = []
    pr_runner_seconds = 0.0
    sharded = single = 0
    alloc_events: list[AllocEvent] = []
    window_start = items[0].start if items else 0.0
    window_end = window_start

    # Track per-run PR wall clock for sharded path.
    pr_partial: dict[str, dict[str, float]] = {}

    for item in items:
        if parallel and item.key.startswith("shards:"):
            run_id = int(item.key.split(":")[1])
            run = next(r for r in pr_runs if r.run_id == run_id)
            cap = pool.capacity_cap(item.preset)
            mono = float(run.rwdi_job["duration_sec"])
            _, shard_count, prepare_sec, shard_sec = sharded_rwdi_timeline(
                mono,
                started_at=parse_ts(run.rwdi_job["started_at"]),
                capacity_cap=cap,
            )
            run.shard_count = shard_count
            item.parallel_count = shard_count
            item.duration_sec = shard_sec
            # align prepare item for this run
            prep_key = f"prepare:{run_id}"
            for other in items:
                if other.key == prep_key:
                    other.duration_sec = prepare_sec
                    break

        if item.parallel_count == 1:
            wait = pool.allocate(item.start, item.duration_sec, item.preset, item.key)
            end = item.start + wait + item.duration_sec
        else:
            wait = pool.allocate_parallel(
                item.start,
                item.duration_sec,
                item.preset,
                item.parallel_count,
                item.key,
            )
            end = item.start + wait + item.duration_sec

        alloc_events.append(
            AllocEvent(
                requested_at=item.start,
                started_at=item.start + wait,
                ended_at=end,
                wait_sec=wait,
                work_sec=item.duration_sec,
                preset=item.preset,
                parallel_count=item.parallel_count,
                is_pr_rwdi=item.is_pr_rwdi,
                is_pr_check=item.is_pr_check,
                key=item.key,
            )
        )

        window_end = max(window_end, end)
        if not item.is_pr_rwdi:
            continue

        if item.key.startswith("prepare:"):
            run_id = item.key.split(":")[1]
            pr_partial[run_id] = {"wait": wait, "start": item.start, "prepare": item.duration_sec}
            pr_runner_seconds += item.duration_sec
        elif item.key.startswith("shards:"):
            run_id = item.key.split(":")[1]
            partial = pr_partial.get(run_id, {"wait": 0.0, "start": item.start, "prepare": 0.0})
            wall = partial["prepare"] + item.duration_sec + partial["wait"] + wait
            pr_walls.append(wall)
            pr_waits.append(partial["wait"] + wait)
            pr_runner_seconds += item.parallel_count * item.duration_sec
            sharded += 1
            for run in pr_runs:
                if str(run.run_id) == run_id:
                    run.rwdi_wall_sec = wall
                    break
        elif item.is_pr_rwdi:
            pr_walls.append(item.duration_sec + wait)
            pr_waits.append(wait)
            pr_runner_seconds += item.duration_sec
            single += 1
            if item.key.isdigit():
                for run in pr_runs:
                    if str(run.rwdi_job["job_id"]) == item.key:
                        run.rwdi_wall_sec = item.duration_sec + wait
                        break

    pool.finalize(window_end)
    return ScenarioResult(
        name=name,
        pool=pool,
        pr_wall_times=pr_walls,
        pr_queue_waits=pr_waits,
        pr_sharded=sharded,
        pr_single=single,
        pr_runner_seconds=pr_runner_seconds,
        window_start=window_start,
        window_end=window_end,
        alloc_events=alloc_events,
    )


def duration_breakdown(
    pr_runs: list[PrCheckRun],
    baseline_walls: list[float],
    parallel_walls: list[float],
) -> dict[str, Any]:
    """Compare wall times for heavy PR-check runs (monolith duration >= 60 min)."""
    heavy_base: list[float] = []
    heavy_par: list[float] = []
    by_run = {run.run_id: run for run in pr_runs}
    # baseline and parallel walls are in replay order; rebuild mapping via sorted runs
    base_map = {
        run.run_id: float(run.rwdi_job["duration_sec"])
        for run in sorted(pr_runs, key=lambda r: parse_ts(r.rwdi_job["started_at"]).timestamp())
    }
    for run_id, mono in base_map.items():
        if mono < 3600:
            continue
        heavy_base.append(mono)
    # approximate parallel walls for heavy runs from pr_runs rwdi_wall_sec after replay
    for run in pr_runs:
        if float(run.rwdi_job["duration_sec"]) < 3600:
            continue
        heavy_par.append(run.rwdi_wall_sec)

    def stats(values: list[float]) -> dict[str, float]:
        if not values:
            return {"count": 0, "median_min": 0.0, "p90_min": 0.0}
        return {
            "count": len(values),
            "median_min": round(statistics.median(values) / 60.0, 1),
            "p90_min": round(percentile(values, 90) / 60.0, 1),
        }

    return {
        "heavy_pr_checks_monolith_gte_60min": {
            "baseline": stats(heavy_base),
            "parallel": stats(heavy_par),
        }
    }


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round((pct / 100.0) * (len(ordered) - 1)))
    return ordered[idx]


def summarize(result: ScenarioResult, config: PoolConfig) -> dict[str, Any]:
    return {
        "scenario": result.name,
        "pr_checks_total": len(result.pr_wall_times),
        "pr_single": result.pr_single,
        "pr_sharded": result.pr_sharded,
        "pr_rwdi_runner_hours": round(result.pr_runner_seconds / 3600.0, 1),
        "pool_runner_hours": round(result.pool.instance_seconds / 3600.0, 1),
        "peak_concurrent_runners": result.pool.peak_instances,
        "total_queue_wait_hours": round(result.pool.queue_wait_sec / 3600.0, 2),
        "queued_allocation_events": result.pool.queued_events,
        "pool_saturated_pct": round(
            100.0 * result.pool.saturated_seconds / max(result.horizon_sec, 1.0),
            1,
        ),
        "pr_rwdi_wall_median_min": round(
            statistics.median(result.pr_wall_times) / 60.0, 1
        )
        if result.pr_wall_times
        else 0.0,
        "pr_rwdi_wall_p90_min": round(percentile(result.pr_wall_times, 90) / 60.0, 1),
        "pr_queue_wait_p90_min": round(percentile(result.pr_queue_waits, 90) / 60.0, 1),
        "avg_pool_utilization": round(
            (result.pool.instance_seconds / max(result.horizon_sec, 1.0))
            / config.max_instances_budget(),
            3,
        ),
        "mean_concurrent_runners": round(
            result.pool.instance_seconds / max(result.horizon_sec, 1.0),
            1,
        ),
    }


def compare(baseline: dict[str, Any], parallel: dict[str, Any]) -> dict[str, Any]:
    return {
        "pool_runner_hours_delta": round(
            parallel["pool_runner_hours"] - baseline["pool_runner_hours"], 1
        ),
        "pool_runner_hours_delta_pct": round(
            100.0
            * (parallel["pool_runner_hours"] - baseline["pool_runner_hours"])
            / max(baseline["pool_runner_hours"], 1.0),
            1,
        ),
        "pr_rwdi_runner_hours_delta": round(
            parallel["pr_rwdi_runner_hours"] - baseline["pr_rwdi_runner_hours"], 1
        ),
        "pr_rwdi_wall_median_delta_min": round(
            parallel["pr_rwdi_wall_median_min"] - baseline["pr_rwdi_wall_median_min"],
            1,
        ),
        "pr_rwdi_wall_p90_delta_min": round(
            parallel["pr_rwdi_wall_p90_min"] - baseline["pr_rwdi_wall_p90_min"], 1
        ),
        "queue_wait_hours_delta": round(
            parallel["total_queue_wait_hours"] - baseline["total_queue_wait_hours"], 2
        ),
        "peak_runners_delta": parallel["peak_concurrent_runners"]
        - baseline["peak_concurrent_runners"],
        "saturation_pct_delta": round(
            parallel["pool_saturated_pct"] - baseline["pool_saturated_pct"], 1
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--capacity", type=Path, default=DEFAULT_CAPACITY)
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "simulation_report.json")
    parser.add_argument(
        "--skip-classify",
        action="store_true",
        help="Treat all PR-check relwithdebinfo jobs as sharded",
    )
    args = parser.parse_args()

    payload = json.loads(args.data.read_text(encoding="utf-8"))
    jobs = payload["jobs"]
    config = PoolConfig.load(args.capacity)

    pr_runs = build_pr_check_runs(jobs, classify=not args.skip_classify)
    mode_counts = {"single": 0, "sharded": 0}
    for run in pr_runs:
        mode_counts[run.mode] += 1

    baseline_items = build_work_items(jobs, pr_runs, parallel=False)
    parallel_items = build_work_items(jobs, pr_runs, parallel=True)

    baseline = replay(baseline_items, config, name="baseline", pr_runs=pr_runs, parallel=False)
    parallel = replay(parallel_items, config, name="pr_check_parallel", pr_runs=pr_runs, parallel=True)

    base_summary = summarize(baseline, config)
    par_summary = summarize(parallel, config)

    # Attach per-run shard stats for heavy-run analysis.
    for run in pr_runs:
        if run.mode == "sharded" and run.shard_count <= 1:
            run.mode = "sharded_n1"

    report = {
        "data_source": str(args.data),
        "capacity_config": str(args.capacity),
        "collection_stats": payload.get("stats", {}),
        "jobs_in_replay": len(jobs),
        "pr_check_runs": len(pr_runs),
        "pr_check_classifier": mode_counts,
        "pr_check_sharded_with_n1": sum(
            1 for run in pr_runs if run.mode == "sharded_n1"
        ),
        "pool_budget_instances": round(config.max_instances_budget(), 1),
        "heavy_run_breakdown": duration_breakdown(
            pr_runs, baseline.pr_wall_times, parallel.pr_wall_times
        ),
        "baseline": base_summary,
        "parallel": par_summary,
        "delta_parallel_vs_baseline": compare(base_summary, par_summary),
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
