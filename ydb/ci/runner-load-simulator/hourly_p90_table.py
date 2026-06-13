#!/usr/bin/env python3
"""P90 wait + duration tables by workday hour and D-group (baseline vs sharding)."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pool import PoolConfig
from pr_check_model import (
    PrCheckRun,
    build_pr_check_runs,
    estimate_shard_count,
    parse_ts,
)
from simulate import AllocEvent, build_work_items, replay

ROOT = Path(__file__).resolve().parent
DEFAULT_DATA = ROOT / "data" / "jobs_14d.json"
DEFAULT_CAPACITY = ROOT / "vendor" / "runner_capacity.yml"

D_GROUPS = (
    ("d_lt_60", "D < 60 мин"),
    ("d_60_120", "60 ≤ D < 120"),
    ("d_120_200", "120 ≤ D < 200"),
    ("d_gte_200", "D ≥ 200"),
)


def d_group(estimated_d_min: float) -> str:
    if estimated_d_min < 60:
        return "d_lt_60"
    if estimated_d_min < 120:
        return "d_60_120"
    if estimated_d_min < 200:
        return "d_120_200"
    return "d_gte_200"


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = int(round((pct / 100.0) * (len(ordered) - 1)))
    return ordered[idx]


@dataclass
class RunMetrics:
    run_id: int
    hour_utc: int
    d_key: str
    wait_min: float
    work_min: float


def run_id_from_event(e: AllocEvent) -> int | None:
    if e.key.startswith("prepare:") or e.key.startswith("shards:"):
        return int(e.key.split(":")[1])
    if e.is_pr_rwdi and e.key.isdigit():
        return None  # resolved via job_id map
    return None


def metrics_from_events(
    events: list[AllocEvent],
    pr_runs: list[PrCheckRun],
    job_id_to_run: dict[str, int],
) -> list[RunMetrics]:
    by_run: dict[int, dict[str, float]] = defaultdict(lambda: {"wait": 0.0, "work": 0.0})
    for e in events:
        if not e.is_pr_rwdi:
            continue
        rid = run_id_from_event(e)
        if rid is None and e.key.isdigit():
            rid = job_id_to_run.get(e.key)
        if rid is None:
            continue
        by_run[rid]["wait"] += e.wait_sec
        if e.key.startswith("shards:"):
            by_run[rid]["work"] += e.work_sec  # parallel wall for test phase
        elif e.key.startswith("prepare:"):
            by_run[rid]["work"] += e.work_sec
        else:
            by_run[rid]["work"] += e.work_sec

    run_by_id = {r.run_id: r for r in pr_runs}
    out: list[RunMetrics] = []
    for rid, vals in by_run.items():
        run = run_by_id.get(rid)
        if not run:
            continue
        started = parse_ts(run.rwdi_job["started_at"])
        if started.weekday() >= 5:
            continue
        _, est_d = estimate_shard_count(
            float(run.rwdi_job["duration_sec"]),
            started_at=started,
            capacity_cap=12,
        )
        out.append(
            RunMetrics(
                run_id=rid,
                hour_utc=started.astimezone(timezone.utc).hour,
                d_key=d_group(est_d),
                wait_min=vals["wait"] / 60.0,
                work_min=vals["work"] / 60.0,
            )
        )
    return out


def aggregate_p90(rows: list[RunMetrics]) -> dict[tuple[int, str], dict[str, Any]]:
    cells: dict[tuple[int, str], list[RunMetrics]] = defaultdict(list)
    for r in rows:
        cells[(r.hour_utc, r.d_key)].append(r)

    result: dict[tuple[int, str], dict[str, Any]] = {}
    for key, items in cells.items():
        waits = [x.wait_min for x in items]
        works = [x.work_min for x in items]
        result[key] = {
            "n": len(items),
            "wait_p90": percentile(waits, 90),
            "work_p90": percentile(works, 90),
            "wait_median": statistics.median(waits),
            "work_median": statistics.median(works),
        }
    return result


def fmt(v: float | None) -> str:
    return "—" if v is None else f"{v:.1f}"


def build_markdown(
    base: dict[tuple[int, str], dict[str, Any]],
    par: dict[tuple[int, str], dict[str, Any]],
) -> str:
    lines = [
        "# P90 по часам UTC (рабочие дни, агрегировано за 14 дней)",
        "",
        "Метрики на PR-check **relwithdebinfo** (симуляция пула).",
        "- **Ожидание** — суммарный queue wait до старта работы (мин)",
        "- **Выполнение** — время работы на раннере (мин); для sharding: prepare + shard phase",
        "- **D** — оценка длительности монолита (`choose_shard_count`)",
        "",
    ]

    for d_key, d_label in D_GROUPS:
        lines.append(f"## {d_label}")
        lines.append("")
        lines.append(
            "| Час UTC | n | baseline wait p90 | sharding wait p90 | "
            "Δ wait | baseline work p90 | sharding work p90 | Δ work |"
        )
        lines.append(
            "|--------:|--:|------------------:|------------------:|"
            "------:|------------------:|------------------:|-------:|"
        )
        for hour in range(24):
            key = (hour, d_key)
            b = base.get(key)
            p = par.get(key)
            if not b and not p:
                continue
            n = max((b or {}).get("n", 0), (p or {}).get("n", 0))
            bw, pw = (b or {}).get("wait_p90"), (p or {}).get("wait_p90")
            bwk, pwk = (b or {}).get("work_p90"), (p or {}).get("work_p90")
            dw = "" if bw is None or pw is None else f"{pw - bw:+.1f}"
            dwk = "" if bwk is None or pwk is None else f"{pwk - bwk:+.1f}"
            lines.append(
                f"| {hour:02d}:00 | {n} | {fmt(bw)} | {fmt(pw)} | {dw} | "
                f"{fmt(bwk)} | {fmt(pwk)} | {dwk} |"
            )
        lines.append("")

    return "\n".join(lines)


def build_summary_section(
    base_rows: list[RunMetrics],
    par_rows: list[RunMetrics],
) -> str:
    lines = [
        "## Все группы D (сводка по часу)",
        "",
        "| Час UTC | n | baseline wait p90 | sharding wait p90 | "
        "baseline work p90 | sharding work p90 |",
        "|--------:|--:|------------------:|------------------:|------------------:|------------------:|",
    ]
    bh: dict[int, list[RunMetrics]] = defaultdict(list)
    ph: dict[int, list[RunMetrics]] = defaultdict(list)
    for r in base_rows:
        bh[r.hour_utc].append(r)
    for r in par_rows:
        ph[r.hour_utc].append(r)
    for hour in range(24):
        if hour not in bh:
            continue
        n = len(bh[hour])
        lines.append(
            f"| {hour:02d}:00 | {n} | {fmt(percentile([x.wait_min for x in bh[hour]], 90))} | "
            f"{fmt(percentile([x.wait_min for x in ph.get(hour, [])], 90))} | "
            f"{fmt(percentile([x.work_min for x in bh[hour]], 90))} | "
            f"{fmt(percentile([x.work_min for x in ph.get(hour, [])], 90))} |"
        )
    return "\n".join(lines)


def build_full_markdown(
    base_rows: list[RunMetrics],
    par_rows: list[RunMetrics],
    base: dict[tuple[int, str], dict[str, Any]],
    par: dict[tuple[int, str], dict[str, Any]],
) -> str:
    return build_markdown(base, par) + "\n" + build_summary_section(base_rows, par_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--capacity", type=Path, default=DEFAULT_CAPACITY)
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "hourly_p90_table.md")
    parser.add_argument("--json-output", type=Path, default=ROOT / "data" / "hourly_p90_table.json")
    args = parser.parse_args()

    payload = json.loads(args.data.read_text(encoding="utf-8"))
    jobs = payload["jobs"]
    config = PoolConfig.load(args.capacity)
    pr_runs = build_pr_check_runs(jobs, classify=True)

    job_id_to_run: dict[str, int] = {}
    for run in pr_runs:
        job_id_to_run[str(run.rwdi_job["job_id"])] = run.run_id

    baseline = replay(
        build_work_items(jobs, pr_runs, parallel=False),
        config,
        name="baseline",
        pr_runs=pr_runs,
        parallel=False,
    )
    parallel = replay(
        build_work_items(jobs, pr_runs, parallel=True),
        config,
        name="parallel",
        pr_runs=pr_runs,
        parallel=True,
    )

    base_rows = metrics_from_events(baseline.alloc_events, pr_runs, job_id_to_run)
    par_rows = metrics_from_events(parallel.alloc_events, pr_runs, job_id_to_run)
    base_agg = aggregate_p90(base_rows)
    par_agg = aggregate_p90(par_rows)

    md = build_full_markdown(base_rows, par_rows, base_agg, par_agg)
    args.output.write_text(md, encoding="utf-8")

    json_payload = {
        "baseline": {f"{h:02d}_{d}": v for (h, d), v in base_agg.items()},
        "sharding": {f"{h:02d}_{d}": v for (h, d), v in par_agg.items()},
    }
    args.json_output.write_text(json.dumps(json_payload, indent=2), encoding="utf-8")
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
