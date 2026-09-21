#!/usr/bin/env python3
"""10-minute workday queue/duration profiles: historical vs baseline vs sharding."""

from __future__ import annotations

import argparse
import json
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np

from pool import PoolConfig
from pr_check_model import build_pr_check_runs, parse_ts
from simulate import (
    AllocEvent,
    build_work_items,
    is_pr_check_rwdi,
    replay,
)

ROOT = Path(__file__).resolve().parent
DEFAULT_DATA = ROOT / "data" / "jobs_14d.json"
DEFAULT_CAPACITY = ROOT / "vendor" / "runner_capacity.yml"
CHARTS_DIR = ROOT / "data" / "charts"
SLOT_MIN = 10
SLOTS_PER_DAY = 24 * 60 // SLOT_MIN

C_HIST = "#8B5CF6"
C_BASE = "#5B7C99"
C_PAR = "#2E9B6A"
C_ACCENT = "#E07A2F"


@dataclass
class SlotStats:
    slot: int
    label: str
    count: int
    queue_median_min: float
    queue_p90_min: float
    queue_max_min: float
    duration_median_min: float
    duration_p90_min: float
    queue_depth_mean: float
    queue_depth_p90: float


def _setup_style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "#FAFBFC",
            "axes.edgecolor": "#D0D7DE",
            "font.size": 10,
            "grid.color": "#EAEEF2",
        }
    )


def slot_label(slot: int) -> str:
    total_min = slot * SLOT_MIN
    h, m = divmod(total_min, 60)
    return f"{h:02d}:{m:02d}"


def is_workday_epoch(epoch: float) -> bool:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).weekday() < 5


def time_slot(epoch: float) -> int | None:
    if not is_workday_epoch(epoch):
        return None
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return (dt.hour * 60 + dt.minute) // SLOT_MIN


def day_start_utc(epoch: float) -> datetime:
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round((pct / 100.0) * (len(ordered) - 1)))
    return ordered[idx]


def entry_events(events: list[AllocEvent]) -> list[AllocEvent]:
    """First runner touch per PR-check rwdi (exclude parallel shard batch)."""
    return [e for e in events if e.is_pr_rwdi and not e.key.startswith("shards:")]


def aggregate_events(events: list[AllocEvent], *, pr_rwdi_only: bool) -> dict[int, SlotStats]:
    """Bucket simulated allocations by workday time-of-day (10 min)."""
    if pr_rwdi_only:
        events = entry_events(events)
    queues: dict[int, list[float]] = {s: [] for s in range(SLOTS_PER_DAY)}
    durations: dict[int, list[float]] = {s: [] for s in range(SLOTS_PER_DAY)}
    depth_samples: dict[int, list[float]] = {s: [] for s in range(SLOTS_PER_DAY)}

    filtered = [
        e
        for e in events
        if (not pr_rwdi_only or e.is_pr_rwdi)
        and is_workday_epoch(e.requested_at)
    ]
    if not filtered:
        return {}

    # Queue wait + duration by slot of request time.
    for e in filtered:
        slot = time_slot(e.requested_at)
        if slot is None:
            continue
        queues[slot].append(e.wait_sec / 60.0)
        wall = e.wait_sec + e.work_sec
        durations[slot].append(wall / 60.0)

    # Queue depth: sample each workday at each slot boundary.
    t0 = day_start_utc(min(e.requested_at for e in filtered))
    t1 = day_start_utc(max(e.ended_at for e in filtered)) + timedelta(days=1)
    day = t0
    while day < t1:
        if day.weekday() < 5:
            for slot in range(SLOTS_PER_DAY):
                t = day.timestamp() + slot * SLOT_MIN * 60
                depth = sum(
                    1
                    for e in filtered
                    if e.requested_at <= t < e.started_at
                )
                depth_samples[slot].append(float(depth))
        day += timedelta(days=1)

    result: dict[int, SlotStats] = {}
    for slot in range(SLOTS_PER_DAY):
        q = queues[slot]
        d = durations[slot]
        dep = depth_samples[slot]
        if not q:
            continue
        result[slot] = SlotStats(
            slot=slot,
            label=slot_label(slot),
            count=len(q),
            queue_median_min=round(statistics.median(q), 1),
            queue_p90_min=round(percentile(q, 90), 1),
            queue_max_min=round(max(q), 1),
            duration_median_min=round(statistics.median(d), 1) if d else 0.0,
            duration_p90_min=round(percentile(d, 90), 1) if d else 0.0,
            queue_depth_mean=round(statistics.mean(dep), 2) if dep else 0.0,
            queue_depth_p90=round(percentile(dep, 90), 1) if dep else 0.0,
        )
    return result


def aggregate_historical(jobs: list[dict[str, Any]]) -> dict[int, SlotStats]:
    """Observed GitHub queue (created_at → started_at) for PR-check rwdi."""
    queues: dict[int, list[float]] = {s: [] for s in range(SLOTS_PER_DAY)}
    durations: dict[int, list[float]] = {s: [] for s in range(SLOTS_PER_DAY)}
    depth_samples: dict[int, list[float]] = {s: [] for s in range(SLOTS_PER_DAY)}

    rows = [
        j
        for j in jobs
        if is_pr_check_rwdi(j) and j.get("queue_wait_sec") is not None and j.get("created_at")
    ]
    if not rows:
        return {}

    for j in rows:
        created = parse_ts(j["created_at"]).timestamp()
        started = parse_ts(j["started_at"]).timestamp()
        slot = time_slot(created)
        if slot is None:
            continue
        queues[slot].append(float(j["queue_wait_sec"]) / 60.0)
        durations[slot].append(float(j["duration_sec"]) / 60.0)

    t0 = day_start_utc(min(parse_ts(j["created_at"]).timestamp() for j in rows))
    t1 = day_start_utc(max(parse_ts(j["completed_at"]).timestamp() for j in rows)) + timedelta(
        days=1
    )
    day = t0
    while day < t1:
        if day.weekday() < 5:
            for slot in range(SLOTS_PER_DAY):
                t = day.timestamp() + slot * SLOT_MIN * 60
                depth = 0
                for j in rows:
                    c = parse_ts(j["created_at"]).timestamp()
                    s = parse_ts(j["started_at"]).timestamp()
                    if c <= t < s:
                        depth += 1
                depth_samples[slot].append(float(depth))
        day += timedelta(days=1)

    result: dict[int, SlotStats] = {}
    for slot in range(SLOTS_PER_DAY):
        q = queues[slot]
        if not q:
            continue
        d = durations[slot]
        dep = depth_samples[slot]
        result[slot] = SlotStats(
            slot=slot,
            label=slot_label(slot),
            count=len(q),
            queue_median_min=round(statistics.median(q), 1),
            queue_p90_min=round(percentile(q, 90), 1),
            queue_max_min=round(max(q), 1),
            duration_median_min=round(statistics.median(d), 1),
            duration_p90_min=round(percentile(d, 90), 1),
            queue_depth_mean=round(statistics.mean(dep), 2),
            queue_depth_p90=round(percentile(dep, 90), 1),
        )
    return result


def _series(stats: dict[int, SlotStats], attr: str) -> tuple[np.ndarray, np.ndarray]:
    slots = sorted(stats)
    x = np.array(slots)
    y = np.array([getattr(stats[s], attr) for s in slots], dtype=float)
    return x, y


def _slot_to_hours(slots: np.ndarray) -> np.ndarray:
    return slots * SLOT_MIN / 60.0


def chart_workday_queue_profile(
    hist: dict[int, SlotStats],
    base: dict[int, SlotStats],
    par: dict[int, SlotStats],
    out: Path,
) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(14, 9), sharex=True)

    for ax, attr, title in [
        (axes[0], "queue_p90_min", "Очередь p90 (мин) — PR-check relwithdebinfo, рабочие дни"),
        (axes[1], "queue_median_min", "Очередь median (мин)"),
    ]:
        if hist:
            x, y = _series(hist, attr)
            ax.plot(_slot_to_hours(x), y, color=C_HIST, lw=1.8, label="Факт (GitHub created→started)", alpha=0.9)
            ax.fill_between(_slot_to_hours(x), 0, y, color=C_HIST, alpha=0.08)
        if base:
            x, y = _series(base, attr)
            ax.plot(_slot_to_hours(x), y, color=C_BASE, lw=1.8, label="Симуляция: baseline")
        if par:
            x, y = _series(par, attr)
            ax.plot(_slot_to_hours(x), y, color=C_PAR, lw=1.8, label="Симуляция: sharding")
        ax.axvspan(9, 16, color=C_ACCENT, alpha=0.06, label="Peak cap 09–16 UTC")
        ax.set_ylabel("Минуты")
        ax.set_title(title)
        ax.grid(alpha=0.7)
        ax.legend(loc="upper left", fontsize=8)

    axes[1].set_xlabel("Время суток UTC (слот 10 мин, усреднено по пн–пт за 14 дней)")
    axes[0].set_xlim(0, 24)
    fig.suptitle(
        "Профиль очереди PR-check по времени суток (детализация 10 мин)",
        fontsize=13,
        fontweight="bold",
    )
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def chart_workday_duration_profile(
    hist: dict[int, SlotStats],
    base: dict[int, SlotStats],
    par: dict[int, SlotStats],
    out: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(14, 5))
    if hist:
        x, y = _series(hist, "duration_p90_min")
        ax.plot(_slot_to_hours(x), y, color=C_HIST, lw=1.8, label="Факт p90 duration")
    if base:
        x, y = _series(base, "duration_p90_min")
        ax.plot(_slot_to_hours(x), y, color=C_BASE, lw=1.8, label="Baseline sim p90")
    if par:
        x, y = _series(par, "duration_p90_min")
        ax.plot(_slot_to_hours(x), y, color=C_PAR, lw=1.8, label="Sharding sim p90")
    ax.axvspan(9, 16, color=C_ACCENT, alpha=0.06)
    ax.set_xlabel("Время суток UTC (10-мин слоты, пн–пт)")
    ax.set_ylabel("Минуты (wait + work)")
    ax.set_title("Длительность PR-check rwdi p90 по времени суток")
    ax.set_xlim(0, 24)
    ax.legend()
    ax.grid(alpha=0.7)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def chart_queue_depth(
    base: dict[int, SlotStats],
    par: dict[int, SlotStats],
    out: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(14, 5))
    if base:
        x, y = _series(base, "queue_depth_p90")
        ax.plot(_slot_to_hours(x), y, color=C_BASE, lw=1.8, label="Baseline: p90 глубина очереди")
    if par:
        x, y = _series(par, "queue_depth_p90")
        ax.plot(_slot_to_hours(x), y, color=C_PAR, lw=1.8, label="Sharding: p90 глубина очереди")
    ax.axvspan(9, 16, color=C_ACCENT, alpha=0.06, label="09–16 UTC")
    ax.set_xlabel("Время суток UTC")
    ax.set_ylabel("PR-check rwdi jobs в очереди (p90)")
    ax.set_title("Глубина очереди PR-check (симуляция пула, 10-мин слоты)")
    ax.set_xlim(0, 24)
    ax.legend()
    ax.grid(alpha=0.7)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def find_peak_workdays(events: list[AllocEvent], top_n: int = 3) -> list[datetime]:
    """Workdays with highest total simulated PR-check queue wait."""
    per_day: dict[str, float] = {}
    for e in events:
        if not e.is_pr_rwdi or not is_workday_epoch(e.requested_at):
            continue
        d = day_start_utc(e.requested_at)
        key = d.strftime("%Y-%m-%d")
        per_day[key] = per_day.get(key, 0.0) + e.wait_sec
    ranked = sorted(per_day.items(), key=lambda x: x[1], reverse=True)
    return [datetime.fromisoformat(k).replace(tzinfo=timezone.utc) for k, _ in ranked[:top_n]]


def chart_peak_day_timeline(
    base_events: list[AllocEvent],
    par_events: list[AllocEvent],
    hist_jobs: list[dict[str, Any]],
    peak_day: datetime,
    out: Path,
) -> None:
    """10-minute timeline for one peak workday."""
    day_end = peak_day + timedelta(days=1)
    t0 = peak_day.timestamp()
    slots = list(range(0, 24 * 60 // SLOT_MIN))

    def bucket_events(events: list[AllocEvent]) -> tuple[list[float], list[float]]:
        q_vals = []
        d_vals = []
        for slot in slots:
            t_start = t0 + slot * SLOT_MIN * 60
            t_end = t_start + SLOT_MIN * 60
            waits = [
                e.wait_sec / 60.0
                for e in events
                if e.is_pr_rwdi and t_start <= e.requested_at < t_end
            ]
            walls = [
                (e.wait_sec + e.work_sec) / 60.0
                for e in events
                if e.is_pr_rwdi and t_start <= e.requested_at < t_end
            ]
            q_vals.append(statistics.median(waits) if waits else 0.0)
            d_vals.append(statistics.median(walls) if walls else 0.0)
        return q_vals, d_vals

    def bucket_hist() -> list[float]:
        q_vals = []
        for slot in slots:
            t_start = t0 + slot * SLOT_MIN * 60
            t_end = t_start + SLOT_MIN * 60
            waits = []
            for j in hist_jobs:
                if not is_pr_check_rwdi(j) or not j.get("created_at"):
                    continue
                c = parse_ts(j["created_at"]).timestamp()
                if t_start <= c < t_end:
                    waits.append(float(j.get("queue_wait_sec") or 0) / 60.0)
            q_vals.append(statistics.median(waits) if waits else 0.0)
        return q_vals

    bq, bd = bucket_events(base_events)
    pq, pd = bucket_events(par_events)
    hq = bucket_hist()
    times = [peak_day + timedelta(minutes=s * SLOT_MIN) for s in slots]

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    axes[0].bar(times, hq, width=0.006, color=C_HIST, alpha=0.5, label="Факт median queue")
    axes[0].plot(times, bq, color=C_BASE, lw=2, marker="o", ms=3, label="Baseline sim")
    axes[0].plot(times, pq, color=C_PAR, lw=2, marker="o", ms=3, label="Sharding sim")
    axes[0].set_ylabel("Очередь median (мин)")
    axes[0].set_title(f"Пиковый день {peak_day.date()} — очередь PR-check rwdi (10 мин)")
    axes[0].legend()
    axes[0].grid(alpha=0.7)

    axes[1].plot(times, bd, color=C_BASE, lw=2, label="Baseline duration median")
    axes[1].plot(times, pd, color=C_PAR, lw=2, label="Sharding duration median")
    axes[1].set_ylabel("Длительность median (мин)")
    axes[1].legend()
    axes[1].grid(alpha=0.7)
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def chart_queue_delta_heatmap(
    base: dict[int, SlotStats],
    par: dict[int, SlotStats],
    out: Path,
) -> None:
    """Sharding − baseline queue p90 per 10-min slot (negative = improvement)."""
    slots = sorted(set(base) & set(par))
    if not slots:
        return
    delta = [par[s].queue_p90_min - base[s].queue_p90_min for s in slots]
    hours = _slot_to_hours(np.array(slots))

    fig, ax = plt.subplots(figsize=(14, 3.5))
    colors = [C_PAR if d < 0 else C_ACCENT for d in delta]
    ax.bar(hours, delta, width=10 / 60 * 0.8, color=colors, alpha=0.85)
    ax.axhline(0, color="#57606A", lw=1)
    ax.set_xlabel("Время суток UTC")
    ax.set_ylabel("Δ queue p90 (мин)\nsharding − baseline")
    ax.set_title("Изменение очереди от шардинга по 10-мин слотам (зелёный = меньше очередь)")
    ax.set_xlim(0, 24)
    ax.grid(axis="y", alpha=0.7)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def peak_hour_summary(
    hist: dict[int, SlotStats],
    base: dict[int, SlotStats],
    par: dict[int, SlotStats],
) -> dict[str, Any]:
    """Aggregate 09–16 UTC slots (54..96)."""
    slots = range(54, 97)

    def agg(stats: dict[int, SlotStats]) -> dict[str, float]:
        qp90 = [stats[s].queue_p90_min for s in slots if s in stats]
        qmed = [stats[s].queue_median_min for s in slots if s in stats]
        cnt = sum(stats[s].count for s in slots if s in stats)
        if not qp90:
            return {"jobs": 0}
        return {
            "jobs": cnt,
            "queue_p90_median_min": round(statistics.median(qp90), 1),
            "queue_p90_max_min": round(max(qp90), 1),
            "queue_median_avg_min": round(statistics.mean(qmed), 1),
        }

    return {
        "window": "09:00–16:00 UTC",
        "historical_github": agg(hist),
        "sim_baseline": agg(base),
        "sim_sharding": agg(par),
    }


def write_queue_report(summary: dict[str, Any], out: Path) -> None:
    h, b, p = summary["historical_github"], summary["sim_baseline"], summary["sim_sharding"]
    lines = [
        "# Очереди PR-check: 10-минутный профиль (рабочие дни)",
        "",
        "Два источника данных:",
        "- **Факт (фиолетовый)** — `created_at → started_at` из GitHub API (реальное ожидание раннера)",
        "- **Симуляция (синий/зелёный)** — replay всего пула (PR-check + postcommit + nightly + …) "
        "с лимитами `runner_capacity.yml`",
        "",
        f"## Пиковое окно {summary['window']}",
        "",
        "| Источник | PR-check rwdi jobs | p90 очереди (median по слотам) | max p90 слота | avg median |",
        "|----------|-------------------:|-------------------------------:|--------------:|-----------:|",
        f"| Факт GitHub | {h.get('jobs',0)} | {h.get('queue_p90_median_min','—')} мин | "
        f"{h.get('queue_p90_max_min','—')} мин | {h.get('queue_median_avg_min','—')} мин |",
        f"| Симуляция baseline | {b.get('jobs',0)} | {b.get('queue_p90_median_min','—')} мин | "
        f"{b.get('queue_p90_max_min','—')} мин | {b.get('queue_median_avg_min','—')} мин |",
        f"| Симуляция sharding | {p.get('jobs',0)} | {p.get('queue_p90_median_min','—')} мин | "
        f"{p.get('queue_p90_max_min','—')} мин | {p.get('queue_median_avg_min','—')} мин |",
        "",
        "Метрика очереди на графиках 10/11 — **вход в PR-check rwdi** (первый запрос раннера, "
        "без отдельного учёта shard-матрицы).",
        "",
        "## Графики",
        "",
        "| Файл | Содержание |",
        "|------|------------|",
        "| `10_workday_queue_10min.png` | Очередь p90/median по 10-мин слотам (пн–пт) |",
        "| `11_workday_duration_10min.png` | Длительность p90 по слотам |",
        "| `12_queue_depth_10min.png` | Глубина очереди (сколько PR-check ждут одновременно) |",
        "| `13_queue_delta_heatmap.png` | Δ очереди sharding − baseline по слотам |",
        "| `14_peak_day_*.png` | Детализация 10 мин на самых загруженных днях |",
        "",
        "## Интерпретация для команды",
        "",
        "1. **Симуляция завышает дневную очередь** относительно факта — модель не учитывает "
        "прогрев пула / быстрый provisioning. Абсолютные минуты берите из **факта**, тренды — из **симуляции**.",
        "2. **В пик 09–16 UTC шардинг может ухудшить очередь** на входе: N раннеров на один PR "
        "конкурируют с другими workflow. Peak cap N≤4 и `estimate_runner_capacity` как раз для этого.",
        "3. **Выигрыш шардинга — в duration**, не в очереди на входе: тяжёлый PR освобождает раннер "
        "раньше (короче wall-clock), что косвенно разгружает пул позже.",
        "4. Зелёные слоты на `13_queue_delta_heatmap.png` — окна, где шардинг снижает p90 очереди.",
        "",
    ]
    out.write_text("\n".join(lines), encoding="utf-8")


def export_json(
    hist: dict[int, SlotStats],
    base: dict[int, SlotStats],
    par: dict[int, SlotStats],
    out: Path,
) -> dict[str, Any]:
    peak_summary = peak_hour_summary(hist, base, par)
    payload = {
        "slot_minutes": SLOT_MIN,
        "workdays_only": True,
        "timezone": "UTC",
        "peak_window_09_16_utc": peak_summary,
        "historical_github": {str(k): asdict(v) for k, v in sorted(hist.items())},
        "sim_baseline": {str(k): asdict(v) for k, v in sorted(base.items())},
        "sim_sharding": {str(k): asdict(v) for k, v in sorted(par.items())},
        "peak_slots": {
            "historical_top5_queue_p90": [
                asdict(s)
                for s in sorted(hist.values(), key=lambda s: s.queue_p90_min, reverse=True)[:5]
            ],
            "baseline_top5_queue_p90": [
                asdict(s)
                for s in sorted(base.values(), key=lambda s: s.queue_p90_min, reverse=True)[:5]
            ],
        },
    }
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return peak_summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--capacity", type=Path, default=DEFAULT_CAPACITY)
    parser.add_argument("--charts-dir", type=Path, default=CHARTS_DIR)
    parser.add_argument("--skip-classify", action="store_true")
    args = parser.parse_args()

    _setup_style()
    args.charts_dir.mkdir(parents=True, exist_ok=True)

    payload = json.loads(args.data.read_text(encoding="utf-8"))
    jobs = payload["jobs"]
    config = PoolConfig.load(args.capacity)
    pr_runs = build_pr_check_runs(jobs, classify=not args.skip_classify)

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

    hist = aggregate_historical(jobs)
    base_slots = aggregate_events(baseline.alloc_events, pr_rwdi_only=True)
    par_slots = aggregate_events(parallel.alloc_events, pr_rwdi_only=True)

    chart_workday_queue_profile(
        hist, base_slots, par_slots, args.charts_dir / "10_workday_queue_10min.png"
    )
    chart_workday_duration_profile(
        hist, base_slots, par_slots, args.charts_dir / "11_workday_duration_10min.png"
    )
    chart_queue_depth(base_slots, par_slots, args.charts_dir / "12_queue_depth_10min.png")
    chart_queue_delta_heatmap(
        base_slots, par_slots, args.charts_dir / "13_queue_delta_heatmap.png"
    )

    peak_days = find_peak_workdays(baseline.alloc_events, top_n=3)
    for idx, day in enumerate(peak_days, start=1):
        chart_peak_day_timeline(
            baseline.alloc_events,
            parallel.alloc_events,
            jobs,
            day,
            args.charts_dir / f"14_peak_day_{idx}_{day.date()}.png",
        )

    peak_summary = export_json(hist, base_slots, par_slots, ROOT / "data" / "queue_timeline_10min.json")
    write_queue_report(peak_summary, ROOT / "data" / "QUEUE_REPORT.md")

    print(f"Charts → {args.charts_dir}")
    print(f"Data  → {ROOT / 'data' / 'queue_timeline_10min.json'}")
    print(f"Report→ {ROOT / 'data' / 'QUEUE_REPORT.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
