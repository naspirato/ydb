#!/usr/bin/env python3
"""Generate presentation charts for the runner pool simulation."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from analyze_cycles import SCENARIO_ORDER, aggregate_by_scenario, analyze
from pr_check_model import build_pr_check_runs

ROOT = Path(__file__).resolve().parent
DEFAULT_DATA = ROOT / "data" / "jobs_14d.json"
DEFAULT_REPORT = ROOT / "data" / "simulation_report.json"
CHARTS_DIR = ROOT / "data" / "charts"

# Consistent palette
C_BASE = "#5B7C99"
C_PAR = "#2E9B6A"
C_ACCENT = "#E07A2F"
C_LIGHT = "#9AA5B1"


def _setup_style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "#FAFBFC",
            "axes.edgecolor": "#D0D7DE",
            "axes.labelcolor": "#24292F",
            "axes.titleweight": "bold",
            "axes.titlesize": 13,
            "axes.labelsize": 11,
            "xtick.color": "#57606A",
            "ytick.color": "#57606A",
            "font.size": 10,
            "grid.color": "#EAEEF2",
            "grid.linewidth": 0.8,
        }
    )


def chart_push_to_result_by_scenario(records: list, out: Path) -> None:
    by_sc = aggregate_by_scenario(records)
    keys = [k for k, _ in SCENARIO_ORDER if k in by_sc]
    labels = [by_sc[k]["label"].replace(" ", "\n") for k in keys]
    x = np.arange(len(keys))
    w = 0.35

    base_med = [by_sc[k]["baseline_cycle_median_min"] for k in keys]
    par_med = [by_sc[k]["parallel_cycle_median_min"] for k in keys]
    base_p90 = [by_sc[k]["baseline_cycle_p90_min"] for k in keys]
    par_p90 = [by_sc[k]["parallel_cycle_p90_min"] for k in keys]
    counts = [by_sc[k]["count"] for k in keys]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x - w / 2, base_med, w, label="Сейчас (median)", color=C_BASE, alpha=0.9)
    ax.bar(x + w / 2, par_med, w, label="PR-check parallel (median)", color=C_PAR, alpha=0.9)

    for i, k in enumerate(keys):
        ax.plot([x[i] - w / 2, x[i] - w / 2], [base_med[i], base_p90[i]], color=C_BASE, lw=2)
        ax.plot(
            [x[i] - w / 2 - 0.05, x[i] - w / 2 + 0.05],
            [base_p90[i], base_p90[i]],
            color=C_BASE,
            lw=2,
        )
        ax.plot([x[i] + w / 2, x[i] + w / 2], [par_med[i], par_p90[i]], color=C_PAR, lw=2)
        ax.plot(
            [x[i] + w / 2 - 0.05, x[i] + w / 2 + 0.05],
            [par_p90[i], par_p90[i]],
            color=C_PAR,
            lw=2,
        )
        ax.text(x[i], max(base_p90[i], par_p90[i]) + 3, f"n={counts[i]}", ha="center", fontsize=8)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylabel("Минуты (push → результат PR-check)")
    ax.set_title("Цикл push → результат по сценариям PR\n(усики = p90, столбцы = median)")
    ax.legend(loc="upper left")
    ax.grid(axis="y", alpha=0.7)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def chart_speedup_histogram(records: list, out: Path) -> None:
    sharded = [r for r in records if r.shard_count > 1]
    speedups = [r.cycle_speedup_pct for r in sharded]
    if not speedups:
        return

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(speedups, bins=20, color=C_PAR, edgecolor="white", alpha=0.85)
    med = float(np.median(speedups))
    ax.axvline(med, color=C_ACCENT, ls="--", lw=2, label=f"median = {med:.0f}%")
    ax.set_xlabel("Ускорение цикла push → результат, %")
    ax.set_ylabel("Число PR-check прогонов")
    ax.set_title(f"Распределение выигрыша (только N>1, n={len(speedups)})")
    ax.legend()
    ax.grid(axis="y", alpha=0.7)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def chart_cost_benefit(records: list, out: Path) -> None:
    sharded = [r for r in records if r.shard_count > 1]
    if not sharded:
        return
    x = [r.extra_runner_min for r in sharded]
    y = [r.cycle_saved_min for r in sharded]
    colors = [C_ACCENT if r.is_peak_utc else C_PAR for r in sharded]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(x, y, c=colors, alpha=0.55, s=40, edgecolors="white", linewidth=0.5)
    ax.axhline(0, color=C_LIGHT, lw=1)
    ax.axvline(0, color=C_LIGHT, lw=1)
    ax.set_xlabel("Доп. runner-минуты (rwdi + asan, vs монолит)")
    ax.set_ylabel("Сэкономленные минуты (push → результат)")
    ax.set_title("Цена vs выигрыш на прогон (N>1)\nоранжевый = peak hours UTC 09–16")
    peak_patch = mpatches.Patch(color=C_ACCENT, label="Peak hours")
    off_patch = mpatches.Patch(color=C_PAR, label="Off-peak")
    ax.legend(handles=[peak_patch, off_patch])
    ax.grid(alpha=0.7)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def chart_shard_distribution(records: list, out: Path) -> None:
    counts = Counter()
    for r in records:
        if r.classifier_mode == "single":
            counts["single path"] += 1
        elif r.shard_count <= 1:
            counts["sharded, N=1"] += 1
        else:
            counts[f"N={r.shard_count}"] += 1

    labels = list(counts.keys())
    values = [counts[k] for k in labels]
    colors = [C_LIGHT, C_BASE, "#4A90D9", "#2E9B6A", C_ACCENT, "#8E44AD"][: len(labels)]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.barh(labels, values, color=colors, alpha=0.9)
    for bar, val in zip(bars, values):
        ax.text(bar.get_width() + 2, bar.get_y() + bar.get_height() / 2, str(val), va="center")
    ax.set_xlabel("Число PR-check прогонов за 14 дней")
    ax.set_title("Фактическое распределение shard count\n(после classifier + choose_shard_count)")
    ax.grid(axis="x", alpha=0.7)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def chart_shard_tiers_explainer(out: Path) -> None:
    """Why N=1/4/8/12 — visual explainer from choose_shard_count.py."""
    fig, ax = plt.subplots(figsize=(11, 4))
    ax.set_xlim(0, 240)
    ax.set_ylim(0, 1)
    ax.axis("off")

    tiers = [
        (0, 60, "N = 1", "Шардинг не окупается\n(classifier может уйти в single)"),
        (60, 120, "N = 4", "Средняя длительность\nполного прогона"),
        (120, 200, "N = 8", "Тяжёлый PR"),
        (200, 240, "N = 12", "Очень тяжёлый PR"),
    ]
    colors = [C_LIGHT, "#4A90D9", C_PAR, C_ACCENT]
    for (lo, hi, n_label, desc), color in zip(tiers, colors):
        rect = mpatches.FancyBboxPatch(
            (lo, 0.2),
            hi - lo,
            0.6,
            boxstyle="round,pad=0.02",
            facecolor=color,
            edgecolor="white",
            alpha=0.85,
        )
        ax.add_patch(rect)
        ax.text((lo + hi) / 2, 0.65, n_label, ha="center", va="center", fontsize=14, fontweight="bold")
        ax.text((lo + hi) / 2, 0.35, desc, ha="center", va="center", fontsize=9)

    ax.text(
        120,
        0.05,
        "D = оценка длительности монолита (мин)  |  Peak 09–16 UTC: cap N≤4  |  Pool cap: estimate_runner_capacity",
        ha="center",
        fontsize=9,
        color="#57606A",
    )
    ax.set_title("Почему именно такие N? (choose_shard_count.py, PR #43351)")
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def chart_classifier_funnel(records: list, out: Path) -> None:
    single = sum(1 for r in records if r.classifier_mode == "single")
    sharded = len(records) - single
    n1 = sum(1 for r in records if r.classifier_mode != "single" and r.shard_count <= 1)
    n_gt1 = sum(1 for r in records if r.shard_count > 1)

    stages = [
        f"Все PR-check\nn={len(records)}",
        f"Classifier\nsingle: {single}",
        f"Classifier\nsharded: {sharded}",
        f"Adaptive N=1\n{n1}",
        f"Реальный шардинг\nN>1: {n_gt1}",
    ]
    values = [len(records), single, sharded, n1, n_gt1]

    fig, ax = plt.subplots(figsize=(10, 5))
    y = np.arange(len(stages))
    ax.barh(y, values, color=[C_BASE, C_LIGHT, C_BASE, "#4A90D9", C_PAR], alpha=0.9)
    for i, v in enumerate(values):
        ax.text(v + 3, i, str(v), va="center")
    ax.set_yticks(y)
    ax.set_yticklabels(stages)
    ax.invert_yaxis()
    ax.set_xlabel("Прогонов за 14 дней")
    ax.set_title("Воронка: кто реально получает шардинг")
    ax.grid(axis="x", alpha=0.7)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def chart_pool_utilization(jobs: list, out: Path) -> None:
    """Hourly mean concurrent runners from historical job intervals."""
    from datetime import datetime

    def parse_ts(v: str) -> float:
        return datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp()

    if not jobs:
        return
    t0 = min(parse_ts(j["started_at"]) for j in jobs)
    t1 = max(parse_ts(j["completed_at"]) for j in jobs)
    bucket = 3600.0
    nb = int((t1 - t0) / bucket) + 1
    counts = np.zeros(nb)

    for job in jobs:
        s = int((parse_ts(job["started_at"]) - t0) / bucket)
        e = int((parse_ts(job["completed_at"]) - t0) / bucket)
        for b in range(s, min(e + 1, nb)):
            counts[b] += 1

    hours = np.arange(nb) / 24.0
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.fill_between(hours, counts, alpha=0.4, color=C_BASE)
    ax.plot(hours, counts, color=C_BASE, lw=1)
    ax.axhline(79.2, color=C_ACCENT, ls="--", label="Бюджет пула (~79 VM)")
    ax.set_xlabel("Дни от начала окна (14 дней)")
    ax.set_ylabel("Concurrent auto-provisioned jobs (почасовой снимок)")
    ax.set_title("Фактическая загрузка пула за 14 дней (baseline, все workflow)")
    ax.legend()
    ax.grid(alpha=0.7)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def chart_waterfall_heavy(records: list, out: Path) -> None:
    heavy = [r for r in records if r.shard_count > 1]
    if not heavy:
        return
    # Pick representative: median baseline cycle among N>1
    heavy.sort(key=lambda r: r.baseline_cycle_min)
    r = heavy[len(heavy) // 2]

    fig, ax = plt.subplots(figsize=(10, 5))
    stages = ["Монолит\nrwdi", "Prepare\n(list+build)", f"Shards ×{r.shard_count}", "Итого\nparallel"]
    mono = r.mono_rwdi_min
    prepare = r.prepare_min
    shard = r.shard_phase_min
    total = r.parallel_rwdi_min

    ax.bar(0, mono, color=C_BASE, width=0.6, label="baseline")
    ax.bar(1, prepare, color="#4A90D9", width=0.6)
    ax.bar(2, shard, color=C_PAR, width=0.6)
    ax.bar(3, total, color=C_ACCENT, width=0.6)

    ax.text(0, mono + 2, f"{mono:.0f} мин", ha="center")
    ax.text(1, prepare + 2, f"{prepare:.0f} мин", ha="center")
    ax.text(2, shard + 2, f"{shard:.0f} мин\n(параллельно)", ha="center")
    ax.text(3, total + 2, f"{total:.0f} мин\n(−{mono-total:.0f} мин)", ha="center", fontweight="bold")

    ax.set_xticks([0, 1, 2, 3])
    ax.set_xticklabels(stages)
    ax.set_ylabel("Минуты (только relwithdebinfo)")
    ax.set_title(
        f"Разбор типичного тяжёлого PR (run {r.run_id}, D≈{r.estimated_d_min:.0f} мин → N={r.shard_count})"
    )
    ax.grid(axis="y", alpha=0.7)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def chart_summary_kpi(report: dict, records: list, out: Path) -> None:
    delta = report["delta_parallel_vs_baseline"]
    by_sc = aggregate_by_scenario(records)
    heavy = by_sc.get("shard_n8") or by_sc.get("shard_n4") or {}

    fig, axes = plt.subplots(2, 2, figsize=(11, 8))
    fig.suptitle("PR-check parallel vs baseline — KPI за 14 дней", fontsize=14, fontweight="bold")

    # Panel 1: cycle time
    ax = axes[0, 0]
    metrics = ["median", "p90"]
    base = [
        report["baseline"]["pr_rwdi_wall_median_min"],
        report["baseline"]["pr_rwdi_wall_p90_min"],
    ]
    par = [
        report["parallel"]["pr_rwdi_wall_median_min"],
        report["parallel"]["pr_rwdi_wall_p90_min"],
    ]
    x = np.arange(2)
    ax.bar(x - 0.2, base, 0.4, label="Сейчас", color=C_BASE)
    ax.bar(x + 0.2, par, 0.4, label="Parallel", color=C_PAR)
    ax.set_xticks(x)
    ax.set_xticklabels(["rwdi median", "rwdi p90"])
    ax.set_ylabel("Минуты")
    ax.set_title("Время rwdi job")
    ax.legend()

    # Panel 2: pool cost
    ax = axes[0, 1]
    ax.bar(["Пул runner-h", "Пик runners", "Очередь h"], 
           [delta["pool_runner_hours_delta_pct"], 
            delta["peak_runners_delta"] * 10,  # scale for visibility
            delta["queue_wait_hours_delta"]],
           color=[C_ACCENT, C_BASE, C_PAR])
    ax.set_title(f"Цена: +{delta['pool_runner_hours_delta_pct']}% runner-h, пик +{delta['peak_runners_delta']}")
    ax.set_ylabel("Δ (очередь в часах, пик ×10)")

    # Panel 3: heavy PR cycle
    ax = axes[1, 0]
    if heavy:
        ax.bar(["median", "p90"], 
               [heavy.get("median_saved_min", 0), 
                heavy.get("baseline_cycle_p90_min", 0) - heavy.get("parallel_cycle_p90_min", 0)],
               color=C_PAR)
        ax.set_title(f"Тяжёлые PR: median saved {heavy.get('median_saved_min', 0):.0f} мин")
    ax.set_ylabel("Минуты saved")

    # Panel 4: funnel text
    ax = axes[1, 1]
    ax.axis("off")
    txt = (
        f"PR-check прогонов: {report['pr_check_runs']}\n"
        f"Classifier single: {report['pr_check_classifier']['single']}\n"
        f"Sharded path: {report['pr_check_classifier']['sharded']}\n"
        f"  → adaptive N=1: {report['pr_check_sharded_with_n1']}\n"
        f"  → реальный N>1: {report['parallel']['pr_sharded']}\n\n"
        f"Push→result p90: {delta['pr_rwdi_wall_p90_delta_min']:+.0f} мин\n"
        f"Пул: {delta['pool_runner_hours_delta_pct']:+.1f}% runner-hours"
    )
    ax.text(0.1, 0.5, txt, fontsize=11, va="center", family="monospace")

    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def write_team_report(report: dict, records: list, charts_dir: Path, out: Path) -> None:
    by_sc = aggregate_by_scenario(records)
    delta = report["delta_parallel_vs_baseline"]

    lines = [
        "# PR-check parallel: отчёт для команды",
        "",
        "Данные: 14 дней production GitHub Actions, 9336 auto-provisioned jobs.",
        "",
        "## Главный вывод",
        "",
        "**Выигрыш есть на тяжёлых PR, цена умеренная.**",
        "",
        f"- **p90 rwdi**: {report['baseline']['pr_rwdi_wall_p90_min']:.0f} → "
        f"{report['parallel']['pr_rwdi_wall_p90_min']:.0f} мин "
        f"(**{delta['pr_rwdi_wall_p90_delta_min']:+.0f} мин**)",
        f"- **Медиана rwdi**: {delta['pr_rwdi_wall_median_delta_min']:+.1f} мин (лёгкие PR не страдают)",
        f"- **Цена для пула**: +{delta['pool_runner_hours_delta_pct']}% runner-hours, "
        f"пик +{delta['peak_runners_delta']} VM",
        "",
        "## Push → результат по сценариям",
        "",
        "| Сценарий | n | Сейчас median | Parallel median | Δ median | Δ p90 |",
        "|----------|---|---------------|-----------------|----------|-------|",
    ]
    for key, _ in SCENARIO_ORDER:
        if key not in by_sc:
            continue
        s = by_sc[key]
        saved_med = s["baseline_cycle_median_min"] - s["parallel_cycle_median_min"]
        saved_p90 = s["baseline_cycle_p90_min"] - s["parallel_cycle_p90_min"]
        med_str = f"−{saved_med:.0f} мин" if saved_med > 0 else (f"+{-saved_med:.0f} мин" if saved_med < 0 else "0")
        p90_str = f"−{saved_p90:.0f} мин" if saved_p90 > 0 else (f"+{-saved_p90:.0f} мин" if saved_p90 < 0 else "0")
        lines.append(
            f"| {s['label']} | {s['count']} | {s['baseline_cycle_median_min']:.0f} мин | "
            f"{s['parallel_cycle_median_min']:.0f} мин | **{med_str}** | {p90_str} |"
        )

    lines.extend(
        [
            "",
            "## Почему именно N=1/4/8/12?",
            "",
            "См. график `05_shard_tiers_explainer.png`:",
            "",
            "- **D < 60 мин** → N=1 (overhead шардинга не окупается)",
            "- **60–120 мин** → N=4",
            "- **120–200 мин** → N=8",
            "- **200+ мин** → N=12",
            "- **09–16 UTC**: cap N≤4 (защита shared pool)",
            "- **estimate_runner_capacity**: доп. cap по свободным vCPU/RAM/VM",
            "",
            "Classifier до этого решает **single vs sharded path** по путям в diff.",
            "",
            "## Графики",
            "",
        ]
    )
    for png in sorted(charts_dir.glob("*.png")):
        lines.append(f"![{png.stem}](charts/{png.name})")
        lines.append("")

    lines.extend(
        [
            "## FAQ",
            "",
            "**Почему +6% runner-hours?** Шардинг берёт N раннеров одновременно на фазе тестов. "
            "Зато wall-clock падает — разработчик ждёт меньше.",
            "",
            "**Почему медиана почти не меняется?** 139 PR идут в single path, ещё 156 — sharded но N=1. "
            "Шардинг затрагивает только ~122 прогона с N>1.",
            "",
            "**Что такое push→result?** max(relwithdebinfo, release-asan) от старта первого job "
            "до завершения последнего — как видит разработчик.",
            "",
        ]
    )
    out.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--charts-dir", type=Path, default=CHARTS_DIR)
    args = parser.parse_args()

    _setup_style()
    args.charts_dir.mkdir(parents=True, exist_ok=True)

    payload = json.loads(args.data.read_text(encoding="utf-8"))
    jobs = payload["jobs"]
    pr_runs = build_pr_check_runs(jobs, classify=True)
    records = analyze(jobs, pr_runs)

    report = json.loads(args.report.read_text(encoding="utf-8"))

    chart_push_to_result_by_scenario(records, args.charts_dir / "01_push_to_result_by_scenario.png")
    chart_speedup_histogram(records, args.charts_dir / "02_speedup_distribution.png")
    chart_cost_benefit(records, args.charts_dir / "03_cost_benefit.png")
    chart_shard_distribution(records, args.charts_dir / "04_shard_count_distribution.png")
    chart_shard_tiers_explainer(args.charts_dir / "05_shard_tiers_explainer.png")
    chart_classifier_funnel(records, args.charts_dir / "06_classifier_funnel.png")
    chart_pool_utilization(jobs, args.charts_dir / "07_pool_utilization_timeline.png")
    chart_waterfall_heavy(records, args.charts_dir / "08_waterfall_heavy_pr.png")
    chart_summary_kpi(report, records, args.charts_dir / "09_summary_dashboard.png")

    write_team_report(report, records, args.charts_dir, ROOT / "data" / "TEAM_REPORT.md")

    print(f"Generated {len(list(args.charts_dir.glob('*.png')))} charts in {args.charts_dir}")
    print(f"Team report: {ROOT / 'data' / 'TEAM_REPORT.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
