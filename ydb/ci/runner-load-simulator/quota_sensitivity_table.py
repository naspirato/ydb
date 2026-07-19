#!/usr/bin/env python3
"""Wait / work / total P90 for monolith vs sharding at scaled pool quotas."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

from hourly_p90_table import metrics_from_events, percentile
from pool import PoolConfig
from pr_check_model import build_pr_check_runs, parse_ts
from simulate import build_work_items, replay

ROOT = Path(__file__).resolve().parent
DEFAULT_DATA = ROOT / "data" / "jobs_14d.json"
DEFAULT_CAPACITY = ROOT / "vendor" / "runner_capacity.yml"
DEFAULT_OUT = ROOT / "data" / "quota_sensitivity.md"


def scaled_config(base_path: Path, *, vcpu_mult: float, all_mult: float | None) -> PoolConfig:
    raw = yaml.safe_load(base_path.read_text(encoding="utf-8"))
    mults = {
        "instances": all_mult if all_mult is not None else 1.0,
        "vcpu": vcpu_mult if all_mult is None else all_mult,
        "ram_gb": all_mult if all_mult is not None else 1.0,
        "nrd_ssd_gb": all_mult if all_mult is not None else 1.0,
    }
    for res, mult in mults.items():
        raw["quotas"][res] = int(raw["quotas"][res] * mult)
    tmp = Path(f"/tmp/runner_cap_{vcpu_mult}_{all_mult}.yml")
    tmp.write_text(yaml.dump(raw), encoding="utf-8")
    return PoolConfig.load(tmp)


def p90_metrics(events, pr_runs, job_id_to_run, *, peak_only: bool) -> dict[str, float | int]:
    rows = metrics_from_events(events, pr_runs, job_id_to_run)
    if peak_only:
        rows = [r for r in rows if 9 <= r.hour_utc <= 15]
    if not rows:
        return {"n": 0, "wait_p90": 0.0, "work_p90": 0.0, "total_p90": 0.0}
    waits = [r.wait_min for r in rows]
    works = [r.work_min for r in rows]
    totals = [r.wait_min + r.work_min for r in rows]
    return {
        "n": len(rows),
        "wait_p90": round(percentile(waits, 90) or 0.0, 1),
        "work_p90": round(percentile(works, 90) or 0.0, 1),
        "total_p90": round(percentile(totals, 90) or 0.0, 1),
    }


def fmt_delta(a: float, b: float) -> str:
    return f"{b - a:+.1f}"


def fmt_pct(a: float, b: float) -> str:
    if a == 0:
        return "—"
    return f"{(b - a) / a * 100:+.1f}%"


def run_pair(
    jobs,
    pr_runs,
    config: PoolConfig,
) -> tuple[dict[str, float | int], dict[str, float | int], dict[str, float | int], dict[str, float | int]]:
    job_id_to_run = {str(r.rwdi_job["job_id"]): r.run_id for r in pr_runs}
    base = replay(
        build_work_items(jobs, pr_runs, parallel=False),
        config,
        name="baseline",
        pr_runs=pr_runs,
        parallel=False,
    )
    par = replay(
        build_work_items(jobs, pr_runs, parallel=True),
        config,
        name="parallel",
        pr_runs=pr_runs,
        parallel=True,
    )
    return (
        p90_metrics(base.alloc_events, pr_runs, job_id_to_run, peak_only=False),
        p90_metrics(par.alloc_events, pr_runs, job_id_to_run, peak_only=False),
        p90_metrics(base.alloc_events, pr_runs, job_id_to_run, peak_only=True),
        p90_metrics(par.alloc_events, pr_runs, job_id_to_run, peak_only=True),
    )


def build_markdown(rows: list[dict[str, object]]) -> str:
    lines = [
        "# Чувствительность к квотам: ожидание и выполнение (p90, мин)",
        "",
        "PR-check relwithdebinfo, рабочие дни, replay 14d.",
        "Базовые квоты: `vendor/runner_capacity.yml` (110 VM / 5400 vCPU / 23 TB RAM).",
        "",
        "## Все рабочие часы",
        "",
        "| Сценарий квот | vCPU | VM budget | Монолит wait | Монолит work | Монолит total | "
        "Шардинг wait | Шардинг work | Шардинг total | Δ wait | Δ work | Δ total | Δ total % |",
        "|---------------|-----:|----------:|-------------:|-------------:|--------------:|"
        "------------:|-------------:|--------------:|-------:|-------:|--------:|----------:|",
    ]
    for row in rows:
        if row.get("section") != "all":
            continue
        lines.append(
            f"| {row['label']} | {row['vcpu']} | {row['budget']:.1f} | "
            f"{row['b_wait']} | {row['b_work']} | {row['b_total']} | "
            f"{row['p_wait']} | {row['p_work']} | {row['p_total']} | "
            f"{fmt_delta(row['b_wait'], row['p_wait'])} | "
            f"{fmt_delta(row['b_work'], row['p_work'])} | "
            f"{fmt_delta(row['b_total'], row['p_total'])} | "
            f"{fmt_pct(row['b_total'], row['p_total'])} |"
        )

    lines.extend([
        "",
        "## Пик 09–15 UTC",
        "",
        "| Сценарий квот | vCPU | Монолит wait | Монолит work | Монолит total | "
        "Шардинг wait | Шардинг work | Шардинг total | Δ wait | Δ work | Δ total | Δ total % |",
        "|---------------|-----:|-------------:|-------------:|--------------:|"
        "------------:|-------------:|--------------:|-------:|-------:|--------:|----------:|",
    ])
    for row in rows:
        if row.get("section") != "peak":
            continue
        lines.append(
            f"| {row['label']} | {row['vcpu']} | "
            f"{row['b_wait']} | {row['b_work']} | {row['b_total']} | "
            f"{row['p_wait']} | {row['p_work']} | {row['p_total']} | "
            f"{fmt_delta(row['b_wait'], row['p_wait'])} | "
            f"{fmt_delta(row['b_work'], row['p_work'])} | "
            f"{fmt_delta(row['b_total'], row['p_total'])} | "
            f"{fmt_pct(row['b_total'], row['p_total'])} |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--capacity", type=Path, default=DEFAULT_CAPACITY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    jobs = json.loads(args.data.read_text(encoding="utf-8"))["jobs"]
    pr_runs = build_pr_check_runs(jobs, classify=True)

    scenarios = (
        ("Текущие (1.0×)", 1.0, None),
        ("vCPU +10%", 1.1, None),
        ("vCPU +25%", 1.25, None),
        ("vCPU +50%", 1.5, None),
        ("vCPU ×2", 2.0, None),
        ("Все квоты +25%", 1.0, 1.25),
        ("Все квоты ×2", 1.0, 2.0),
    )

    out_rows: list[dict[str, object]] = []
    for label, vcpu_mult, all_mult in scenarios:
        cfg = scaled_config(args.capacity, vcpu_mult=vcpu_mult, all_mult=all_mult)
        b_all, p_all, b_peak, p_peak = run_pair(jobs, pr_runs, cfg)
        vcpu = int(yaml.safe_load(args.capacity.read_text())["quotas"]["vcpu"] * (all_mult or vcpu_mult))
        base = {
            "label": label,
            "vcpu": vcpu,
            "budget": cfg.max_instances_budget(),
        }
        row_all = {
            **base,
            "section": "all",
            "b_wait": b_all["wait_p90"],
            "b_work": b_all["work_p90"],
            "b_total": b_all["total_p90"],
            "p_wait": p_all["wait_p90"],
            "p_work": p_all["work_p90"],
            "p_total": p_all["total_p90"],
        }
        row_peak = {
            **base,
            "section": "peak",
            "b_wait": b_peak["wait_p90"],
            "b_work": b_peak["work_p90"],
            "b_total": b_peak["total_p90"],
            "p_wait": p_peak["wait_p90"],
            "p_work": p_peak["work_p90"],
            "p_total": p_peak["total_p90"],
        }
        out_rows.append(row_all)
        out_rows.append(row_peak)
        print(json.dumps({"label": label, "all": row_all, "peak": row_peak}, ensure_ascii=False), flush=True)

    md = build_markdown(out_rows)
    args.output.write_text(md, encoding="utf-8")
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
