#!/usr/bin/env python3
"""Combined wait / work / total P90 table for all D-groups and rollout scenarios."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
DEFAULT_OUT = ROOT / "data" / "rollout_comparison_table.md"

D_GROUPS = (
    ("d_lt_60", "D < 60"),
    ("d_60_120", "60 ≤ D < 120"),
    ("d_120_200", "120 ≤ D < 200"),
    ("d_gte_200", "D ≥ 200"),
    ("all", "все D"),
)

SCENARIOS = (
    ("all", "all eligible", ROOT / "data" / "hourly_p90_table.json"),
    ("main_stable", "main + stable/*", ROOT / "data" / "hourly_p90_table_main_stable.json"),
    ("main_only", "main only", ROOT / "data" / "hourly_p90_table_main_only.json"),
)

SUMMARY_MD = {
    "all": ROOT / "data" / "hourly_p90_table.md",
    "main_stable": ROOT / "data" / "hourly_p90_table_main_stable.md",
    "main_only": ROOT / "data" / "hourly_p90_table_main_only.md",
}

SUMMARY_ROW_RE = re.compile(
    r"^\|\s*(\d{2}):00\s*\|\s*(\d+)\s*\|\s*([\d.]+|-)\s*\|\s*([\d.]+|-)\s*\|\s*"
    r"([\d.]+|-)\s*\|\s*([\d.]+|-)\s*\|\s*([\d.]+|-)\s*\|\s*([\d.+]+|-)\s*\|\s*([+-]?[\d.]+|-)\s*\|"
)


def fmt(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:.1f}"


def delta(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return b - a


def fmt_delta(a: float | None, b: float | None) -> str:
    d = delta(a, b)
    if d is None:
        return "—"
    return f"{d:+.1f}"


def load_scenario(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def cell(hour: int, d_key: str, side: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    if d_key == "all":
        return None
    key = f"{hour:02d}_{d_key}"
    return payload.get(side, {}).get(key)


def parse_summary_md(path: Path) -> dict[int, dict[str, float | int]]:
    text = path.read_text(encoding="utf-8")
    start = text.find("## Все группы D")
    if start < 0:
        return {}
    section = text[start:]
    out: dict[int, dict[str, float | int]] = {}
    for line in section.splitlines():
        m = SUMMARY_ROW_RE.match(line)
        if not m:
            continue
        hour = int(m.group(1))
        out[hour] = {
            "n": int(m.group(2)),
            "wait_b": float(m.group(3)),
            "wait_s": float(m.group(4)),
            "work_b": float(m.group(5)),
            "work_s": float(m.group(6)),
            "total_b": float(m.group(7)),
            "total_s": float(m.group(8)),
            "total_d": float(m.group(9)),
        }
    return out


def metrics_for(
    hour: int,
    d_key: str,
    scenario_key: str,
    payload: dict[str, Any],
    summaries: dict[str, dict[int, dict[str, float | int]]],
) -> dict[str, float | int] | None:
    if d_key == "all":
        row = summaries.get(scenario_key, {}).get(hour)
        if not row:
            return None
        return {
            "n": row["n"],
            "wait_b": row["wait_b"],
            "wait_s": row["wait_s"],
            "work_b": row["work_b"],
            "work_s": row["work_s"],
            "total_b": row["total_b"],
            "total_s": row["total_s"],
        }

    b = cell(hour, d_key, "baseline", payload)
    s = cell(hour, d_key, "sharding", payload)
    if not b and not s:
        return None
    b = b or {}
    s = s or {}
    return {
        "n": max(int(b.get("n", 0)), int(s.get("n", 0))),
        "wait_b": b.get("wait_p90"),
        "wait_s": s.get("wait_p90"),
        "work_b": b.get("work_p90"),
        "work_s": s.get("work_p90"),
        "total_b": b.get("total_p90"),
        "total_s": s.get("total_p90"),
    }


def build_table(
    *,
    hours: list[int],
    payloads: dict[str, dict[str, Any]],
    summaries: dict[str, dict[int, dict[str, float | int]]],
) -> str:
    sharded = {
        key: payload["coverage"].get("sharded_runs_applied", 0)
        for key, payload in payloads.items()
    }
    lines = [
        "# Сводка P90: ожидание / выполнение / итого по D и rollout",
        "",
        "Рабочие дни, PR-check relwithdebinfo, симуляция пула. "
        "P90 по объединённой выборке runs в ячейке (час × D).",
        "",
        f"- **all eligible** — sharded runs: {sharded.get('all', 0)}",
        f"- **main + stable/\\*** — sharded runs: {sharded.get('main_stable', 0)}",
        f"- **main only** — sharded runs: {sharded.get('main_only', 0)}",
        "",
        "| Час | D | Rollout | n | wait B | wait S | Δ wait | work B | work S | Δ work | total B | total S | Δ total |",
        "|----:|---|---------|--:|-------:|-------:|-------:|-------:|-------:|-------:|--------:|--------:|--------:|",
    ]

    for hour in hours:
        for d_key, d_label in D_GROUPS:
            for scenario_key, rollout_label, _ in SCENARIOS:
                m = metrics_for(hour, d_key, scenario_key, payloads[scenario_key], summaries)
                if m is None:
                    continue
                if m["n"] == 0:
                    continue
                lines.append(
                    f"| {hour:02d}:00 | {d_label} | {rollout_label} | {m['n']} | "
                    f"{fmt(m['wait_b'])} | {fmt(m['wait_s'])} | {fmt_delta(m['wait_b'], m['wait_s'])} | "
                    f"{fmt(m['work_b'])} | {fmt(m['work_s'])} | {fmt_delta(m['work_b'], m['work_s'])} | "
                    f"{fmt(m['total_b'])} | {fmt(m['total_s'])} | {fmt_delta(m['total_b'], m['total_s'])} |"
                )
    return "\n".join(lines) + "\n"


def build_peak_wide(
    *,
    hours: list[int],
    payloads: dict[str, dict[str, Any]],
    summaries: dict[str, dict[int, dict[str, float | int]]],
) -> str:
    rollout_labels = [label for _, label, _ in SCENARIOS]
    header = (
        "| Час | D | "
        + " | ".join(
            f"{label} wait B/S/Δ | {label} work B/S/Δ | {label} total B/S/Δ"
            for label in rollout_labels
        )
        + " |"
    )
    sep_parts = ["----:|---"] + [":---:"] * (len(rollout_labels) * 3)
    lines = [
        "## Пик 09–15 UTC — все rollout в одной строке (p90, мин)",
        "",
        "Формат блока: **baseline / sharding / Δ**.",
        "",
        header,
        "|" + "|".join(sep_parts) + "|",
    ]

    def cell_triplet(m: dict[str, float | int] | None) -> str:
        if not m:
            return "— / — / —"
        return (
            f"{fmt(m['wait_b'])} / {fmt(m['wait_s'])} / {fmt_delta(m['wait_b'], m['wait_s'])}"
        )

    def work_triplet(m: dict[str, float | int] | None) -> str:
        if not m:
            return "— / — / —"
        return (
            f"{fmt(m['work_b'])} / {fmt(m['work_s'])} / {fmt_delta(m['work_b'], m['work_s'])}"
        )

    def total_triplet(m: dict[str, float | int] | None) -> str:
        if not m:
            return "— / — / —"
        return (
            f"{fmt(m['total_b'])} / {fmt(m['total_s'])} / {fmt_delta(m['total_b'], m['total_s'])}"
        )

    for hour in hours:
        for d_key, d_label in D_GROUPS:
            cells: list[str] = []
            for scenario_key, _, _ in SCENARIOS:
                m = metrics_for(hour, d_key, scenario_key, payloads[scenario_key], summaries)
                cells.extend([cell_triplet(m), work_triplet(m), total_triplet(m)])
            lines.append(f"| {hour:02d}:00 | {d_label} | " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def build_peak_pivot(
    *,
    hours: list[int],
    payloads: dict[str, dict[str, Any]],
    summaries: dict[str, dict[int, dict[str, float | int]]],
) -> str:
    lines = [
        "## Пик 09–15 UTC — только Δ итого (мин) по rollout",
        "",
        "| Час | D | all eligible | main + stable/* | main only |",
        "|----:|---|-------------:|----------------:|----------:|",
    ]
    for hour in hours:
        for d_key, d_label in D_GROUPS:
            deltas: list[str] = []
            for scenario_key, _, _ in SCENARIOS:
                m = metrics_for(hour, d_key, scenario_key, payloads[scenario_key], summaries)
                if not m:
                    deltas.append("—")
                else:
                    deltas.append(fmt_delta(m["total_b"], m["total_s"]))
            lines.append(f"| {hour:02d}:00 | {d_label} | {deltas[0]} | {deltas[1]} | {deltas[2]} |")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--hours",
        default="0-23",
        help="Hour range UTC, e.g. 9-15 or 0-23",
    )
    args = parser.parse_args()

    if "-" in args.hours:
        lo, hi = args.hours.split("-", 1)
        hours = list(range(int(lo), int(hi) + 1))
    else:
        hours = [int(x) for x in args.hours.split(",")]

    payloads = {key: load_scenario(path) for key, _, path in SCENARIOS}
    summaries = {key: parse_summary_md(path) for key, path in SUMMARY_MD.items()}

    md = build_table(hours=hours, payloads=payloads, summaries=summaries)
    peak_hours = list(range(9, 16))
    if set(hours) >= set(peak_hours):
        md += "\n" + build_peak_wide(hours=peak_hours, payloads=payloads, summaries=summaries)
        md += "\n" + build_peak_pivot(hours=peak_hours, payloads=payloads, summaries=summaries)

    args.output.write_text(md, encoding="utf-8")
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
